# Copyright 2025 Confluent Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import asyncio
import concurrent.futures
import contextlib
import functools
import itertools
import logging
from typing import Any, Callable, Dict, Iterator, Optional, Tuple, TypeVar

from confluent_kafka import cimpl

T = TypeVar('T')


class ReentryIdentity:
    """Internal use only. The identity AIOConsumer presents to the Consumer
    reentrancy gate.

    The sync Consumer uses the calling thread's ID as its gate identity, but
    that does not work for AIOConsumer: a call and the re-entrant calls its
    callbacks make can run on different ThreadPoolExecutor worker threads (e.g.
    a rebalance callback dispatched to one worker, then a re-entrant call from
    within it scheduled on another). AIOConsumer therefore generates an
    identity per top-level call and carries it in a ContextVar the gate reads
    -- see Handle_gate_enter() in Consumer.c.
    """

    _var = cimpl._reentry_identity_var

    # Process-wide counter generating identities.
    _ctr = itertools.count(1)

    # The gate stores an identity in a C unsigned long, which is only 32 bits
    # on Windows, so identities are masked to stay representable there.
    _MASK = 0xFFFFFFFF

    @classmethod
    def get_or_generate(cls) -> int:
        """Return the identity for an AIOConsumer call: the current context's
        identity for a re-entrant call, or a fresh one for a top-level call.

        Must be called on the event-loop thread, before dispatching to the
        executor.
        """
        identity = cls.current()
        if identity:
            return identity
        return (next(cls._ctr) & cls._MASK) or 1

    @classmethod
    def current(cls) -> int:
        """Return the identity of the call currently in flight, or 0 if none.

        Called on a worker thread from inside a gated call, to capture the
        identity that the enclosing call set.
        """
        return cls._var.get()

    @classmethod
    @contextlib.contextmanager
    def active(cls, identity: int) -> Iterator[None]:
        """Present `identity` to the gate for the duration of the block.

        The identity is set here, inside the worker thread (or callback task)
        that makes the call, rather than before dispatching to the executor:
        contextvars only propagate into a ThreadPoolExecutor worker when that
        thread's Context is first established, so a set() on the event-loop
        thread would be invisible to later calls reusing the same worker.
        """
        token = cls._var.set(identity)
        try:
            yield
        finally:
            cls._var.reset(token)


class AsyncLogger:

    def __init__(self, loop: asyncio.AbstractEventLoop, logger: logging.Logger) -> None:
        self.loop = loop
        self.logger = logger

    def log(self, *args: Any, **kwargs: Any) -> None:
        self.loop.call_soon_threadsafe(lambda: self.logger.log(*args, **kwargs))


def wrap_callback(
    loop: asyncio.AbstractEventLoop,
    callback: Callable[..., Any],
    edit_args: Optional[Callable[[Tuple[Any, ...]], Tuple[Any, ...]]] = None,
    edit_kwargs: Optional[Callable[[Dict[str, Any]], Dict[str, Any]]] = None,
) -> Callable[..., Any]:

    def ret(*args: Any, **kwargs: Any) -> Any:
        if edit_args:
            args = edit_args(args)
        if edit_kwargs:
            kwargs = edit_kwargs(kwargs)

        # Set by the enclosing call's wrapped_task before this callback
        # trampoline fired -- see AIOConsumer._call().
        identity = ReentryIdentity.current()

        async def _run_with_identity() -> Any:
            # Tasks the callback spawns inherit this identity, so calls back into the
            # Consumer must be awaited one at a time -- concurrent ones (gather,
            # or an un-awaited create_task) would all be let through the gate.
            # Making such concurrent calls is not supported.
            with ReentryIdentity.active(identity):
                return await callback(*args, **kwargs)

        f = asyncio.run_coroutine_threadsafe(_run_with_identity(), loop)
        return f.result()

    return ret


def wrap_conf_callback(
    loop: asyncio.AbstractEventLoop,
    conf: Dict[str, Any],
    name: str,
) -> None:
    if name in conf:
        cb = conf[name]
        conf[name] = wrap_callback(loop, cb)


def wrap_conf_logger(loop: asyncio.AbstractEventLoop, conf: Dict[str, Any]) -> None:
    if 'logger' in conf:
        conf['logger'] = AsyncLogger(loop, conf['logger'])


async def async_call(
    executor: concurrent.futures.Executor, blocking_task: Callable[..., T], *args: Any, **kwargs: Any
) -> T:
    """Helper function for blocking operations that need ThreadPool execution

    Args:
        executor: ThreadPoolExecutor to use for blocking operations
        blocking_task: The blocking function to execute
        *args, **kwargs: Arguments to pass to the blocking function

    Returns:
        Result of the blocking function execution
    """
    return (
        await asyncio.gather(
            asyncio.get_running_loop().run_in_executor(executor, functools.partial(blocking_task, *args, **kwargs))
        )
    )[0]


def wrap_common_callbacks(loop: asyncio.AbstractEventLoop, conf: Dict[str, Any]) -> None:
    wrap_conf_callback(loop, conf, 'error_cb')
    wrap_conf_callback(loop, conf, 'throttle_cb')
    wrap_conf_callback(loop, conf, 'stats_cb')
    wrap_conf_logger(loop, conf)
