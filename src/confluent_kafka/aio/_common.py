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
import contextvars
import functools
import itertools
import logging
from typing import Any, Callable, Dict, Iterator, Optional, Tuple, TypeVar

from confluent_kafka import cimpl

T = TypeVar('T')


# TODO NOGIL: Fix for fire and forget tasks created inside callbacks.
# We don't want such tasks to escape serialization and run concurrently.
class ReentryContext:
    """Internal use only. Per-call-chain context AIOConsumer carries through
    a ContextVar: the identity presented to the Consumer reentrancy gate, and
    the lock serializing concurrent dispatches within one callback invocation.

    The sync Consumer uses the calling thread's ID as its gate identity, but
    that does not work for AIOConsumer: a call and the re-entrant calls its
    callbacks make can run on different ThreadPoolExecutor worker threads (e.g.
    a rebalance callback dispatched to one worker, then a re-entrant call from
    within it scheduled on another). AIOConsumer therefore generates an
    identity per top-level call and carries it in a ContextVar the gate reads
    -- see Handle_gate_enter() in Consumer.c.
    """

    # The ContextVar the C gate itself reads, defined in confluent_kafka.c.
    _id_var = cimpl._reentry_identity_var

    # Serializes concurrent/un-awaited calls dispatched from within the same
    # callback invocation. Not used by C gate and only used by AIO Consumer.
    _lock_var: contextvars.ContextVar = contextvars.ContextVar('reentry_lock_var', default=None)

    # Process-wide counter generating identities.
    _ctr = itertools.count(1)

    # The gate stores an identity in a C unsigned long, which is only 32 bits
    # on Windows, so identities are masked to stay representable there.
    _MASK = 0xFFFFFFFF

    @classmethod
    def get_or_generate_id(cls) -> int:
        """Return the identity for an AIOConsumer call: the current context's
        identity for a re-entrant call, or a fresh one for a top-level call.

        Must be called on the event-loop thread, before dispatching to the
        executor.
        """
        identity = cls.current_id()
        if identity:
            return identity
        return (next(cls._ctr) & cls._MASK) or 1

    @classmethod
    def current_id(cls) -> int:
        """Return the identity of the call currently in flight, or 0 if none.

        Called on a worker thread from inside a gated call, to capture the
        identity that the enclosing call set.
        """
        return cls._id_var.get()

    @classmethod
    def current_lock(cls) -> Optional[asyncio.Lock]:
        """Return the lock serializing calls dispatched concurrently from
        the current callback invocation, or None for a top-level call.

        Called on the event-loop thread, before dispatching to the executor.
        """
        return cls._lock_var.get()

    @classmethod
    @contextlib.contextmanager
    def active(cls, identity: int, lock: Optional[asyncio.Lock] = None) -> Iterator[None]:
        """Present `identity` (and, for a callback invocation, `lock`) to
        the gate for the duration of the block.

        The identity is set here, inside the worker thread (or callback task)
        that makes the call, rather than before dispatching to the executor:
        contextvars only propagate into a ThreadPoolExecutor worker when that
        thread's Context is first established, so a set() on the event-loop
        thread would be invisible to later calls reusing the same worker.
        """
        active_id = cls._id_var.set(identity)
        active_lock = cls._lock_var.set(lock)
        try:
            yield
        finally:
            cls._id_var.reset(active_id)
            cls._lock_var.reset(active_lock)


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
        identity = ReentryContext.current_id()

        async def _run_with_identity() -> Any:
            # A fresh lock per invocation. Concurrent calls from the
            # callback inherit the ID and lock via context propagation
            # to the tasks asyncio creates for them, so only one is ever
            # dispatched to the gate at a time.
            with ReentryContext.active(identity, asyncio.Lock()):
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
