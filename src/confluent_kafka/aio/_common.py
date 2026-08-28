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


class Invocation:
    """Internal use only. State for one callback invocation (e.g. one
    on_assign firing): the identity it presents to the Consumer gate, the
    lock serializing sibling calls dispatched from within it, whether the
    callback has returned yet, and how many dispatches carrying its
    identity are still in flight on executor workers.

    The record is shared by reference into every context copied from the
    callback's context (the tasks gather()/create_task() create), so
    closing it here is visible to all of them -- including a task that
    outlives the callback and only runs later (see AIOConsumer._call()).
    """

    __slots__ = ('identity', 'lock', '_alive', '_inflight', '_idle')

    def __init__(self, identity: int) -> None:
        self.identity = identity
        self.lock = asyncio.Lock()
        self._alive = True
        self._inflight = 0
        self._idle = asyncio.Event()
        self._idle.set()

    @property
    def alive(self) -> bool:
        """Whether the callback invocation is still open. Only a call made
        while it is open may present its identity to the gate: that is
        exactly when the gated call that fired the callback is guaranteed
        to be parked, waiting for the callback to return."""
        return self._alive

    def dispatch_started(self) -> None:
        """Record one gated dispatch carrying this invocation's identity.
        Event-loop thread only, under self.lock, while alive."""
        self._inflight += 1
        self._idle.clear()

    def dispatch_finished(self) -> None:
        """Counterpart to dispatch_started(), called once the dispatch has
        fully left the gate (or was cancelled before it ever started).
        Event-loop thread only -- marshal via call_soon_threadsafe from
        other threads."""
        self._inflight -= 1
        if self._inflight == 0:
            self._idle.set()

    async def close(self) -> None:
        """Close the invocation and wait until no dispatch carrying its
        identity is still inside the gate.

        The gated call that fired the callback resumes as soon as this
        returns, so it must not return while a same-identity dispatch could
        still be executing -- even if the surrounding task is cancelled.
        Cancellation is therefore re-raised only after the drain completes.
        """
        self._alive = False
        cancelled = False
        while self._inflight:
            try:
                await self._idle.wait()
            except asyncio.CancelledError:
                cancelled = True
        if cancelled:
            raise asyncio.CancelledError


class ReentryContext:
    """Internal use only. Per-call-chain context AIOConsumer carries through
    ContextVars: the identity presented to the Consumer reentrancy gate, and
    the Invocation record of the callback invocation the call chain belongs
    to, if any.

    The sync Consumer uses the calling thread's ID as its gate identity, but
    that does not work for AIOConsumer: a call and the re-entrant calls its
    callbacks make can run on different ThreadPoolExecutor worker threads (e.g.
    a rebalance callback dispatched to one worker, then a re-entrant call from
    within it scheduled on another). AIOConsumer therefore generates an
    identity per top-level call and carries it in a ContextVar the gate reads
    -- see Handle_serialize_enter() in confluent_kafka.c.
    """

    # The ContextVar the C gate itself reads, defined in confluent_kafka.c.
    _id_var = cimpl._reentry_identity_var

    # The Invocation of the callback invocation the current context belongs
    # to, or None outside any callback. Not read by the C gate; used by
    # AIOConsumer._call() to decide whether the inherited identity may still
    # be presented.
    _invocation_var: contextvars.ContextVar = contextvars.ContextVar('reentry_invocation_var', default=None)

    # Process-wide counter generating identities.
    _ctr = itertools.count(1)

    # The gate stores an identity in a C unsigned long, which is only 32 bits
    # on Windows, so identities are masked to stay representable there.
    _MASK = 0xFFFFFFFF

    @classmethod
    def generate_id(cls) -> int:
        """Return a fresh identity for a top-level AIOConsumer call.

        Must be called on the event-loop thread, before dispatching to the
        executor.
        """
        return (next(cls._ctr) & cls._MASK) or 1

    @classmethod
    def current_id(cls) -> int:
        """Return the identity of the call currently in flight, or 0 if none.

        Called on a worker thread from inside a gated call, to capture the
        identity that the enclosing call set.
        """
        return cls._id_var.get()

    @classmethod
    def current_invocation(cls) -> Optional[Invocation]:
        """Return the Invocation of the callback invocation the current
        context belongs to, or None for a top-level call.

        Called on the event-loop thread, before dispatching to the executor.
        """
        return cls._invocation_var.get()

    @classmethod
    @contextlib.contextmanager
    def active(cls, identity: int, invocation: Optional[Invocation] = None) -> Iterator[None]:
        """Present `identity` (and, for a callback invocation, its
        Invocation record) for the duration of the block.

        The identity is set here, inside the worker thread (or callback task)
        that makes the call, rather than before dispatching to the executor:
        contextvars only propagate into a ThreadPoolExecutor worker when that
        thread's Context is first established, so a set() on the event-loop
        thread would be invisible to later calls reusing the same worker.
        """
        active_id = cls._id_var.set(identity)
        active_invocation = cls._invocation_var.set(invocation)
        try:
            yield
        finally:
            cls._id_var.reset(active_id)
            cls._invocation_var.reset(active_invocation)


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

        async def _run_with_invocation() -> Any:
            # A fresh Invocation per callback invocation. Calls made from
            # the callback inherit it (and the identity) via context
            # propagation to the tasks asyncio creates for them; its lock
            # ensures only one is ever dispatched to the gate at a time.
            invocation = Invocation(identity)
            with ReentryContext.active(identity, invocation):
                try:
                    return await callback(*args, **kwargs)
                finally:
                    # Close before returning: once this coroutine finishes,
                    # the gated call that fired the callback resumes, and
                    # it must not run concurrently with a dispatch still
                    # carrying this invocation's identity. Calls that run
                    # after this point present a fresh identity instead --
                    # see AIOConsumer._call().
                    await invocation.close()

        f = asyncio.run_coroutine_threadsafe(_run_with_invocation(), loop)
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
