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
import contextvars
import functools
import logging
from typing import Any, Callable, Dict, Optional, Tuple, TypeVar

T = TypeVar('T')

# Internal use only. Set on the event-loop-thread's context as the first
# thing that runs inside a rebalance/on_commit callback coroutine scheduled
# via wrap_callback()/AIOConsumer._wrap_callback(). Carries the one-shot
# NOGIL gate token (see Handle_gate_mint_token()/Handle_gate_redeem_token()
# in confluent_kafka.h) that a re-entrant call made from within that
# callback (e.g. `await consumer.assign(...)` from an on_assign callback)
# must present to be let through the gate, since it may be dispatched to a
# different ThreadPoolExecutor worker thread than the one blocked inside
# the callback trampoline that currently holds the gate.
#
# Note: contextvars do NOT propagate across a ThreadPoolExecutor submission
# boundary (run_in_executor/executor.submit start the callable with a fresh
# context on the worker thread). Methods that need the token must therefore
# read it via `.get()` on the event-loop thread -- where the callback
# coroutine actually runs -- and pass it through explicitly as a plain
# argument into the executor call; it must never be relied upon to
# auto-propagate to the worker thread performing the blocking C call.
_reentry_token_var: 'contextvars.ContextVar[int]' = contextvars.ContextVar('_reentry_token', default=0)


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
    get_consumer: Optional[Callable[[], Any]] = None,
) -> Callable[..., Any]:
    """Wrap a user callback so it runs on the event loop, bridged from the
    ThreadPoolExecutor worker thread currently inside librdkafka's
    synchronous callback trampoline (which already holds the NOGIL Consumer
    gate on free-threaded builds).

    If `get_consumer` is given, it is called (with no arguments) to obtain
    the underlying confluent_kafka.Consumer on demand, and a one-shot gate
    token is minted on the calling (gate-owning) thread before scheduling
    the callback coroutine, then set on `_reentry_token_var` as the first
    thing that runs inside it. This lets a small set of re-entrant
    AIOConsumer methods (assign/incremental_assign/unassign/
    incremental_unassign/commit) called from within the user's callback
    borrow the gate even if dispatched to a different worker thread.
    `get_consumer` is a callable rather than the Consumer instance itself
    because this wrapping happens before the Consumer object exists (it is
    embedded in the config dict passed to the Consumer constructor). On a
    regular (non-free-threaded) build, or if `get_consumer` is None, minting
    is a no-op (returns 0) and the token is simply never presented.
    """

    def ret(*args: Any, **kwargs: Any) -> Any:
        if edit_args:
            args = edit_args(args)
        if edit_kwargs:
            kwargs = edit_kwargs(kwargs)

        # Mint the token on this thread (the current gate owner) BEFORE
        # scheduling, not inside the scheduled coroutine which will run on
        # the event-loop thread.
        token = get_consumer()._gate_mint_token() if get_consumer is not None else 0

        async def _run_with_token() -> Any:
            _reentry_token_var.set(token)
            return await callback(*args, **kwargs)

        f = asyncio.run_coroutine_threadsafe(_run_with_token(), loop)
        return f.result()

    return ret


def wrap_conf_callback(
    loop: asyncio.AbstractEventLoop,
    conf: Dict[str, Any],
    name: str,
    get_consumer: Optional[Callable[[], Any]] = None,
) -> None:
    if name in conf:
        cb = conf[name]
        conf[name] = wrap_callback(loop, cb, get_consumer=get_consumer)


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
