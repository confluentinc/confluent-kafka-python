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
import itertools
import logging
from typing import Any, Callable, Dict, Optional, Tuple, TypeVar

T = TypeVar('T')

# Internal use only. Carries the identity that AIOConsumer presents to the
# Consumer gate on behalf of the "logical" caller currently in flight,
# which may move across ThreadPoolExecutor worker threads (e.g. a
# rebalance callback dispatched to one worker thread, followed by
# a re-entrant call made from within it which gets scheduled on a different worker thread.)
_reentry_identity_var: 'contextvars.ContextVar[int]' = contextvars.ContextVar('_reentry_identity', default=0)

# Internal use only. Process-wide counter that generates the identities
# AIOConsumer calls present to the Consumer gate.
_gate_id_ctr = itertools.count(1)


def get_or_generate_identity() -> int:
    """Return the identity to present to the Consumer gate for an
    AIOConsumer call. For re-entrant calls, the identity is fetched
    from the current context. For top-level calls, a fresh identity
    is generated. This method must be called on the event-loop thread,
    before dispatching to the executor.

    On Python versions where the Consumer gate is compiled out (see
    CFL_CONSUMER_GATE_ENABLED in confluent_kafka.h), the identity is still
    generated and passed through as usual, but remains unused by the gate.
    """
    identity = _reentry_identity_var.get()
    if identity:
        return identity
    # TODO NOGIL: Confirm itertools.count.__next__ is thread safe
    return next(_gate_id_ctr)


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
        identity = _reentry_identity_var.get()

        async def _run_with_identity() -> Any:
            _reentry_identity_var.set(identity)
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
