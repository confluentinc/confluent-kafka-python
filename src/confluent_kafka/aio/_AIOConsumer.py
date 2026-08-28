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
from typing import Any, Callable, Dict, Optional, Tuple

try:
    from typing import Self
except ImportError:
    # FIXME: drop fallback once we require Python >= 3.11
    from typing_extensions import Self

import confluent_kafka

from . import _common as _common


class AIOConsumer:
    """
    Asyncio wrapper around confluent_kafka.Consumer.

    Every method dispatches the underlying blocking Consumer call to a
    thread pool executor and returns an awaitable, so a single AIOConsumer
    can be driven from asyncio code without blocking the event loop.

    librdkafka's Consumer is not thread-safe, so concurrent access to
    the same AIOConsumer instance is serialized rather than allowed to
    race: if a second call arrives while another is still in flight, it
    waits for the first to finish.

    The one exception is re-entrancy: a call made from within a
    callback that this same logical operation triggered (for example,
    on_assign calling assign(), or on_commit calling commit()) is
    recognized as belonging to the same caller and is admitted
    immediately rather than waiting on itself.

    A call created inside a callback but not awaited there (e.g. via an
    un-awaited asyncio.create_task()) may outlive the callback invocation
    it was created in. Such a call is not re-entrant: it runs as a new
    top-level call, waiting for the operation that fired the callback to
    finish rather than being admitted alongside it.
    """

    def __init__(
        self,
        consumer_conf: Dict[str, Any],
        max_workers: int = 100,
        executor: Optional[concurrent.futures.Executor] = None,
    ) -> None:
        self._closed = False
        self._owns_executor = False
        if executor is not None:
            # Executor must have at least one worker.
            # At least two workers are needed when calling re-entrant
            # methods from callbacks.
            self.executor = executor
        else:
            if max_workers < 1:
                raise ValueError("max_workers must be at least 1")
            self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=max_workers)
            self._owns_executor = True

        loop = asyncio.get_event_loop()
        wrap_common_callbacks = _common.wrap_common_callbacks
        wrap_conf_callback = _common.wrap_conf_callback
        wrap_common_callbacks(loop, consumer_conf)
        wrap_conf_callback(loop, consumer_conf, 'on_commit')

        self._consumer: confluent_kafka.Consumer = confluent_kafka.Consumer(consumer_conf)

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *_) -> None:
        await self.close()

    async def _call(self, blocking_task: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        if self._closed:
            raise RuntimeError("Consumer closed")

        # Set when this call was made from within a callback invocation
        # (e.g. on_assign), inherited via context propagation -- including
        # into tasks created inside the callback. See _common.Invocation.
        invocation = _common.ReentryContext.current_invocation()

        if invocation is not None:
            # Sibling calls dispatched concurrently from the same callback
            # invocation serialize on its lock, so only one presents the
            # shared identity to the gate at a time.
            async with invocation.lock:
                if invocation.alive:
                    return await self._reentrant_call(invocation, blocking_task, *args, **kwargs)
            # The callback has already returned (e.g. an un-awaited
            # create_task() that outlived it): its identity is stale, and
            # presenting it could run this call concurrently with the still
            # in-flight call that fired the callback. Fall through and run
            # as a fresh top-level call instead, outside the lock, which
            # only exists to serialize the invocation's own calls.

        # Resolved here, on the event-loop thread, and presented from inside
        # wrapped_task so it is visible to the call it guards, including any
        # re-entrant callback (e.g. on_assign) that blocking_task triggers
        # synchronously before returning.
        identity = _common.ReentryContext.generate_id()

        def wrapped_task(*task_args: Any, **task_kwargs: Any) -> Any:
            with _common.ReentryContext.active(identity):
                return blocking_task(*task_args, **task_kwargs)

        return await _common.async_call(self.executor, wrapped_task, *args, **kwargs)

    async def _reentrant_call(
        self, invocation: _common.Invocation, blocking_task: Callable[..., Any], *args: Any, **kwargs: Any
    ) -> Any:
        """Dispatch a call made from within a live callback invocation,
        presenting the enclosing call's identity so the gate admits it.

        The invocation must stay open until this dispatch has fully left
        the gate, even if the task awaiting it is cancelled (e.g. by
        asyncio.wait_for()): a worker thread cannot be interrupted, so the
        blocking call keeps running after the await below is abandoned.
        The done callback on the executor future -- which fires only once
        wrapped_task has actually returned, or the submission was cancelled
        before ever starting -- is therefore what balances the in-flight
        count, never this coroutine's own await.

        Called with invocation.lock held.
        """
        loop = asyncio.get_running_loop()

        def wrapped_task() -> Any:
            with _common.ReentryContext.active(invocation.identity):
                return blocking_task(*args, **kwargs)

        invocation.dispatch_started()
        future = self.executor.submit(wrapped_task)

        def on_done(_: concurrent.futures.Future) -> None:
            try:
                loop.call_soon_threadsafe(invocation.dispatch_finished)
            except RuntimeError:
                # Event loop already closed (interpreter shutdown); nothing
                # is waiting on the drain anymore.
                pass

        future.add_done_callback(on_done)
        return await asyncio.wrap_future(future)

    def _wrap_callback(
        self,
        loop: asyncio.AbstractEventLoop,
        callback: Callable[..., Any],
        edit_args: Optional[Callable[[Tuple[Any, ...]], Tuple[Any, ...]]] = None,
        edit_kwargs: Optional[Callable[[Any], Any]] = None,
    ) -> Callable[..., Any]:
        return _common.wrap_callback(loop, callback, edit_args=edit_args, edit_kwargs=edit_kwargs)

    async def poll(self, *args: Any, **kwargs: Any) -> Any:
        """
        Polls for a single message from the subscribed topics.

        Performance Note:
            For high-throughput applications, prefer consume() over poll():
            consume() can retrieve multiple messages per call and amortize the
            async overhead across the entire batch.

            On the other hand, poll() retrieves one message per call, which means
            the ThreadPoolExecutor overhead is applied to each individual message.
            This can result in lower throughput compared to the synchronous
            consumer.poll() due to the async coordination overhead not being
            amortized.

        """
        return await self._call(self._consumer.poll, *args, **kwargs)

    async def consume(self, *args: Any, **kwargs: Any) -> Any:
        """
        Consumes a batch of messages from the subscribed topics.

        Performance Note:
            This method is recommended for high-throughput applications.

            By retrieving multiple messages per ThreadPoolExecutor call, the async
            coordination overhead is shared across all messages in the batch,
            resulting in much better throughput compared to repeated poll() calls.
        """
        return await self._call(self._consumer.consume, *args, **kwargs)

    def _edit_rebalance_callbacks_args(self, args: Tuple[Any, ...]) -> Tuple[Any, ...]:
        args_list = list(args)
        args_list[0] = self
        return tuple(args_list)

    async def subscribe(self, *args: Any, **kwargs: Any) -> Any:
        loop = asyncio.get_event_loop()
        for callback in ['on_assign', 'on_revoke', 'on_lost']:
            if callback in kwargs:
                kwargs[callback] = self._wrap_callback(
                    loop, kwargs[callback], self._edit_rebalance_callbacks_args
                )  # noqa: E501
        return await self._call(self._consumer.subscribe, *args, **kwargs)

    async def unsubscribe(self, *args: Any, **kwargs: Any) -> Any:
        return await self._call(self._consumer.unsubscribe, *args, **kwargs)

    async def commit(self, *args: Any, **kwargs: Any) -> Any:
        return await self._call(self._consumer.commit, *args, **kwargs)

    async def close(self, *args: Any, **kwargs: Any) -> Any:
        try:
            return await self._call(self._consumer.close, *args, **kwargs)
        finally:
            self._closed = True
            if self._owns_executor:
                await asyncio.get_running_loop().run_in_executor(None, self.executor.shutdown, True)

    async def seek(self, *args: Any, **kwargs: Any) -> Any:
        return await self._call(self._consumer.seek, *args, **kwargs)

    async def pause(self, *args: Any, **kwargs: Any) -> Any:
        return await self._call(self._consumer.pause, *args, **kwargs)

    async def resume(self, *args: Any, **kwargs: Any) -> Any:
        return await self._call(self._consumer.resume, *args, **kwargs)

    async def store_offsets(self, *args: Any, **kwargs: Any) -> Any:
        return await self._call(self._consumer.store_offsets, *args, **kwargs)

    async def committed(self, *args: Any, **kwargs: Any) -> Any:
        return await self._call(self._consumer.committed, *args, **kwargs)

    async def assign(self, *args: Any, **kwargs: Any) -> Any:
        return await self._call(self._consumer.assign, *args, **kwargs)

    async def unassign(self, *args: Any, **kwargs: Any) -> Any:
        return await self._call(self._consumer.unassign, *args, **kwargs)

    async def incremental_assign(self, *args: Any, **kwargs: Any) -> Any:
        return await self._call(self._consumer.incremental_assign, *args, **kwargs)

    async def incremental_unassign(self, *args: Any, **kwargs: Any) -> Any:
        return await self._call(self._consumer.incremental_unassign, *args, **kwargs)

    async def assignment(self, *args: Any, **kwargs: Any) -> Any:
        return await self._call(self._consumer.assignment, *args, **kwargs)

    async def position(self, *args: Any, **kwargs: Any) -> Any:
        return await self._call(self._consumer.position, *args, **kwargs)

    async def consumer_group_metadata(self, *args: Any, **kwargs: Any) -> Any:
        return await self._call(self._consumer.consumer_group_metadata, *args, **kwargs)

    async def set_sasl_credentials(self, *args: Any, **kwargs: Any) -> Any:
        return await self._call(self._consumer.set_sasl_credentials, *args, **kwargs)

    async def list_topics(self, *args: Any, **kwargs: Any) -> Any:
        return await self._call(self._consumer.list_topics, *args, **kwargs)

    async def get_watermark_offsets(self, *args: Any, **kwargs: Any) -> Any:
        return await self._call(self._consumer.get_watermark_offsets, *args, **kwargs)

    async def offsets_for_times(self, *args: Any, **kwargs: Any) -> Any:
        return await self._call(self._consumer.offsets_for_times, *args, **kwargs)
