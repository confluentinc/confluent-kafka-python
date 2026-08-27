#!/usr/bin/env python
# -*- coding: utf-8 -*-
import asyncio
import concurrent.futures
import itertools
import threading
import time
from unittest.mock import Mock, patch

import pytest

from confluent_kafka import KafkaError, KafkaException, TopicPartition, cimpl
from confluent_kafka.aio import _common
from confluent_kafka.aio._AIOConsumer import AIOConsumer


class TestAIOConsumer:
    """Unit tests for AIOConsumer class."""

    @pytest.fixture
    def mock_consumer(self):
        """Mock the underlying confluent_kafka.Consumer."""
        with patch('confluent_kafka.aio._AIOConsumer.confluent_kafka.Consumer') as mock:
            yield mock

    @pytest.fixture
    def mock_common(self):
        """Mock the _common module callback wrapping."""
        with patch('confluent_kafka.aio._AIOConsumer._common') as mock:

            async def mock_async_call(executor, blocking_task, *args, **kwargs):
                return blocking_task(*args, **kwargs)

            mock.async_call.side_effect = mock_async_call
            # Outside a callback invocation there is no Invocation record;
            # _call() must take the fresh top-level path.
            mock.ReentryContext.current_invocation.return_value = None
            yield mock

    @pytest.fixture
    def basic_config(self):
        """Basic consumer configuration."""
        return {'bootstrap.servers': 'localhost:9092', 'group.id': 'test-group', 'auto.offset.reset': 'earliest'}

    @pytest.mark.asyncio
    async def test_constructor_executor_handling(self, mock_consumer, mock_common, basic_config):
        """Test constructor correctly handles custom executor vs max_workers parameter."""
        custom_executor = concurrent.futures.ThreadPoolExecutor(max_workers=4)
        try:
            # When using custom executor, max_workers of executor should be left unchanged
            consumer1 = AIOConsumer(basic_config, max_workers=2, executor=custom_executor)
            assert consumer1.executor is custom_executor
            assert consumer1.executor._max_workers == 4

            # When using default executor, max_workers of executor should be set to max_workers parameter
            consumer2 = AIOConsumer(basic_config, max_workers=3)
            assert consumer2.executor._max_workers == 3

        finally:
            custom_executor.shutdown(wait=True)

    @pytest.mark.asyncio
    async def test_constructor_invalid_max_workers(self, mock_consumer, mock_common, basic_config):
        """Test constructor validation logic for max_workers."""
        with pytest.raises(ValueError, match="max_workers must be at least 1"):
            AIOConsumer(basic_config, max_workers=0)

    @pytest.mark.asyncio
    async def test_call_method_executor_usage(self, mock_consumer, mock_common, basic_config):
        """Test that _call method properly uses ThreadPoolExecutor for
        async-to-sync bridging, setting the identity presented to the
        Consumer gate on the reentry ContextVar before invoking it."""
        mock_common.ReentryContext.generate_id.return_value = 42
        consumer = AIOConsumer(basic_config)

        mock_method = Mock(return_value="test_result")
        result = await consumer._call(mock_method, "arg1", kwarg1="value1")

        mock_method.assert_called_once_with("arg1", kwarg1="value1")
        mock_common.ReentryContext.active.assert_called_once_with(42)
        assert result == "test_result"

    @pytest.mark.asyncio
    async def test_poll_success(self, mock_consumer, mock_common, basic_config):
        """Test successful message polling."""
        consumer = AIOConsumer(basic_config)

        mock_message = Mock()
        mock_consumer.return_value.poll.return_value = mock_message

        result = await consumer.poll(timeout=1.0)

        assert result is mock_message

    @pytest.mark.asyncio
    async def test_consume_success(self, mock_consumer, mock_common, basic_config):
        """Test successful message consumption."""
        consumer = AIOConsumer(basic_config)

        mock_messages = [Mock(), Mock()]
        mock_consumer.return_value.consume.return_value = mock_messages

        result = await consumer.consume(num_messages=2, timeout=1.0)

        assert result == mock_messages

    @pytest.mark.asyncio
    async def test_subscribe_with_callbacks(self, mock_consumer, mock_common, basic_config):
        """Test subscription with async callbacks."""
        consumer = AIOConsumer(basic_config)

        async def on_assign(consumer, partitions):
            pass

        await consumer.subscribe(['test-topic'], on_assign=on_assign)
        mock_consumer.return_value.subscribe.assert_called_once()

    @pytest.mark.asyncio
    async def test_multiple_concurrent_operations(self, mock_consumer, mock_common, basic_config):
        """Test concurrent async operations."""
        consumer = AIOConsumer(basic_config)

        mock_message = Mock()
        mock_partitions = [TopicPartition('test', 0)]
        mock_metadata = Mock()
        mock_consumer.return_value.poll.return_value = mock_message
        mock_consumer.return_value.assignment.return_value = mock_partitions
        mock_consumer.return_value.consumer_group_metadata.return_value = mock_metadata

        tasks = [
            asyncio.create_task(consumer.poll(timeout=1.0)),
            asyncio.create_task(consumer.assignment()),
            asyncio.create_task(consumer.consumer_group_metadata()),
        ]

        results = await asyncio.gather(*tasks)
        assert results == [mock_message, mock_partitions, mock_metadata]

    @pytest.mark.asyncio
    async def test_concurrent_operations_error_handling(self, mock_consumer, mock_common, basic_config):
        """Test concurrent async operations handle errors gracefully."""
        mock_consumer.return_value.poll.side_effect = [
            KafkaException(KafkaError(KafkaError._TRANSPORT)),
            KafkaException(KafkaError(KafkaError._TRANSPORT)),
        ]
        mock_consumer.return_value.assignment.return_value = []

        consumer = AIOConsumer(basic_config)

        # Run concurrent operations
        tasks = [
            consumer.poll(timeout=0.1),
            consumer.poll(timeout=0.1),
        ]

        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Verify results
        assert len(results) == 2
        assert isinstance(results[0], KafkaException)
        assert isinstance(results[1], KafkaException)

    @pytest.mark.asyncio
    async def test_network_error_handling(self, mock_consumer, mock_common, basic_config):
        """Test AIOConsumer handles network errors gracefully."""
        mock_consumer.return_value.poll.side_effect = KafkaException(
            KafkaError(KafkaError._TRANSPORT, "Network timeout")
        )

        consumer = AIOConsumer(basic_config)

        with pytest.raises(KafkaException) as exc_info:
            await consumer.poll(timeout=1.0)

        assert exc_info.value.args[0].code() == KafkaError._TRANSPORT

    @pytest.mark.asyncio
    async def test_async_context_manager(self, mock_consumer, mock_common, basic_config):
        """Test AIOConsumer handles network errors gracefully."""
        async with AIOConsumer(basic_config) as _:
            pass

    # The tests below cover AIOConsumer's re-entrancy support.

    @pytest.mark.asyncio
    async def test_call_presents_nonzero_identity_to_blocking_task(self, mock_consumer, basic_config):
        """_call() must set a real, nonzero identity on the reentry
        ContextVar before invoking blocking_task on the worker thread."""
        consumer = AIOConsumer(basic_config)
        seen = []

        def blocking_task():
            seen.append(cimpl._reentry_identity_var.get())
            return "ok"

        result = await consumer._call(blocking_task)

        assert result == "ok"
        assert seen == [seen[0]] and seen[0] != 0

    @pytest.mark.asyncio
    async def test_call_generates_different_identities_for_independent_calls(self, mock_consumer, basic_config):
        """Two independent (non-nested) _call() invocations must each
        present their own fresh identity to the gate."""
        consumer = AIOConsumer(basic_config)
        seen = []

        def blocking_task():
            seen.append(cimpl._reentry_identity_var.get())

        await consumer._call(blocking_task)
        await consumer._call(blocking_task)

        assert len(seen) == 2
        assert seen[0] != 0 and seen[1] != 0
        assert seen[0] != seen[1]

    @pytest.mark.asyncio
    async def test_reentrant_call_from_wrapped_callback_reuses_enclosing_identity(self, mock_consumer, basic_config):
        """A rebalance callback fired synchronously from inside a blocking
        call must see the enclosing call's identity, and a re-entrant _call()
        made from within that callback must reuse it too."""
        consumer = AIOConsumer(basic_config)
        seen = {}

        async def on_assign(c, partitions):
            seen['callback'] = cimpl._reentry_identity_var.get()
            await consumer._call(lambda: seen.setdefault('reentrant', cimpl._reentry_identity_var.get()))

        loop = asyncio.get_event_loop()
        wrapped = consumer._wrap_callback(loop, on_assign, consumer._edit_rebalance_callbacks_args)

        def outer_blocking_task():
            seen['outer'] = cimpl._reentry_identity_var.get()
            # Simulate librdkafka invoking the rebalance callback
            # synchronously from inside the blocking call, on this same
            # worker thread.
            wrapped(None, [])

        await consumer._call(outer_blocking_task)

        assert seen['outer'] != 0
        assert seen['callback'] == seen['outer'], "callback should see the enclosing call's identity"
        assert seen['reentrant'] == seen['outer'], "re-entrant call from the callback must reuse the same identity"

    @pytest.mark.asyncio
    async def test_identity_does_not_leak_into_recycled_worker_thread(self, mock_consumer, basic_config):
        """Regression test: _call()'s wrapped_task must reset the reentry
        identity when it's done, not just set it."""
        executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
        try:
            consumer = AIOConsumer(basic_config, executor=executor)

            await consumer._call(lambda: None)

            # Submitted directly to the same single-worker executor,
            # bypassing _call()/ReentryContext entirely, to force reuse of
            # the same OS thread _call() just used.
            leaked = await asyncio.get_event_loop().run_in_executor(executor, cimpl._reentry_identity_var.get)
            assert leaked == 0, f"identity leaked into the worker thread's persistent context: {leaked}"
        finally:
            executor.shutdown(wait=True)

    @pytest.mark.asyncio
    async def test_call_acquires_and_releases_invocation_lock(self, mock_consumer, basic_config):
        """_call() must hold the current invocation's lock (as wrap_callback
        binds one Invocation per callback invocation) around the executor
        dispatch, and release it once the dispatch completes."""
        consumer = AIOConsumer(basic_config, max_workers=2)
        assert _common.ReentryContext.current_invocation() is None
        assert await consumer._call(lambda: "ok") == "ok"

        invocation = _common.Invocation(_common.ReentryContext.generate_id())
        entered = threading.Event()
        release = threading.Event()

        def blocking_task():
            entered.set()
            release.wait(timeout=5)
            return "done"

        async def call_under_invocation():
            with _common.ReentryContext.active(invocation.identity, invocation):
                return await consumer._call(blocking_task)

        task = asyncio.create_task(call_under_invocation())
        await asyncio.get_event_loop().run_in_executor(None, entered.wait, 5)
        assert invocation.lock.locked(), "the invocation's lock must be held while the dispatch is in flight"

        release.set()
        result = await task

        assert result == "done"
        assert not invocation.lock.locked(), "the lock must be released once the dispatch completes"

    @pytest.mark.asyncio
    async def test_concurrent_reentrant_calls_sharing_invocation_serialize(self, mock_consumer, basic_config):
        """Two reentrant calls belonging to the same callback invocation --
        as gather()/create_task() from within a callback produce, since both
        inherit the same context -- must never run their blocking_task
        concurrently."""
        consumer = AIOConsumer(basic_config, max_workers=3)
        invocation = _common.Invocation(_common.ReentryContext.generate_id())

        active_count = 0
        max_active = 0
        count_lock = threading.Lock()

        def blocking_task():
            nonlocal active_count, max_active
            with count_lock:
                active_count += 1
                max_active = max(max_active, active_count)
            time.sleep(0.1)
            with count_lock:
                active_count -= 1
            return "ok"

        async def reentrant_call():
            return await consumer._call(blocking_task)

        with _common.ReentryContext.active(invocation.identity, invocation):
            results = await asyncio.gather(reentrant_call(), reentrant_call())

        assert results == ["ok", "ok"]
        assert (
            max_active == 1
        ), f"blocking_task ran concurrently (max_active={max_active}) -- the lock failed to serialize"

    @pytest.mark.asyncio
    async def test_wrap_callback_binds_a_fresh_invocation_per_invocation(self, mock_consumer, basic_config):
        """wrap_callback's trampoline must bind a fresh Invocation per
        callback invocation, visible to the wrapped callback via
        current_invocation(). Two separate invocations (e.g. two separate
        rebalances) must not share one, and each must be closed once its
        callback has returned."""
        consumer = AIOConsumer(basic_config, max_workers=2)
        loop = asyncio.get_event_loop()
        seen = []

        async def callback(*args, **kwargs):
            invocation = _common.ReentryContext.current_invocation()
            seen.append(invocation)
            assert invocation.alive, "the invocation must be open while its callback is running"

        wrapped = consumer._wrap_callback(loop, callback)

        await loop.run_in_executor(None, wrapped)
        await loop.run_in_executor(None, wrapped)

        assert len(seen) == 2
        assert all(isinstance(invocation, _common.Invocation) for invocation in seen)
        assert seen[0] is not seen[1], "each callback invocation must get its own fresh Invocation"
        assert all(
            not invocation.alive for invocation in seen
        ), "invocations must be closed once their callback has returned"

    # The tests below cover calls that escape their callback invocation
    # (un-awaited create_task) and cancellation of re-entrant calls.

    @pytest.mark.asyncio
    async def test_escaped_task_gets_fresh_identity_after_callback_returns(self, mock_consumer, basic_config):
        """A call created inside a callback with create_task() but not
        awaited there outlives the callback invocation. When it eventually
        runs it must present a fresh identity, not the enclosing call's --
        presenting the inherited one would admit it into the gate alongside
        the still-running enclosing call."""
        consumer = AIOConsumer(basic_config, max_workers=4)
        seen = {}
        escaped = []

        async def on_assign(c, partitions):
            seen['callback'] = cimpl._reentry_identity_var.get()
            escaped.append(
                asyncio.create_task(
                    consumer._call(lambda: seen.setdefault('escaped', cimpl._reentry_identity_var.get()))
                )
            )
            # Returns without awaiting the task: it runs only after this
            # callback invocation has been closed.

        loop = asyncio.get_event_loop()
        wrapped = consumer._wrap_callback(loop, on_assign, consumer._edit_rebalance_callbacks_args)

        def outer_blocking_task():
            seen['outer'] = cimpl._reentry_identity_var.get()
            # Simulate librdkafka invoking the rebalance callback
            # synchronously from inside the blocking call.
            wrapped(None, [])

        await consumer._call(outer_blocking_task)
        await escaped[0]

        assert seen['callback'] == seen['outer'] != 0
        assert seen['escaped'] != 0
        assert (
            seen['escaped'] != seen['outer']
        ), "an escaped call must not present the identity of a callback invocation that already ended"

    @pytest.mark.asyncio
    async def test_task_awaited_inside_callback_inherits_identity(self, mock_consumer, basic_config):
        """A create_task() call awaited within the callback runs while the
        invocation is still open, so it must inherit the enclosing identity
        -- a fresh one would deadlock: the enclosing call holds the gate
        until the callback returns."""
        consumer = AIOConsumer(basic_config, max_workers=4)
        seen = {}

        async def on_assign(c, partitions):
            seen['callback'] = cimpl._reentry_identity_var.get()
            task = asyncio.create_task(
                consumer._call(lambda: seen.setdefault('awaited', cimpl._reentry_identity_var.get()))
            )
            await task

        loop = asyncio.get_event_loop()
        wrapped = consumer._wrap_callback(loop, on_assign, consumer._edit_rebalance_callbacks_args)

        def outer_blocking_task():
            seen['outer'] = cimpl._reentry_identity_var.get()
            wrapped(None, [])

        await consumer._call(outer_blocking_task)

        assert seen['callback'] == seen['outer'] != 0
        assert (
            seen['awaited'] == seen['outer']
        ), "a task awaited inside the callback must reuse the enclosing call's identity"

    @pytest.mark.asyncio
    async def test_cancelled_reentrant_call_holds_callback_open_until_worker_exits(self, mock_consumer, basic_config):
        """asyncio.wait_for() cancels the awaiting task on timeout, but an
        executor worker cannot be interrupted: the blocking call keeps
        running. The callback invocation must stay open -- keeping the
        enclosing gated call parked -- until that worker actually finishes,
        otherwise the two would run inside the gate concurrently."""
        consumer = AIOConsumer(basic_config, max_workers=4)
        slow_started = threading.Event()
        release_slow = threading.Event()
        order = []

        def slow_blocking():
            slow_started.set()
            release_slow.wait(timeout=5)
            order.append('worker_exited')

        async def on_assign(c, partitions):
            with pytest.raises(asyncio.TimeoutError):
                await asyncio.wait_for(consumer._call(slow_blocking), timeout=0.05)
            order.append('callback_body_done')

        loop = asyncio.get_event_loop()
        wrapped = consumer._wrap_callback(loop, on_assign, consumer._edit_rebalance_callbacks_args)

        def outer_blocking_task():
            wrapped(None, [])
            order.append('enclosing_call_resumed')

        outer = asyncio.create_task(consumer._call(outer_blocking_task))

        await loop.run_in_executor(None, slow_started.wait, 5)
        # Let wait_for() time out and the callback body finish; the
        # trampoline must still be parked, draining the cancelled-but-still-
        # running dispatch.
        while 'callback_body_done' not in order:
            await asyncio.sleep(0.01)
        await asyncio.sleep(0.05)
        assert not outer.done(), "the enclosing call resumed while the cancelled dispatch was still running"
        assert 'enclosing_call_resumed' not in order

        release_slow.set()
        await outer

        assert order.index('worker_exited') < order.index('enclosing_call_resumed'), order

    @pytest.mark.asyncio
    async def test_reentrant_dispatch_cancelled_before_start_unwinds_inflight(self, mock_consumer, basic_config):
        """A dispatch cancelled while still queued behind a busy worker
        never runs, so its in-flight accounting must be unwound by the
        executor future's done callback -- otherwise Invocation.close()
        would wait forever for a dispatch that will never finish."""
        executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
        try:
            consumer = AIOConsumer(basic_config, executor=executor)
            release_worker = threading.Event()
            worker_busy = threading.Event()

            def occupy_worker():
                worker_busy.set()
                release_worker.wait(timeout=5)

            executor.submit(occupy_worker)
            await asyncio.get_event_loop().run_in_executor(None, worker_busy.wait, 5)

            invocation = _common.Invocation(_common.ReentryContext.generate_id())
            ran = threading.Event()

            async def reentrant_call():
                with _common.ReentryContext.active(invocation.identity, invocation):
                    return await consumer._call(ran.set)

            task = asyncio.create_task(reentrant_call())
            # Once the lock is observed held, the task has already submitted
            # its dispatch (everything up to its await is one synchronous
            # step on the event loop).
            while not invocation.lock.locked():
                await asyncio.sleep(0.01)

            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task

            # close() must return promptly: the queued dispatch was
            # cancelled before it ever started.
            await asyncio.wait_for(invocation.close(), timeout=5)
            assert not ran.is_set(), "the cancelled-before-start dispatch must never run"
        finally:
            release_worker.set()
            executor.shutdown(wait=True)


class TestInvocation:
    """Unit tests for confluent_kafka.aio._common.Invocation: the record of
    one callback invocation and the drain that keeps the enclosing gated
    call parked until every dispatch carrying its identity has finished."""

    @pytest.mark.asyncio
    async def test_close_marks_dead_and_returns_immediately_when_idle(self):
        invocation = _common.Invocation(7)
        assert invocation.alive
        await asyncio.wait_for(invocation.close(), timeout=1)
        assert not invocation.alive

    @pytest.mark.asyncio
    async def test_close_waits_for_all_inflight_dispatches(self):
        invocation = _common.Invocation(7)
        invocation.dispatch_started()
        invocation.dispatch_started()

        closer = asyncio.create_task(invocation.close())
        await asyncio.sleep(0.05)
        assert not closer.done(), "close() must wait for in-flight dispatches"
        assert not invocation.alive, "close() must mark the invocation dead before draining"

        invocation.dispatch_finished()
        await asyncio.sleep(0.05)
        assert not closer.done(), "close() must wait for ALL in-flight dispatches"

        invocation.dispatch_finished()
        await asyncio.wait_for(closer, timeout=1)

    @pytest.mark.asyncio
    async def test_close_survives_cancellation_and_reraises_after_drain(self):
        """The gated call that fired the callback resumes when close()
        returns, so close() must keep draining even when its task is
        cancelled, and only re-raise the cancellation once the drain is
        complete."""
        invocation = _common.Invocation(7)
        invocation.dispatch_started()

        closer = asyncio.create_task(invocation.close())
        await asyncio.sleep(0)  # let close() block on the drain
        closer.cancel()
        await asyncio.sleep(0.05)
        assert not closer.done(), "cancellation must not abandon the drain"

        invocation.dispatch_finished()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(closer, timeout=1)


class TestReentryContext:
    """Unit tests for confluent_kafka.aio._common.ReentryContext: the
    identity AIOConsumer presents to the Consumer gate."""

    def test_generate_id_returns_nonzero_and_distinct_identities(self):
        """Two independent (non-nested) top-level calls must each get their
        own fresh, nonzero identity -- 0 means "not set" to the gate."""
        a = _common.ReentryContext.generate_id()
        b = _common.ReentryContext.generate_id()
        assert a != 0 and b != 0
        assert a != b

    def test_active_sets_current_id_then_resets_on_exit(self):
        """active() must make the identity visible via current_id() for the
        duration of its block -- this is what a re-entrant callback
        captures and presents through the gate. Once the block exits, it
        must reset the ContextVar rather than just setting it."""
        assert _common.ReentryContext.current_id() == 0
        with _common.ReentryContext.active(4242):
            assert _common.ReentryContext.current_id() == 4242
        assert _common.ReentryContext.current_id() == 0

    def test_active_nesting_restores_outer_identity(self):
        with _common.ReentryContext.active(11):
            with _common.ReentryContext.active(22):
                assert _common.ReentryContext.current_id() == 22
            assert _common.ReentryContext.current_id() == 11
        assert _common.ReentryContext.current_id() == 0

    def test_generated_identity_stays_within_unsigned_long_mask(self, monkeypatch):
        """The gate stores an identity in a C unsigned long, 32-bit on
        Windows, so generated identities must always fit -- verified here
        by forcing the counter to the edge of that range and confirming it
        wraps instead of overflowing."""
        monkeypatch.setattr(_common.ReentryContext, '_ctr', itertools.count(0xFFFFFFFE))
        seen = [_common.ReentryContext.generate_id() for _ in range(5)]
        assert all(0 < i <= 0xFFFFFFFF for i in seen), seen
        # 0xFFFFFFFF + 1 wraps to 0, remapped to 1 (0 means unset); the next
        # draw masks to 1 too (0x100000001 & 0xFFFFFFFF == 1), so 1 appears
        # twice at the wrap before the sequence continues from 2.
        assert seen == [0xFFFFFFFE, 0xFFFFFFFF, 1, 1, 2], seen

    def test_active_with_invocation_binds_current_invocation_then_resets_to_none(self):
        invocation = _common.Invocation(1)
        assert _common.ReentryContext.current_invocation() is None
        with _common.ReentryContext.active(1, invocation):
            assert _common.ReentryContext.current_invocation() is invocation
        assert _common.ReentryContext.current_invocation() is None

    def test_active_nesting_with_invocation_restores_outer_invocation(self):
        outer_invocation = _common.Invocation(1)
        inner_invocation = _common.Invocation(2)
        with _common.ReentryContext.active(1, outer_invocation):
            with _common.ReentryContext.active(2, inner_invocation):
                assert _common.ReentryContext.current_invocation() is inner_invocation
            assert _common.ReentryContext.current_invocation() is outer_invocation
        assert _common.ReentryContext.current_invocation() is None

    def test_active_without_invocation_arg_clears_outer_invocation_for_the_block(self):
        """active() called without an invocation argument does not preserve
        an outer bound Invocation -- it explicitly clears it for the
        duration of the block, then restores it on exit."""
        invocation = _common.Invocation(1)
        with _common.ReentryContext.active(1, invocation):
            assert _common.ReentryContext.current_invocation() is invocation
            with _common.ReentryContext.active(2):
                assert _common.ReentryContext.current_invocation() is None
            assert _common.ReentryContext.current_invocation() is invocation
