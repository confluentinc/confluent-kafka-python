#!/usr/bin/env python
# -*- coding: utf-8 -*-
import asyncio
import concurrent.futures
import itertools
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
        mock_common.ReentryIdentity.get_or_generate.return_value = 42
        consumer = AIOConsumer(basic_config, max_workers=2)

        mock_method = Mock(return_value="test_result")
        result = await consumer._call(mock_method, "arg1", kwarg1="value1")

        mock_method.assert_called_once_with("arg1", kwarg1="value1")
        mock_common.ReentryIdentity.active.assert_called_once_with(42)
        assert result == "test_result"

    @pytest.mark.asyncio
    async def test_poll_success(self, mock_consumer, mock_common, basic_config):
        """Test successful message polling."""
        consumer = AIOConsumer(basic_config, max_workers=2)

        mock_message = Mock()
        mock_consumer.return_value.poll.return_value = mock_message

        result = await consumer.poll(timeout=1.0)

        assert result is mock_message

    @pytest.mark.asyncio
    async def test_consume_success(self, mock_consumer, mock_common, basic_config):
        """Test successful message consumption."""
        consumer = AIOConsumer(basic_config, max_workers=2)

        mock_messages = [Mock(), Mock()]
        mock_consumer.return_value.consume.return_value = mock_messages

        result = await consumer.consume(num_messages=2, timeout=1.0)

        assert result == mock_messages

    @pytest.mark.asyncio
    async def test_subscribe_with_callbacks(self, mock_consumer, mock_common, basic_config):
        """Test subscription with async callbacks."""
        consumer = AIOConsumer(basic_config, max_workers=2)

        async def on_assign(consumer, partitions):
            pass

        await consumer.subscribe(['test-topic'], on_assign=on_assign)
        mock_consumer.return_value.subscribe.assert_called_once()

    @pytest.mark.asyncio
    async def test_multiple_concurrent_operations(self, mock_consumer, mock_common, basic_config):
        """Test concurrent async operations."""
        consumer = AIOConsumer(basic_config, max_workers=3)

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
        consumer = AIOConsumer(basic_config, max_workers=2)
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
        consumer = AIOConsumer(basic_config, max_workers=2)
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
        consumer = AIOConsumer(basic_config, max_workers=2)
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
            # bypassing _call()/ReentryIdentity entirely, to force reuse of
            # the same OS thread _call() just used.
            leaked = await asyncio.get_event_loop().run_in_executor(executor, cimpl._reentry_identity_var.get)
            assert leaked == 0, f"identity leaked into the worker thread's persistent context: {leaked}"
        finally:
            executor.shutdown(wait=True)


class TestReentryIdentity:
    """Unit tests for confluent_kafka.aio._common.ReentryIdentity: the
    identity AIOConsumer presents to the Consumer gate."""

    def test_get_or_generate_returns_nonzero_and_distinct_identities(self):
        """Two independent (non-nested) top-level calls must each get their
        own fresh, nonzero identity -- 0 means "not set" to the gate."""
        a = _common.ReentryIdentity.get_or_generate()
        b = _common.ReentryIdentity.get_or_generate()
        assert a != 0 and b != 0
        assert a != b

    def test_active_sets_current_reuses_for_get_or_generate_then_resets_on_exit(self):
        """active() must, for the duration of its block: (1) make the
        identity visible via current(), and (2) have get_or_generate()
        reuse it rather than generating a fresh one -- this is what lets a
        re-entrant call present the same identity through the gate. Once
        the block exits, it must reset the ContextVar rather than just
        setting it."""
        assert _common.ReentryIdentity.current() == 0
        with _common.ReentryIdentity.active(4242):
            assert _common.ReentryIdentity.current() == 4242
            assert _common.ReentryIdentity.get_or_generate() == 4242
        assert _common.ReentryIdentity.current() == 0

    def test_active_nesting_restores_outer_identity(self):
        with _common.ReentryIdentity.active(11):
            with _common.ReentryIdentity.active(22):
                assert _common.ReentryIdentity.current() == 22
            assert _common.ReentryIdentity.current() == 11
        assert _common.ReentryIdentity.current() == 0

    def test_generated_identity_stays_within_unsigned_long_mask(self, monkeypatch):
        """The gate stores an identity in a C unsigned long, 32-bit on
        Windows, so generated identities must always fit -- verified here
        by forcing the counter to the edge of that range and confirming it
        wraps instead of overflowing."""
        monkeypatch.setattr(_common.ReentryIdentity, '_ctr', itertools.count(0xFFFFFFFE))
        seen = [_common.ReentryIdentity.get_or_generate() for _ in range(5)]
        assert all(0 < i <= 0xFFFFFFFF for i in seen), seen
        # 0xFFFFFFFF + 1 wraps to 0, remapped to 1 (0 means unset); the next
        # draw masks to 1 too (0x100000001 & 0xFFFFFFFF == 1), so 1 appears
        # twice at the wrap before the sequence continues from 2.
        assert seen == [0xFFFFFFFE, 0xFFFFFFFF, 1, 1, 2], seen
