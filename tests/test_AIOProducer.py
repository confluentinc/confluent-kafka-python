#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Unit tests for AIOProducer class.

"""

import asyncio
import concurrent.futures
from unittest.mock import Mock, patch

import pytest

from confluent_kafka import KafkaError, KafkaException
from confluent_kafka.aio.producer._AIOProducer import AIOProducer


class TestAIOProducer:
    """Unit tests for AIOProducer class."""

    @pytest.fixture
    def mock_producer(self):
        with patch('confluent_kafka.aio.producer._AIOProducer.confluent_kafka.Producer') as mock:
            yield mock

    @pytest.fixture
    def mock_common(self):
        with patch('confluent_kafka.aio.producer._AIOProducer._common') as mock:

            async def mock_async_call(executor, blocking_task, *args, **kwargs):
                return blocking_task(*args, **kwargs)

            mock.async_call.side_effect = mock_async_call
            yield mock

    @pytest.fixture
    def basic_config(self):
        return {'bootstrap.servers': 'localhost:9092'}

    @pytest.mark.asyncio
    async def test_constructor_behavior(self, mock_producer, mock_common, basic_config):
        custom_executor = concurrent.futures.ThreadPoolExecutor(max_workers=5)
        try:
            producer1 = AIOProducer(basic_config, max_workers=3, executor=custom_executor)

            assert producer1.executor is custom_executor
            assert producer1.executor._max_workers == 5
            assert producer1._is_closed is False
            assert hasattr(producer1, '_buffer_timeout_manager')
            assert hasattr(producer1, '_producer')

            producer2 = AIOProducer(basic_config, max_workers=2, batch_size=500)

            assert producer2.executor is not custom_executor
            assert isinstance(producer2.executor, concurrent.futures.ThreadPoolExecutor)
            assert producer2.executor._max_workers == 2

            assert producer2._batch_size == 500
            assert producer2._is_closed is False
            assert hasattr(producer2, '_buffer_timeout_manager')

            await producer2.close()
            assert producer2._is_closed is True

        finally:
            custom_executor.shutdown(wait=True)

    @pytest.mark.asyncio
    async def test_close_method(self, mock_producer, mock_common, basic_config):
        producer = AIOProducer(basic_config)
        assert producer._is_closed is False

        await producer.close()
        assert producer._is_closed is True

        producer2 = AIOProducer(basic_config)
        assert producer2._is_closed is False

        await producer2.close()
        await producer2.close()
        assert producer2._is_closed is True

    @pytest.mark.asyncio
    async def test_call_method_executor_usage(self, mock_producer, mock_common, basic_config):
        producer = AIOProducer(basic_config)

        mock_method = Mock(return_value="test_result")
        result = await producer._call(mock_method, "arg1", kwarg1="value1")

        mock_method.assert_called_once_with("arg1", kwarg1="value1")
        assert result == "test_result"

    @pytest.mark.asyncio
    async def test_produce_success(self, mock_producer, mock_common, basic_config):
        producer = AIOProducer(basic_config, batch_size=1)
        mock_msg = Mock()

        async def mock_flush_buffer(target_topic=None):
            batches = producer._batch_processor.create_batches(target_topic)
            for batch in batches:
                for future in batch.futures:
                    if not future.done():
                        future.set_result(mock_msg)
            producer._batch_processor.clear_buffer()

        with patch.object(producer, '_flush_buffer', side_effect=mock_flush_buffer):
            result_future = await producer.produce(topic="test_topic", value="test_value")
            result = await result_future
            assert result is mock_msg

        await producer.close()

    @pytest.mark.asyncio
    async def test_produce_error(self, mock_producer, mock_common, basic_config):
        producer = AIOProducer(basic_config, batch_size=1)
        mock_error = KafkaError(KafkaError._MSG_TIMED_OUT)

        async def mock_flush_buffer(target_topic=None):
            batches = producer._batch_processor.create_batches(target_topic)
            for batch in batches:
                for future in batch.futures:
                    if not future.done():
                        future.set_exception(KafkaException(mock_error))
            producer._batch_processor.clear_buffer()

        with patch.object(producer, '_flush_buffer', side_effect=mock_flush_buffer):
            result_future = await producer.produce(topic="test_topic", value="test_value")

            with pytest.raises(KafkaException):
                await result_future

        await producer.close()

    @pytest.mark.asyncio
    async def test_produce_with_delayed_callback(self, mock_producer, mock_common, basic_config):
        """Test that Future properly waits for delayed delivery callback with batching."""
        producer = AIOProducer(basic_config, batch_size=2)  # Need 2 messages to trigger flush

        batch_called = asyncio.Event()
        captured_messages = None

        def mock_produce_batch(topic, messages, partition=None):
            nonlocal captured_messages
            captured_messages = messages
            batch_called.set()
            # Don't call callbacks immediately - simulate real async behavior

        mock_producer.return_value.produce_batch.side_effect = mock_produce_batch
        mock_producer.return_value.poll.return_value = 1

        # Start first produce - won't trigger flush yet, but will return a Future
        first_future = await producer.produce(topic="test", value="test1")

        # The Future should be pending (not resolved yet)
        assert not first_future.done()

        # Add second message to trigger batch flush
        await producer.produce(topic="test", value="test2")

        # Wait for the batch operation to be called
        await asyncio.wait_for(batch_called.wait(), timeout=2.0)

        # Batch was called and should have captured messages with callbacks
        assert captured_messages is not None
        assert len(captured_messages) == 2
        assert not first_future.done()

        # Simulate delayed delivery callback (like from background polling)
        mock_msg = Mock()
        mock_msg.topic.return_value = "test"
        mock_msg.value.return_value = b"test1"

        # Call callback for first message (index 0)
        first_callback = captured_messages[0]['callback']
        first_callback(None, mock_msg)

        # Now the Future should resolve
        result = await first_future
        assert result == mock_msg

        await producer.close()

    @pytest.mark.asyncio
    async def test_buffer_timeout_background_task(self, mock_producer, mock_common, basic_config):
        """Test that buffer timeout task runs continuously in the background."""
        # Create producer with short timeout for testing
        producer = AIOProducer(basic_config, buffer_timeout=0.1)

        # Test that timeout task is created and running
        assert producer._buffer_timeout_manager._timeout_task is not None
        assert not producer._buffer_timeout_manager._timeout_task.done()
        assert producer._is_closed is False

        # Wait a bit to ensure task is running
        await asyncio.sleep(0.05)
        assert not producer._buffer_timeout_manager._timeout_task.done()

        # Close the producer
        await producer.close()

        # Verify task stops and producer is closed
        assert producer._is_closed is True
        await asyncio.sleep(0.1)  # Grace period for cleanup
        assert (
            producer._buffer_timeout_manager._timeout_task is None
            or producer._buffer_timeout_manager._timeout_task.done()
        )

    @pytest.mark.asyncio
    async def test_multiple_concurrent_produce(self, mock_producer, mock_common, basic_config):
        """Test multiple concurrent produce operations with batching."""
        producer = AIOProducer(basic_config, max_workers=3, batch_size=1)  # Force immediate flush

        completed_produces = []
        batch_call_count = 0

        def mock_produce_batch(topic, messages, partition=None):
            nonlocal batch_call_count
            batch_call_count += 1

            # Simulate successful delivery for each message in batch
            for i, msg_data in enumerate(messages):
                mock_msg = Mock()
                mock_msg.topic.return_value = topic
                mock_msg.value.return_value = (
                    msg_data['value'].encode() if isinstance(msg_data['value'], str) else msg_data['value']
                )

                completed_produces.append((topic, msg_data['value']))
                # Call the individual message callback
                if 'callback' in msg_data:
                    msg_data['callback'](None, mock_msg)

        mock_producer.return_value.produce_batch.side_effect = mock_produce_batch
        mock_producer.return_value.poll.return_value = 1

        # Start multiple produce operations concurrently
        tasks = [asyncio.create_task(producer.produce(topic="test", value=f"msg{i}")) for i in range(3)]

        # All tasks should complete successfully (tests real concurrency)
        results = await asyncio.gather(*tasks)

        # Verify all operations completed
        assert len(results) == 3
        assert all(result is not None for result in results)
        assert batch_call_count == 3  # Each message triggers its own batch due to batch_size=1
        assert len(completed_produces) == 3

        # Verify all messages were produced
        produced_values = [value for topic, value in completed_produces]
        expected_values = ["msg0", "msg1", "msg2"]
        assert sorted(produced_values) == sorted(expected_values)

        await producer.close()

    @pytest.mark.asyncio
    async def test_constructor_new_implementation(self, mock_producer, mock_common, basic_config):
        producer1 = AIOProducer(basic_config)
        assert producer1._batch_size == 1000
        assert isinstance(producer1.executor, concurrent.futures.ThreadPoolExecutor)
        assert hasattr(producer1, '_loop')
        assert hasattr(producer1, '_buffer_timeout_manager')
        assert producer1._batch_processor.is_buffer_empty()
        assert producer1._is_closed is False
        await producer1.close()

        custom_executor = concurrent.futures.ThreadPoolExecutor(max_workers=8)
        try:
            producer2 = AIOProducer(basic_config, executor=custom_executor, batch_size=500, buffer_timeout=10.0)
            assert producer2.executor is custom_executor
            assert producer2._batch_size == 500
            assert hasattr(producer2, '_buffer_timeout_manager')
            await producer2.close()
        finally:
            custom_executor.shutdown(wait=True)

    @pytest.mark.asyncio
    async def test_lifecycle_management_new_implementation(self, mock_producer, mock_common, basic_config):
        """Test lifecycle management for current implementation."""

        # Test close method with messages in buffer
        producer = AIOProducer(basic_config)

        # Add some messages to buffer
        with patch.object(producer, '_flush_buffer'):
            await producer.produce('test', 'msg1')
            assert producer._batch_processor.get_buffer_size() == 1

            # Test close method
            await producer.close()
            assert producer._is_closed is True
            assert (
                producer._buffer_timeout_manager._timeout_task is None
                or producer._buffer_timeout_manager._timeout_task.done()
            )

    @pytest.mark.asyncio
    async def test_buffer_timeout_task_management(self, mock_producer, mock_common, basic_config):
        """Test timeout task lifecycle and weak references."""

        # Test task creation and configuration
        producer = AIOProducer(basic_config, buffer_timeout=1.0)
        assert producer._buffer_timeout_manager._timeout_task is not None
        assert not producer._buffer_timeout_manager._timeout_task.done()
        assert producer._buffer_timeout_manager._timeout == 1.0
        assert producer._is_closed is False

        # Test task stops on close
        await producer.close()
        assert producer._is_closed is True
        assert (
            producer._buffer_timeout_manager._timeout_task is None
            or producer._buffer_timeout_manager._timeout_task.done()
        )

    @pytest.mark.asyncio
    async def test_buffer_timeout_behavior(self, mock_producer, mock_common, basic_config):
        """Test buffer activity tracking and timeout triggers."""

        # Test buffer activity tracking
        producer = AIOProducer(basic_config)
        initial_time = producer._buffer_timeout_manager._last_activity
        assert initial_time > 0

        # Activity updates on produce
        await asyncio.sleep(0.01)  # Ensure time difference
        with patch.object(producer, '_flush_buffer'):  # Prevent auto-flush
            await producer.produce('test', 'msg1')
        assert producer._buffer_timeout_manager._last_activity > initial_time

        await producer.close()

    @pytest.mark.asyncio
    async def test_poll_method_new_implementation(self, mock_producer, mock_common, basic_config):
        """Test poll method with different timeout scenarios."""
        producer = AIOProducer(basic_config)

        # Test timeout=0 (non-blocking)
        with patch.object(producer, '_call') as mock_call:
            await producer.poll(timeout=0)
            mock_call.assert_called_with(producer._producer.poll, 0)

        # Test positive timeout (blocking via ThreadPool)
        with patch.object(producer, '_call') as mock_call:
            await producer.poll(timeout=5)
            mock_call.assert_called_with(producer._producer.poll, 5)

        await producer.close()

    @pytest.mark.asyncio
    async def test_produce_method_batching(self, mock_producer, mock_common, basic_config):
        """Test produce method with batching behavior."""
        producer = AIOProducer(basic_config, batch_size=3)

        # Test basic produce adds to buffer
        with patch.object(producer, '_flush_buffer') as mock_flush:
            future1 = await producer.produce('topic1', 'value1', key='key1')
            assert producer._batch_processor.get_buffer_size() == 1
            assert len(producer._batch_processor._buffer_futures) == 1
            assert isinstance(future1, asyncio.Future)
            mock_flush.assert_not_called()  # Should not flush yet

        # Test batch size trigger (3rd message should trigger flush)
        with patch.object(producer, '_flush_buffer') as mock_flush:
            await producer.produce('topic1', 'value2')  # 2nd message
            await producer.produce('topic1', 'value3')  # 3rd message - should trigger flush
            mock_flush.assert_called()

        await producer.close()

    @pytest.mark.asyncio
    async def test_flush_and_purge_methods_new_implementation(self, mock_producer, mock_common, basic_config):
        """Test flush and purge methods for current implementation."""
        producer = AIOProducer(basic_config)

        # Add messages to buffer
        with patch.object(producer, '_flush_buffer'):  # Prevent auto-flush
            await producer.produce('test', 'msg1')
            await producer.produce('test', 'msg2')
        assert producer._batch_processor.get_buffer_size() == 2

        # Test purge clears buffers
        with patch.object(producer, '_call') as mock_call:
            await producer.purge()
            mock_call.assert_called_with(producer._producer.purge)

        assert producer._batch_processor.get_buffer_size() == 0
        assert len(producer._batch_processor._buffer_futures) == 0

        await producer.close()

    @pytest.mark.asyncio
    async def test_group_messages_by_topic(self, mock_producer, mock_common, basic_config):
        """Test message grouping by topic for batch processing."""
        producer = AIOProducer(basic_config)

        # Test empty buffer
        groups = producer._batch_processor._group_messages_by_topic_and_partition()
        assert groups == {}

        # Add mixed topic messages
        producer._batch_processor._message_buffer = [
            {'topic': 'topic1', 'value': 'msg1', 'user_callback': None},
            {'topic': 'topic2', 'value': 'msg2', 'user_callback': Mock()},
            {'topic': 'topic1', 'value': 'msg3', 'user_callback': None},
        ]
        producer._batch_processor._buffer_futures = [Mock(), Mock(), Mock()]

        groups = producer._batch_processor._group_messages_by_topic_and_partition()

        # Test grouping correctness
        assert len(groups) == 2
        assert ('topic1', -1) in groups and ('topic2', -1) in groups
        assert len(groups[('topic1', -1)]['messages']) == 2  # msg1, msg3
        assert len(groups[('topic2', -1)]['messages']) == 1  # msg2

        await producer.close()

    @pytest.mark.asyncio
    async def test_error_handling_new_implementation(self, mock_producer, mock_common, basic_config):
        """Test error handling in current implementation."""
        producer = AIOProducer(basic_config)

        # Test batch error propagation
        producer._batch_processor._message_buffer = [{'topic': 'test', 'value': 'msg', 'user_callback': None}]
        producer._batch_processor._buffer_futures = [asyncio.Future()]

        with patch.object(producer._batch_processor, 'flush_buffer', side_effect=Exception("Batch failed")):
            with pytest.raises(Exception, match="Batch failed"):
                await producer._flush_buffer()

        await producer.close()

    @pytest.mark.asyncio
    async def test_future_based_usage_pattern(self, mock_producer, mock_common, basic_config):
        """Test the recommended Future-based usage pattern instead of callbacks."""
        producer = AIOProducer(basic_config, batch_size=1)

        # Mock successful delivery
        mock_msg = Mock()
        mock_msg.topic.return_value = "test-topic"
        mock_msg.value.return_value = b"test-value"

        async def mock_flush_buffer(target_topic=None):
            batches = producer._batch_processor.create_batches(target_topic)
            for batch in batches:
                for future in batch.futures:
                    if not future.done():
                        future.set_result(mock_msg)
            producer._batch_processor.clear_buffer()

        with patch.object(producer, '_flush_buffer', side_effect=mock_flush_buffer):
            # Recommended usage: await the Future returned by produce()
            future = await producer.produce(topic="test-topic", value="test-value")
            result = await future

            # Verify the result
            assert result is mock_msg
            assert result.topic() == "test-topic"
            assert result.value() == b"test-value"

        await producer.close()

    @pytest.mark.asyncio
    async def test_future_based_error_handling(self, mock_producer, mock_common, basic_config):
        """Test Future-based error handling pattern."""
        producer = AIOProducer(basic_config, batch_size=1)

        # Mock delivery error
        mock_error = KafkaException(KafkaError(KafkaError._MSG_TIMED_OUT))

        async def mock_flush_buffer(target_topic=None):
            batches = producer._batch_processor.create_batches(target_topic)
            for batch in batches:
                for future in batch.futures:
                    if not future.done():
                        future.set_exception(mock_error)
            producer._batch_processor.clear_buffer()

        with patch.object(producer, '_flush_buffer', side_effect=mock_flush_buffer):
            # Recommended usage: handle exceptions via Future
            future = await producer.produce(topic="test-topic", value="test-value")

            with pytest.raises(KafkaException):
                await future

        await producer.close()

    @pytest.mark.asyncio
    async def test_future_based_concurrent_usage(self, mock_producer, mock_common, basic_config):
        """Test Future-based concurrent usage pattern."""
        producer = AIOProducer(basic_config, batch_size=1)

        # Mock successful delivery
        mock_msg = Mock()
        mock_msg.topic.return_value = "test-topic"
        mock_msg.value.return_value = b"test-value"

        async def mock_flush_buffer(target_topic=None):
            batches = producer._batch_processor.create_batches(target_topic)
            for batch in batches:
                for future in batch.futures:
                    if not future.done():
                        future.set_result(mock_msg)
            producer._batch_processor.clear_buffer()

        with patch.object(producer, '_flush_buffer', side_effect=mock_flush_buffer):
            # Recommended usage: collect Futures and await them together
            futures = []
            for i in range(5):
                future = await producer.produce(topic="test-topic", value=f"test-value-{i}")
                futures.append(future)

            # Wait for all deliveries to complete
            results = await asyncio.gather(*futures)

            # Verify all results
            assert len(results) == 5
            for result in results:
                assert result is mock_msg

        await producer.close()

    @pytest.mark.asyncio
    async def test_edge_cases_batching(self, mock_producer, mock_common, basic_config):
        """Test edge cases in batching behavior."""
        producer = AIOProducer(basic_config, batch_size=100)

        # Test large batch handling
        with patch.object(producer, '_flush_buffer') as mock_flush:
            large_batch_tasks = [producer.produce('test', f'msg{i}') for i in range(150)]  # Exceeds batch_size

            # Should trigger flush automatically at 100
            await asyncio.gather(*large_batch_tasks)
            assert mock_flush.call_count >= 1  # At least one flush

        await producer.close()

    @pytest.mark.asyncio
    async def test_aio_producer_len_with_buffered_messages(self, mock_producer, mock_common, basic_config):
        """Test that __len__ counts messages in async batch buffer"""
        producer = AIOProducer(basic_config, batch_size=10, buffer_timeout=0)

        # Produce 5 messages (less than batch_size, so they stay in buffer)
        with patch.object(producer, '_flush_buffer') as mock_flush:  # Prevent auto-flush
            for i in range(5):
                await producer.produce('test-topic', value=f'msg-{i}'.encode())

        # Verify flush was not called (messages stayed in buffer)
        mock_flush.assert_not_called()

        # len() should count messages in buffer
        assert len(producer) == 5
        assert producer._batch_processor.get_buffer_size() == 5
        assert len(producer._producer) == 0  # Nothing in librdkafka yet
        # Verify len() equals the sum
        assert len(producer) == producer._batch_processor.get_buffer_size() + len(producer._producer)

        await producer.close()

    @pytest.mark.asyncio
    async def test_aio_producer_len_after_flush(self, mock_producer, mock_common, basic_config):
        """Test that __len__ counts messages after flush to librdkafka"""
        producer = AIOProducer(basic_config, batch_size=10, buffer_timeout=0)

        # Produce and flush
        with patch.object(producer, '_flush_buffer'):  # Prevent auto-flush
            for i in range(5):
                await producer.produce('test-topic', value=f'msg-{i}'.encode())

        # Flush to move messages to librdkafka
        await producer.flush()

        # After flush, messages move to librdkafka queue
        # Verify len() correctly equals the sum of buffer + librdkafka
        buffer_count = producer._batch_processor.get_buffer_size()
        librdkafka_count = len(producer._producer)
        total_len = len(producer)

        # Buffer should be empty after flush
        assert buffer_count == 0
        # Verify len() equals the sum (this validates the __len__ implementation)
        assert total_len == buffer_count + librdkafka_count
        # Messages should be in librdkafka queue after flush
        assert total_len == librdkafka_count

        await producer.close()

    @pytest.mark.asyncio
    async def test_aio_producer_len_closed_producer(self, mock_producer, mock_common, basic_config):
        """Test that __len__ returns 0 for closed producer"""
        producer = AIOProducer(basic_config, batch_size=10, buffer_timeout=0)

        # Produce some messages
        with patch.object(producer, '_flush_buffer'):  # Prevent auto-flush
            for i in range(3):
                await producer.produce('test-topic', value=f'msg-{i}'.encode())

        # Verify messages are there
        assert len(producer) == 3

        # Close producer
        await producer.close()

        # len() should return 0 for closed producer
        assert len(producer) == 0

    @pytest.mark.asyncio
    async def test_aio_producer_len_mixed_state(self, mock_producer, mock_common, basic_config):
        """Test __len__ when messages are in both buffer and librdkafka queue"""
        producer = AIOProducer(basic_config, batch_size=5, buffer_timeout=0)

        # Produce 7 messages - first 5 should flush (batch_size=5), last 2 stay in buffer
        with patch.object(producer, '_flush_buffer') as mock_flush:
            for i in range(7):
                await producer.produce('test-topic', value=f'msg-{i}'.encode())

        # With batch_size=5, flush should be called after 5th message
        # Verify flush was called (at least once when batch_size reached)
        assert mock_flush.call_count >= 1

        # After batch_size messages, some may have flushed
        # Total should be sum of buffer + librdkafka queue
        buffer_count = producer._batch_processor.get_buffer_size()
        librdkafka_count = len(producer._producer)
        total_count = len(producer)

        # Verify len() correctly equals the sum (this validates the __len__ implementation)
        assert total_count == buffer_count + librdkafka_count
        # At least the messages beyond batch_size should be in buffer
        # (exact count depends on flush behavior)
        assert total_count >= 2  # At least the last 2 should be pending

        await producer.close()
