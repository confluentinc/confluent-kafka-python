#!/usr/bin/env python
# -*- coding: utf-8 -*-
import pytest
import asyncio
import concurrent.futures
from unittest.mock import Mock, patch

from confluent_kafka import KafkaError, KafkaException
from confluent_kafka.aio._AIOProducer import AIOProducer


class TestAIOProducer:
    """Unit tests for AIOProducer class."""

    @pytest.fixture
    def mock_producer(self):
        """Mock the underlying confluent_kafka.Producer."""
        with patch('confluent_kafka.aio._AIOProducer.confluent_kafka.Producer') as mock:
            yield mock

    @pytest.fixture
    def mock_common(self):
        """Mock the _common module callback wrapping."""
        with patch('confluent_kafka.aio._AIOProducer._common') as mock:
            yield mock

    @pytest.fixture
    def basic_config(self):
        """Basic producer configuration."""
        return {'bootstrap.servers': 'localhost:9092'}

    @pytest.mark.asyncio
    async def test_constructor_behavior(self, mock_producer, mock_common, basic_config):
        """Test constructor creates producer with correct configuration and behavior."""
        custom_executor = concurrent.futures.ThreadPoolExecutor(max_workers=5)
        try:
            # Test 1: Constructor with custom executor and auto_poll=False
            producer1 = AIOProducer(
                basic_config,
                max_workers=3,  # Should be ignored since executor is provided
                executor=custom_executor,
                auto_poll=False
            )

            # Test actual object state, not mock calls
            assert producer1.executor is custom_executor
            assert producer1.executor._max_workers == 5
            assert producer1._running is False
            assert not hasattr(producer1, '_running_loop')
            assert hasattr(producer1, '_producer')  # Should have underlying producer

            # Test 2: Constructor with default executor and auto_poll=True
            producer2 = AIOProducer(basic_config, max_workers=2, auto_poll=True)

            # Test executor was created with correct max_workers
            assert producer2.executor is not custom_executor
            assert isinstance(producer2.executor, concurrent.futures.ThreadPoolExecutor)
            assert producer2.executor._max_workers == 2

            # Test auto-polling was started
            assert producer2._running is True
            assert hasattr(producer2, '_running_loop')
            assert not producer2._running_loop.done()

            # Clean up
            await producer2.stop()
            assert producer2._running is False

        finally:
            custom_executor.shutdown(wait=True)

    @pytest.mark.asyncio
    async def test_stop_method(self, mock_producer, mock_common, basic_config):
        """Test stop method functionality."""
        # Test stopping running producer
        producer = AIOProducer(basic_config)
        assert producer._running is True

        await producer.stop()
        assert producer._running is False

        # Test stopping non-running producer
        producer2 = AIOProducer(basic_config, auto_poll=False)
        assert producer2._running is False

        await producer2.stop()  # Should not raise exception
        assert producer2._running is False

    @pytest.mark.asyncio
    async def test_call_method_executor_usage(self, mock_producer, mock_common, basic_config):
        """Test that _call method uses ThreadPoolExecutor for async-to-sync bridging."""
        producer = AIOProducer(basic_config, auto_poll=False)

        mock_method = Mock(return_value="test_result")
        result = await producer._call(mock_method, "arg1", kwarg1="value1")

        mock_method.assert_called_once_with("arg1", kwarg1="value1")
        assert result == "test_result"

    @pytest.mark.asyncio
    async def test_produce_success(self, mock_producer, mock_common, basic_config):
        """Test successful message production."""
        producer = AIOProducer(basic_config, auto_poll=False)

        mock_msg = Mock()
        loop = asyncio.get_event_loop()

        def sync_produce(*args, **kwargs):
            callback = kwargs.get('on_delivery')
            if callback:
                # Simulate successful delivery - callback would normally come from poll()
                loop.call_soon_threadsafe(callback, None, mock_msg)

        mock_producer.return_value.produce.side_effect = sync_produce
        result = await producer.produce(topic="test_topic", value="test_value")

        assert result is mock_msg

    @pytest.mark.asyncio
    async def test_produce_error(self, mock_producer, mock_common, basic_config):
        """Test message production error handling through real ThreadPoolExecutor."""
        producer = AIOProducer(basic_config, auto_poll=False)

        # Capture the main event loop to use from thread
        main_loop = asyncio.get_event_loop()

        def sync_produce(*args, **kwargs):
            callback = kwargs.get('on_delivery')
            if callback:
                mock_error = KafkaError(KafkaError._MSG_TIMED_OUT)
                # Simulate error delivery - callback would normally come from poll()
                main_loop.call_soon_threadsafe(callback, mock_error, None)

        mock_producer.return_value.produce.side_effect = sync_produce

        # This should go through real _call() method and ThreadPoolExecutor
        with pytest.raises(KafkaException):
            await producer.produce(topic="test_topic", value="test_value")

        # Verify the sync producer.produce was actually called
        mock_producer.return_value.produce.assert_called_once()

    @pytest.mark.asyncio
    async def test_produce_with_delayed_callback(self, mock_producer, mock_common, basic_config):
        """Test that Future properly waits for delayed delivery callback through real ThreadPoolExecutor."""
        producer = AIOProducer(basic_config, auto_poll=False)

        produce_called = asyncio.Event()
        captured_callback = None

        def sync_produce(*args, **kwargs):
            nonlocal captured_callback
            # This runs in real ThreadPoolExecutor
            captured_callback = kwargs.get('on_delivery')
            produce_called.set()
            # Don't call callback immediately - simulate real async behavior

        mock_producer.return_value.produce.side_effect = sync_produce

        # Start produce but don't await yet - this tests real Future behavior
        produce_task = asyncio.create_task(
            producer.produce(topic="test", value="test")
        )

        # Wait for the ThreadPoolExecutor to execute the produce call
        await asyncio.wait_for(produce_called.wait(), timeout=2.0)

        # Produce was called but Future should still be waiting for callback
        assert captured_callback is not None
        assert not produce_task.done()

        # Simulate delayed delivery callback (like from background polling)
        mock_msg = Mock()
        mock_msg.topic.return_value = "test"
        mock_msg.value.return_value = b"test"
        captured_callback(None, mock_msg)

        # Now the Future should resolve
        result = await produce_task
        assert result == mock_msg

    @pytest.mark.asyncio
    async def test_auto_polling_background_loop(self, mock_producer, mock_common, basic_config):
        """Test that auto-polling runs continuously in the background."""
        mock_producer.return_value.poll.return_value = 0

        # Track polling with event
        multiple_polls = asyncio.Event()
        poll_count = 0

        def poll_tracker(*args, **kwargs):
            nonlocal poll_count
            poll_count += 1
            if poll_count >= 5:
                multiple_polls.set()
            return 0

        mock_producer.return_value.poll.side_effect = poll_tracker

        # Create producer with auto-polling enabled
        producer = AIOProducer(basic_config, auto_poll=True)

        try:
            assert producer._running is True
            assert hasattr(producer, '_running_loop')

            # Wait for continuous polling (proves both start and continuity)
            await asyncio.wait_for(multiple_polls.wait(), timeout=2.0)
            assert poll_count >= 5

        finally:
            # Stop the producer
            await producer.stop()

        # Verify polling stops
        final_count = poll_count
        await asyncio.sleep(0.1)  # Grace period for cleanup
        assert poll_count - final_count <= 1  # Allow one final poll

    @pytest.mark.asyncio
    async def test_multiple_concurrent_produce(self, mock_producer, mock_common, basic_config):
        """Test multiple concurrent produce operations through real ThreadPoolExecutor."""
        producer = AIOProducer(basic_config, auto_poll=False, max_workers=3)

        completed_produces = []
        produce_call_count = 0

        # Capture the main event loop to use from thread
        main_loop = asyncio.get_event_loop()

        def sync_produce(*args, **kwargs):
            nonlocal produce_call_count
            produce_call_count += 1

            # This runs in ThreadPoolExecutor - simulate real produce call
            callback = kwargs.get('on_delivery')
            topic = kwargs.get('topic')
            value = kwargs.get('value')

            if callback:
                # Create mock message for this specific produce call
                mock_msg = Mock()
                mock_msg.topic.return_value = topic
                mock_msg.value.return_value = value.encode() if isinstance(value, str) else value

                # Simulate successful delivery from background thread
                def deliver_callback():
                    completed_produces.append((topic, value))
                    callback(None, mock_msg)

                # Simulate slight delay like real produce would have
                main_loop.call_later(0.01, deliver_callback)

        mock_producer.return_value.produce.side_effect = sync_produce

        # Start multiple produce operations concurrently
        tasks = [
            asyncio.create_task(producer.produce(topic="test", value=f"msg{i}"))
            for i in range(3)
        ]

        # All tasks should complete successfully (tests real concurrency)
        results = await asyncio.gather(*tasks)

        # Verify all operations completed
        assert len(results) == 3
        assert all(result is not None for result in results)
        assert produce_call_count == 3
        assert len(completed_produces) == 3

        # Verify all messages were produced
        produced_values = [value for topic, value in completed_produces]
        expected_values = ["msg0", "msg1", "msg2"]
        assert sorted(produced_values) == sorted(expected_values)
