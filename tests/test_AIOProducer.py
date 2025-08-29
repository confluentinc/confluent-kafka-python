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


    async def wait_for_condition(self, condition_func, timeout=1.0):
        """Wait for a condition to become true with timeout."""
        start_time = asyncio.get_event_loop().time()
        while asyncio.get_event_loop().time() - start_time < timeout:
            if condition_func():
                return True
            await asyncio.sleep(0.01)
        return False


    @pytest.mark.asyncio
    async def test_constructor(self, mock_producer, mock_common, basic_config):
        """Test constructor with all parameter combinations."""
        custom_executor = concurrent.futures.ThreadPoolExecutor(max_workers=5)
        try:
            config_with_callbacks = {
                **basic_config,
                'error_cb': lambda err: None,
                'stats_cb': lambda stats: None
            }

            producer = AIOProducer(
                config_with_callbacks,
                max_workers=3,  # This should be ignored since executor is provided
                executor=custom_executor,
                auto_poll=False
            )

            # Assert underlying producer creation
            mock_producer.assert_called_with(config_with_callbacks)
            mock_common.wrap_common_callbacks.assert_called()

            # Assert executor configuration (custom executor takes precedence)
            assert producer.executor is custom_executor
            assert producer.executor._max_workers == 5

            # Assert auto-polling configuration
            assert producer._running is False
            assert not hasattr(producer, '_running_loop')

            # Assert callback wrapping was called correctly
            args = mock_common.wrap_common_callbacks.call_args[0]
            assert len(args) == 2
            assert args[1] is config_with_callbacks

        finally:
            custom_executor.shutdown(wait=True)

    @pytest.mark.asyncio
    async def test_stop_method(self, mock_producer, mock_common):
        """Test stop method functionality."""
        # Test stopping running producer
        producer = AIOProducer({'bootstrap.servers': 'localhost:9092'})
        assert producer._running is True

        await producer.stop()
        assert producer._running is False

        # Test stopping non-running producer
        producer2 = AIOProducer({'bootstrap.servers': 'localhost:9092'}, auto_poll=False)
        assert producer2._running is False

        await producer2.stop()  # Should not raise exception
        assert producer2._running is False

    @pytest.mark.asyncio
    async def test_call_method_executor_usage(self, mock_producer, mock_common):
        """Test that _call method uses ThreadPoolExecutor for async-to-sync bridging."""
        producer = AIOProducer({'bootstrap.servers': 'localhost:9092'}, auto_poll=False)

        mock_method = Mock(return_value="test_result")
        result = await producer._call(mock_method, "arg1", kwarg1="value1")

        mock_method.assert_called_once_with("arg1", kwarg1="value1")
        assert result == "test_result"

    @pytest.mark.asyncio
    async def test_produce_success(self, mock_producer, mock_common):
        """Test successful message production."""
        producer = AIOProducer({'bootstrap.servers': 'localhost:9092'}, auto_poll=False)

        def mock_produce(*args, **kwargs):
            callback = kwargs.get('on_delivery')
            if callback:
                mock_msg = Mock()
                callback(None, mock_msg)  # err=None, msg=mock_msg

        producer._producer.produce = mock_produce
        result = await producer.produce(topic="test_topic", value="test_value")

        assert result is not None

    @pytest.mark.asyncio
    async def test_produce_error(self, mock_producer, mock_common):
        """Test message production error handling."""
        producer = AIOProducer({'bootstrap.servers': 'localhost:9092'}, auto_poll=False)

        def mock_produce(*args, **kwargs):
            callback = kwargs.get('on_delivery')
            if callback:
                mock_error = KafkaError(KafkaError._MSG_TIMED_OUT)
                callback(mock_error, None)  # err=mock_error, msg=None

        producer._producer.produce = mock_produce

        with pytest.raises(KafkaException):
            await producer.produce(topic="test_topic", value="test_value")

    @pytest.mark.asyncio
    async def test_produce_callback_overriding(self, mock_producer, mock_common):
        """Test that produce method overrides on_delivery callback for Future resolution."""
        producer = AIOProducer({'bootstrap.servers': 'localhost:9092'}, auto_poll=False)

        original_callback = Mock()
        produce_called = asyncio.Event()
        captured_callback = None

        def mock_produce(*args, **kwargs):
            nonlocal captured_callback
            captured_callback = kwargs.get('on_delivery')
            produce_called.set()

        producer._producer.produce = mock_produce

        produce_task = asyncio.create_task(
            producer.produce(topic="test", value="test", on_delivery=original_callback)
        )

        # Wait for produce to be called
        await asyncio.wait_for(produce_called.wait(), timeout=1.0)

        assert captured_callback is not original_callback
        assert captured_callback is not None

        # Complete the production
        mock_msg = Mock()
        captured_callback(None, mock_msg)

        result = await produce_task
        assert result == mock_msg

    @pytest.mark.asyncio
    async def test_produce_with_delayed_callback(self, mock_producer, mock_common):
        """Test that Future waits for delayed delivery callback."""
        producer = AIOProducer({'bootstrap.servers': 'localhost:9092'}, auto_poll=False)

        produce_called = asyncio.Event()
        captured_callback = None

        def mock_produce(*args, **kwargs):
            nonlocal captured_callback
            captured_callback = kwargs.get('on_delivery')
            produce_called.set()

        producer._producer.produce = mock_produce

        # Start produce but don't await yet
        produce_task = asyncio.create_task(
            producer.produce(topic="test", value="test")
        )

        # Wait for produce to be called
        await asyncio.wait_for(produce_called.wait(), timeout=2.0)

        # Produce should be called but task not completed yet
        assert captured_callback is not None
        assert not produce_task.done()

        # Simulate delayed delivery callback
        mock_msg = Mock()
        captured_callback(None, mock_msg)

        # Now the task should complete
        result = await produce_task
        assert result == mock_msg

    @pytest.mark.asyncio
    async def test_auto_polling_background_loop(self, mock_producer, mock_common):
        """Test that auto-polling runs continuously in the background."""
        mock_producer.return_value.poll.return_value = 0

        # Track polling with events
        first_poll = asyncio.Event()
        multiple_polls = asyncio.Event()
        poll_count = 0

        def poll_tracker(*args, **kwargs):
            nonlocal poll_count
            poll_count += 1
            if poll_count == 1:
                first_poll.set()
            elif poll_count >= 3:
                multiple_polls.set()
            return 0

        mock_producer.return_value.poll.side_effect = poll_tracker

        # Create producer with auto-polling enabled
        producer = AIOProducer({'bootstrap.servers': 'localhost:9092'}, auto_poll=True)

        try:
            assert producer._running is True
            assert hasattr(producer, '_running_loop')

            # Wait for polling to start and continue
            await asyncio.wait_for(first_poll.wait(), timeout=1.0)
            assert poll_count >= 1

            await asyncio.wait_for(multiple_polls.wait(), timeout=2.0)
            assert poll_count >= 3

        finally:
            # Stop the producer
            await producer.stop()

        # Verify polling stops
        final_count = poll_count
        await asyncio.sleep(0.1)  # Grace period for cleanup
        assert poll_count - final_count <= 1  # Allow one final poll

    @pytest.mark.asyncio
    async def test_multiple_concurrent_produce(self, mock_producer, mock_common):
        """Test multiple concurrent produce operations."""
        producer = AIOProducer({'bootstrap.servers': 'localhost:9092'}, auto_poll=False)

        callbacks = []
        all_produced = asyncio.Event()

        def mock_produce(*args, **kwargs):
            callback = kwargs.get('on_delivery')
            if callback:
                callbacks.append(callback)
                if len(callbacks) == 3:
                    all_produced.set()

        producer._producer.produce = mock_produce

        # Start multiple produce operations concurrently
        tasks = [
            asyncio.create_task(producer.produce(topic="test", value=f"msg{i}"))
            for i in range(3)
        ]

        # Wait for all produce calls to complete
        await asyncio.wait_for(all_produced.wait(), timeout=1.0)
        assert len(callbacks) == 3

        # Complete all deliveries
        mock_msgs = [Mock(value=f"msg{i}") for i in range(3)]
        for i, callback in enumerate(callbacks):
            callback(None, mock_msgs[i])

        # All tasks should complete successfully
        results = await asyncio.gather(*tasks)
        assert len(results) == 3
        assert all(result is not None for result in results)
