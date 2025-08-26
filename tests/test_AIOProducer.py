#!/usr/bin/env python
# -*- coding: utf-8 -*-
import pytest
import asyncio
import concurrent.futures
from unittest.mock import Mock, MagicMock, patch, AsyncMock

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
    async def test_constructor(self, mock_producer, mock_common, basic_config):
        """Test constructor with all parameter combinations."""
        # Test with all custom parameters
        custom_executor = concurrent.futures.ThreadPoolExecutor(max_workers=5)
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
        assert producer.executor._max_workers == 5  # From custom executor, not max_workers param

        # Assert auto-polling configuration
        assert producer._running is False
        assert not hasattr(producer, '_running_loop')

        # Assert callback wrapping was called correctly
        args = mock_common.wrap_common_callbacks.call_args[0]
        assert len(args) == 2  # loop, config
        assert args[1] is config_with_callbacks

        custom_executor.shutdown()


@pytest.mark.asyncio
async def test_stop_method():
    """Test stop method functionality."""
    with patch('confluent_kafka.aio._AIOProducer.confluent_kafka.Producer'), \
         patch('confluent_kafka.aio._AIOProducer._common'):

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
async def test_call_method_executor_usage():
    """Test that _call method uses ThreadPoolExecutor properly."""
    with patch('confluent_kafka.aio._AIOProducer.confluent_kafka.Producer'), \
         patch('confluent_kafka.aio._AIOProducer._common'):

        producer = AIOProducer({'bootstrap.servers': 'localhost:9092'}, auto_poll=False)

        mock_method = Mock(return_value="test_result")
        result = await producer._call(mock_method, "arg1", kwarg1="value1")

        mock_method.assert_called_once_with("arg1", kwarg1="value1")
        assert result == "test_result"


@pytest.mark.asyncio
async def test_produce_success():
    """Test successful message production."""
    with patch('confluent_kafka.aio._AIOProducer.confluent_kafka.Producer'), \
         patch('confluent_kafka.aio._AIOProducer._common'):

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
async def test_produce_error():
    """Test message production with error."""
    with patch('confluent_kafka.aio._AIOProducer.confluent_kafka.Producer'), \
         patch('confluent_kafka.aio._AIOProducer._common'):

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
async def test_produce_callback_overriding():
    """Test that produce method overrides on_delivery callback."""
    with patch('confluent_kafka.aio._AIOProducer.confluent_kafka.Producer'), \
         patch('confluent_kafka.aio._AIOProducer._common'):

        producer = AIOProducer({'bootstrap.servers': 'localhost:9092'}, auto_poll=False)

        original_callback = Mock()
        captured_callback = None

        def mock_produce(*args, **kwargs):
            nonlocal captured_callback
            captured_callback = kwargs.get('on_delivery')

        producer._producer.produce = mock_produce

        produce_task = asyncio.create_task(
            producer.produce(topic="test", value="test", on_delivery=original_callback)
        )

        await asyncio.sleep(0.01)  # Give time for produce to be called

        assert captured_callback is not original_callback
        assert captured_callback is not None

        # Complete the production
        mock_msg = Mock()
        captured_callback(None, mock_msg)

        result = await produce_task
        assert result == mock_msg


@pytest.mark.asyncio
async def test_auto_polling_background_loop():
    """Test that auto-polling runs continuously in the background."""
    with patch('confluent_kafka.aio._AIOProducer.confluent_kafka.Producer') as mock_producer_class, \
         patch('confluent_kafka.aio._AIOProducer._common'):

        mock_producer = Mock()
        mock_producer_class.return_value = mock_producer

        # Create producer with auto-polling enabled
        producer = AIOProducer({'bootstrap.servers': 'localhost:9092'}, auto_poll=True)

        assert producer._running is True
        assert hasattr(producer, '_running_loop')

        # Let the background loop run for a bit
        await asyncio.sleep(0.1)

        # Poll should have been called multiple times
        assert mock_producer.poll.call_count > 0

        # Stop the producer
        await producer.stop()

        # Verify polling stops
        poll_count_after_stop = mock_producer.poll.call_count
        await asyncio.sleep(0.1)
        assert mock_producer.poll.call_count == poll_count_after_stop


@pytest.mark.asyncio
async def test_multiple_concurrent_produce():
    """Test multiple concurrent produce operations."""
    with patch('confluent_kafka.aio._AIOProducer.confluent_kafka.Producer'), \
         patch('confluent_kafka.aio._AIOProducer._common'):

        producer = AIOProducer({'bootstrap.servers': 'localhost:9092'}, auto_poll=False)

        # Track all callbacks
        callbacks = []

        def mock_produce(*args, **kwargs):
            callback = kwargs.get('on_delivery')
            if callback:
                callbacks.append(callback)

        producer._producer.produce = mock_produce

        # Start multiple produce operations concurrently
        tasks = [
            asyncio.create_task(producer.produce(topic="test", value=f"msg{i}"))
            for i in range(3)
        ]

        # Let all produce calls complete
        await asyncio.sleep(0.01)

        # Should have 3 callbacks
        assert len(callbacks) == 3

        # Complete all deliveries
        mock_msgs = [Mock(value=f"msg{i}") for i in range(3)]
        for i, callback in enumerate(callbacks):
            callback(None, mock_msgs[i])

        # All tasks should complete successfully
        results = await asyncio.gather(*tasks)
        assert len(results) == 3
        assert all(result is not None for result in results)


@pytest.mark.asyncio
async def test_produce_with_delayed_callback():
    """Test that Future waits for delayed delivery callback."""
    with patch('confluent_kafka.aio._AIOProducer.confluent_kafka.Producer'), \
         patch('confluent_kafka.aio._AIOProducer._common'):

        producer = AIOProducer({'bootstrap.servers': 'localhost:9092'}, auto_poll=False)

        captured_callback = None

        def mock_produce(*args, **kwargs):
            nonlocal captured_callback
            captured_callback = kwargs.get('on_delivery')

        producer._producer.produce = mock_produce

        # Start produce but don't await yet
        produce_task = asyncio.create_task(
            producer.produce(topic="test", value="test")
        )

        # Give time for produce to be called
        await asyncio.sleep(0.01)

        # Produce should be called but task not completed yet
        assert captured_callback is not None
        assert not produce_task.done()

        # Simulate delayed delivery callback
        await asyncio.sleep(0.05)
        mock_msg = Mock()
        captured_callback(None, mock_msg)

        # Now the task should complete
        result = await produce_task
        assert result == mock_msg


@pytest.mark.asyncio
async def test_executor_shutdown_behavior():
    """Test proper executor cleanup."""
    with patch('confluent_kafka.aio._AIOProducer.confluent_kafka.Producer'), \
         patch('confluent_kafka.aio._AIOProducer._common'):

        # Test with default executor
        producer = AIOProducer({'bootstrap.servers': 'localhost:9092'},
                              max_workers=2, auto_poll=False)

        # Executor should be created
        assert isinstance(producer.executor, concurrent.futures.ThreadPoolExecutor)
        assert producer.executor._max_workers == 2

        # Should be able to run tasks
        test_callable = Mock(return_value="success")
        result = await producer._call(test_callable)
        assert result == "success"

        # Manual cleanup for test (in real usage, this would be handled by context managers)
        producer.executor.shutdown(wait=True)


@pytest.mark.asyncio
async def test_thread_safety_callback_handling():
    """Test thread-safe callback handling between executor and event loop."""
    with patch('confluent_kafka.aio._AIOProducer.confluent_kafka.Producer'), \
         patch('confluent_kafka.aio._AIOProducer._common'):

        producer = AIOProducer({'bootstrap.servers': 'localhost:9092'}, auto_poll=False)

        # Mock the event loop and track call_soon_threadsafe calls
        mock_loop = Mock()
        mock_future = Mock()
        mock_loop.create_future.return_value = mock_future

        captured_callback = None

        def mock_produce(*args, **kwargs):
            nonlocal captured_callback
            captured_callback = kwargs.get('on_delivery')

        producer._producer.produce = mock_produce

        with patch('asyncio.get_event_loop', return_value=mock_loop):
            # Start produce operation
            _ = asyncio.create_task(producer.produce(topic="test", value="test"))
            await asyncio.sleep(0.01)

            # Verify Future was created
            mock_loop.create_future.assert_called_once()
            assert captured_callback is not None

            # Simulate delivery callback being called from different thread
            # This should trigger call_soon_threadsafe
            mock_msg = Mock()
            captured_callback(None, mock_msg)

            # Verify call_soon_threadsafe was used for thread-safe callback handling
            assert mock_loop.call_soon_threadsafe.call_count >= 1

            # Verify the callback was set_result (successful delivery)
            call_args = mock_loop.call_soon_threadsafe.call_args_list[0][0]
            assert call_args[0] == mock_future.set_result
            assert call_args[1] == mock_msg
