#!/usr/bin/env python
# -*- coding: utf-8 -*-
import pytest
import asyncio
import concurrent.futures
from unittest.mock import Mock, patch, AsyncMock

from confluent_kafka import KafkaError, KafkaException, TopicPartition
from confluent_kafka.aio._AIOConsumer import AIOConsumer


class TestAIOConsumer:
    """Unit tests for AIOConsumer class - focusing on async wrapper logic."""

    @pytest.fixture
    def mock_consumer(self):
        """Mock the underlying confluent_kafka.Consumer."""
        with patch('confluent_kafka.aio._AIOConsumer.confluent_kafka.Consumer') as mock:
            yield mock

    @pytest.fixture
    def mock_common(self):
        """Mock the _common module callback wrapping."""
        with patch('confluent_kafka.aio._AIOConsumer._common') as mock:
            yield mock

    @pytest.fixture
    def basic_config(self):
        """Basic consumer configuration."""
        return {
            'bootstrap.servers': 'localhost:9092',
            'group.id': 'test-group',
            'auto.offset.reset': 'earliest'
        }

    @pytest.mark.asyncio
    async def test_constructor(self, mock_consumer, mock_common, basic_config):
        """Test constructor with all parameter combinations."""
        custom_executor = concurrent.futures.ThreadPoolExecutor(max_workers=4)
        try:
            config_with_callbacks = {
                **basic_config,
                'error_cb': lambda err: None,
                'on_commit': AsyncMock()
            }

            consumer = AIOConsumer(
                config_with_callbacks,
                max_workers=2,  # Should be ignored since executor is provided
                executor=custom_executor
            )

            # Assert underlying consumer creation
            mock_consumer.assert_called_with(config_with_callbacks)
            mock_common.wrap_common_callbacks.assert_called()
            mock_common.wrap_conf_callback.assert_called_with(
                asyncio.get_event_loop(), config_with_callbacks, 'on_commit'
            )

            # Assert executor configuration (custom executor takes precedence)
            assert consumer.executor is custom_executor
            assert consumer.executor._max_workers == 4

        finally:
            custom_executor.shutdown(wait=True)

    @pytest.mark.asyncio
    async def test_constructor_invalid_max_workers(self, mock_consumer, mock_common, basic_config):
        """Test constructor validation logic for max_workers."""
        with pytest.raises(ValueError, match="max_workers must be at least 1"):
            AIOConsumer(basic_config, max_workers=0)

    @pytest.mark.asyncio
    async def test_call_method_executor_usage(self, mock_consumer, mock_common, basic_config):
        """Test that _call method properly uses ThreadPoolExecutor for async-to-sync bridging."""
        consumer = AIOConsumer(basic_config, max_workers=2)

        mock_method = Mock(return_value="test_result")
        result = await consumer._call(mock_method, "arg1", kwarg1="value1")

        mock_method.assert_called_once_with("arg1", kwarg1="value1")
        assert result == "test_result"

    @pytest.mark.asyncio
    async def test_poll_success(self, mock_consumer, mock_common, basic_config):
        """Test successful message polling."""
        consumer = AIOConsumer(basic_config, max_workers=2)

        # Create a mock message
        mock_message = Mock()
        mock_message.error.return_value = None  # No error
        mock_message.topic.return_value = "test-topic"
        mock_message.value.return_value = b"test message"

        # Mock the underlying consumer's poll method
        consumer._consumer.poll.return_value = mock_message

        # Test polling
        result = await consumer.poll(timeout=1.0)

        # Verify the result
        assert result is mock_message
        consumer._consumer.poll.assert_called_once_with(timeout=1.0)

    @pytest.mark.asyncio
    async def test_consume_success(self, mock_consumer, mock_common, basic_config):
        """Test successful message consumption."""
        consumer = AIOConsumer(basic_config, max_workers=2)

        # Create mock messages
        mock_messages = [Mock(), Mock()]
        for i, msg in enumerate(mock_messages):
            msg.error.return_value = None
            msg.topic.return_value = "test-topic"
            msg.value.return_value = f"message {i}".encode()

        # Mock the underlying consumer's consume method
        consumer._consumer.consume.return_value = mock_messages

        # Test consuming
        result = await consumer.consume(num_messages=2, timeout=1.0)

        # Verify the result
        assert result == mock_messages
        consumer._consumer.consume.assert_called_once_with(num_messages=2, timeout=1.0)

    @pytest.mark.asyncio
    async def test_subscribe_success_with_callbacks(self, mock_consumer, mock_common, basic_config):
        """Test successful subscription with rebalance callback properly wrapped."""
        consumer = AIOConsumer(basic_config, max_workers=2)

        # Create async callbacks
        async def on_assign(consumer, partitions):
            pass

        subscribe_called = asyncio.Event()
        captured_kwargs = {}

        def mock_subscribe(*args, **kwargs):
            nonlocal captured_kwargs
            captured_kwargs = kwargs
            subscribe_called.set()

        consumer._consumer.subscribe = mock_subscribe

        # Test subscribing with callbacks
        await consumer.subscribe(
            ['test-topic'],
            on_assign=on_assign,
        )

        # Wait for subscribe to be called
        await asyncio.wait_for(subscribe_called.wait(), timeout=1.0)

        # Verify callbacks were wrapped (should be different objects)
        assert 'on_assign' in captured_kwargs
        assert captured_kwargs['on_assign'] != on_assign  # Wrapped
        assert callable(captured_kwargs['on_assign'])

    @pytest.mark.asyncio
    async def test_rebalance_callback_args_editing(self, mock_consumer, mock_common, basic_config):
        """Test that rebalance callbacks receive AIOConsumer instead of sync Consumer for async method access."""
        consumer = AIOConsumer(basic_config, max_workers=2)

        # Simulate what librdkafka actually passes: (sync_consumer, partitions)
        sync_consumer_mock = Mock()  # Represents confluent_kafka.Consumer
        partitions = [Mock(), Mock()]  # Represents [TopicPartition(...), ...]

        original_args = (sync_consumer_mock, partitions)
        edited_args = consumer._edit_rebalance_callbacks_args(original_args)

        # Verify the key transformation: sync consumer → aio consumer
        assert edited_args[0] is consumer  # First arg replaced with AIOConsumer
        assert edited_args[1] is partitions  # Partitions unchanged (same object)
        assert len(edited_args) == len(original_args)

    @pytest.mark.asyncio
    async def test_multiple_concurrent_operations(self, mock_consumer, mock_common, basic_config):
        """Test concurrent async operations."""
        consumer = AIOConsumer(basic_config, max_workers=3)

        # Mock different return values for different operations
        consumer._consumer.poll.return_value = Mock()
        consumer._consumer.assignment.return_value = [TopicPartition('test', 0)]
        consumer._consumer.consumer_group_metadata.return_value = Mock()

        # Run multiple operations concurrently (tests ThreadPoolExecutor usage)
        tasks = [
            asyncio.create_task(consumer.poll(timeout=1.0)),
            asyncio.create_task(consumer.assignment()),
            asyncio.create_task(consumer.consumer_group_metadata())
        ]

        # All operations should complete successfully
        results = await asyncio.gather(*tasks)
        assert len(results) == 3
        assert all(result is not None for result in results)

        # Verify all methods were called (proves concurrent execution worked)
        consumer._consumer.poll.assert_called_once()
        consumer._consumer.assignment.assert_called_once()
        consumer._consumer.consumer_group_metadata.assert_called_once()
