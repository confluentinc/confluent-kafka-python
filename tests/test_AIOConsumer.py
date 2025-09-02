#!/usr/bin/env python
# -*- coding: utf-8 -*-
import pytest
import asyncio
import concurrent.futures
from unittest.mock import Mock, patch, AsyncMock

from confluent_kafka import TopicPartition
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
    async def test_constructor_behavior(self, mock_consumer, mock_common, basic_config):
        """Test constructor creates consumer with correct configuration."""
        custom_executor = concurrent.futures.ThreadPoolExecutor(max_workers=4)
        try:
            # Test with custom executor
            consumer = AIOConsumer(basic_config, max_workers=2, executor=custom_executor)

            assert consumer.executor is custom_executor
            assert consumer.executor._max_workers == 4
            assert hasattr(consumer, '_consumer')

            # Test with default executor
            consumer2 = AIOConsumer(basic_config, max_workers=3)
            assert consumer2.executor is not custom_executor
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

        # Mock the sync poll() method
        mock_message = Mock()
        mock_consumer.return_value.poll.return_value = mock_message

        result = await consumer.poll(timeout=1.0)

        assert result is mock_message

    @pytest.mark.asyncio
    async def test_consume_success(self, mock_consumer, mock_common, basic_config):
        """Test successful message consumption."""
        consumer = AIOConsumer(basic_config, max_workers=2)

        # Mock the sync consume() method
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

        # Verify subscribe was called (callback wrapping is implementation detail)
        mock_consumer.return_value.subscribe.assert_called_once()

    @pytest.mark.asyncio
    async def test_multiple_concurrent_operations(self, mock_consumer, mock_common, basic_config):
        """Test concurrent async operations."""
        consumer = AIOConsumer(basic_config, max_workers=3)

        mock_consumer.return_value.poll.return_value = Mock()
        mock_consumer.return_value.assignment.return_value = [TopicPartition('test', 0)]
        mock_consumer.return_value.consumer_group_metadata.return_value = Mock()

        tasks = [
            asyncio.create_task(consumer.poll(timeout=1.0)),
            asyncio.create_task(consumer.assignment()),
            asyncio.create_task(consumer.consumer_group_metadata())
        ]

        results = await asyncio.gather(*tasks)
        assert len(results) == 3
        assert all(result is not None for result in results)
