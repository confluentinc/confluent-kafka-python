#!/usr/bin/env python
# -*- coding: utf-8 -*-
import pytest
import asyncio
import concurrent.futures
from unittest.mock import Mock, patch

from confluent_kafka import TopicPartition, KafkaError, KafkaException
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

    @pytest.mark.asyncio
    async def test_concurrent_operations_error_handling(self, mock_consumer, mock_common, basic_config):
        """Test concurrent async operations handle errors gracefully."""
        # Mock: 2 poll calls fail, assignment succeeds
        mock_consumer.return_value.poll.side_effect = [
            KafkaException(KafkaError(KafkaError._TRANSPORT)),
            KafkaException(KafkaError(KafkaError._TRANSPORT))
        ]
        mock_consumer.return_value.assignment.return_value = []

        consumer = AIOConsumer(basic_config)

        # Run concurrent operations
        tasks = [
            consumer.poll(timeout=0.1),
            consumer.poll(timeout=0.1),
            consumer.assignment()
        ]

        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Verify results
        assert len(results) == 3
        assert isinstance(results[0], KafkaException)
        assert isinstance(results[1], KafkaException)
        assert results[2] == []

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
