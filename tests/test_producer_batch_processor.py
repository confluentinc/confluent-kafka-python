#!/usr/bin/env python3
"""
Unit tests for the BatchProcessor class (_batch_processor.py)

This module tests the BatchProcessor class to ensure proper
message batching, topic grouping, and future management.
"""

import asyncio
import concurrent.futures
import os
import sys
import unittest
from unittest.mock import Mock, patch

import confluent_kafka
from confluent_kafka.experimental.aio.producer._AIOProducer import AIOProducer
from confluent_kafka.experimental.aio.producer._kafka_batch_executor import ProducerBatchExecutor as KafkaBatchExecutor
from confluent_kafka.experimental.aio.producer._producer_batch_processor import (
    ProducerBatchManager as ProducerBatchProcessor,
)

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))


class TestProducerBatchProcessor(unittest.TestCase):
    """Test cases for ProducerBatchProcessor class"""

    def setUp(self):
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)

        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=2)

        self.producer_config = {
            'bootstrap.servers': 'localhost:9092',
            'client.id': 'test-producer',
            'message.timeout.ms': 100,
            'queue.buffering.max.messages': 1000,
            'api.version.request': False,
        }

        self.confluent_kafka_producer = confluent_kafka.Producer(self.producer_config)

        self.kafka_executor = KafkaBatchExecutor(self.confluent_kafka_producer, self.executor)
        self.batch_processor = ProducerBatchProcessor(self.kafka_executor)

        async def create_aio_producer():
            return AIOProducer(self.producer_config, executor=self.executor)

        self.aio_producer = self.loop.run_until_complete(create_aio_producer())

    def tearDown(self):
        try:
            self.loop.run_until_complete(self.aio_producer.close())
        except Exception:
            pass

        try:
            self.confluent_kafka_producer.flush(timeout=1)
        except Exception:
            pass

        try:
            self.executor.shutdown(wait=True, timeout=1)
        except Exception:
            pass

        self.loop.close()

    def test_basic_functionality(self):
        self.assertEqual(self.batch_processor.get_buffer_size(), 0)
        self.assertTrue(self.batch_processor.is_buffer_empty())

        future1 = Mock()
        future2 = Mock()
        msg_data1 = {'topic': 'topic1', 'value': 'test1', 'key': 'key1'}
        msg_data2 = {'topic': 'topic2', 'value': 'test2', 'key': 'key2'}

        self.batch_processor.add_message(msg_data1, future1)
        self.batch_processor.add_message(msg_data2, future2)

        self.assertEqual(self.batch_processor.get_buffer_size(), 2)
        self.assertFalse(self.batch_processor.is_buffer_empty())

        self.batch_processor.clear_buffer()

        self.assertEqual(self.batch_processor.get_buffer_size(), 0)
        self.assertTrue(self.batch_processor.is_buffer_empty())

    def test_group_messages_by_topic(self):
        future1 = Mock()
        future2 = Mock()
        future3 = Mock()

        msg1 = {'topic': 'topic1', 'value': 'test1', 'user_callback': Mock()}
        msg2 = {'topic': 'topic2', 'value': 'test2'}
        msg3 = {'topic': 'topic1', 'value': 'test3', 'user_callback': Mock()}

        self.batch_processor.add_message(msg1, future1)
        self.batch_processor.add_message(msg2, future2)
        self.batch_processor.add_message(msg3, future3)

        topic_groups = self.batch_processor._group_messages_by_topic_and_partition()

        self.assertEqual(len(topic_groups), 2)

        self.assertIn(('topic1', -1), topic_groups)
        topic1_group = topic_groups[('topic1', -1)]
        self.assertEqual(len(topic1_group['messages']), 2)
        self.assertEqual(len(topic1_group['futures']), 2)
        self.assertEqual(topic1_group['futures'][0], future1)
        self.assertEqual(topic1_group['futures'][1], future3)

        self.assertIn(('topic2', -1), topic_groups)
        topic2_group = topic_groups[('topic2', -1)]
        self.assertEqual(len(topic2_group['messages']), 1)
        self.assertEqual(len(topic2_group['futures']), 1)
        self.assertEqual(topic2_group['futures'][0], future2)

    def test_prepare_batch_messages(self):
        messages = [
            {'topic': 'test', 'value': 'test1', 'user_callback': Mock(), 'key': 'key1'},
            {'topic': 'test', 'value': 'test2', 'partition': 1},
        ]

        batch_messages = self.batch_processor._prepare_batch_messages(messages)

        self.assertEqual(len(batch_messages), 2)

        self.assertNotIn('topic', batch_messages[0])
        self.assertIn('value', batch_messages[0])
        self.assertIn('key', batch_messages[0])

        self.assertNotIn('topic', batch_messages[1])
        self.assertIn('value', batch_messages[1])
        self.assertIn('partition', batch_messages[1])

    def test_assign_future_callbacks(self):
        batch_messages = [
            {'value': 'test1'},
            {'value': 'test2'},
        ]
        futures = [Mock(), Mock()]

        self.batch_processor._assign_future_callbacks(batch_messages, futures)

        self.assertIn('callback', batch_messages[0])
        self.assertIn('callback', batch_messages[1])

    def test_handle_batch_failure(self):
        """Test handling batch failures"""
        futures = [Mock(), Mock()]
        futures[0].done.return_value = False
        futures[1].done.return_value = True  # Already done

        exception = RuntimeError("Batch failed")

        # Handle batch failure
        self.batch_processor._handle_batch_failure(exception, futures)

        # Verify first future got exception (not already done)
        futures[0].set_exception.assert_called_once_with(exception)

        # Verify second future was not modified (already done)
        futures[1].set_exception.assert_not_called()

        # Note: For real AIOProducer, the user callback is invoked directly by _handle_user_callback

    def test_flush_empty_buffer(self):
        """Test flushing empty buffer is no-op"""

        async def async_test():
            await self.batch_processor.flush_buffer()
            self.assertTrue(self.batch_processor.is_buffer_empty())

        self.loop.run_until_complete(async_test())

    def test_flush_buffer_with_messages(self):
        """Test successful buffer flush with messages"""

        async def async_test():
            future1 = self.loop.create_future()
            future2 = self.loop.create_future()
            msg1 = {'topic': 'topic1', 'value': 'test1'}
            msg2 = {'topic': 'topic1', 'value': 'test2'}

            self.batch_processor.add_message(msg1, future1)
            self.batch_processor.add_message(msg2, future2)

            success_future = self.loop.create_future()
            success_future.set_result("success")

            with patch.object(self.loop, 'run_in_executor', return_value=success_future):
                await self.batch_processor.flush_buffer()

            self.assertTrue(self.batch_processor.is_buffer_empty())

        self.loop.run_until_complete(async_test())

    def test_flush_buffer_selective_topic(self):
        """Test selective topic flushing"""

        async def async_test():
            future3 = self.loop.create_future()
            future4 = self.loop.create_future()
            msg3 = {'topic': 'topic1', 'value': 'test3'}
            msg4 = {'topic': 'topic2', 'value': 'test4'}

            self.batch_processor.add_message(msg3, future3)
            self.batch_processor.add_message(msg4, future4)

            success_future = self.loop.create_future()
            success_future.set_result("success")

            with patch.object(self.loop, 'run_in_executor', return_value=success_future):
                await self.batch_processor.flush_buffer(target_topic='topic1')

            self.assertEqual(self.batch_processor.get_buffer_size(), 1)

        self.loop.run_until_complete(async_test())

    def test_flush_buffer_exception_handling(self):
        """Test exception handling during buffer flush"""

        async def async_test():
            future = self.loop.create_future()
            msg = {'topic': 'topic1', 'value': 'test'}
            self.batch_processor.add_message(msg, future)

            exception = RuntimeError("Batch execution failed")

            with patch.object(self.loop, 'run_in_executor', side_effect=exception):
                with self.assertRaises(RuntimeError):
                    await self.batch_processor.flush_buffer()

            # Buffer should NOT be empty on exception - we want to retry
            self.assertFalse(self.batch_processor.is_buffer_empty())
            self.assertEqual(self.batch_processor.get_buffer_size(), 1)

        self.loop.run_until_complete(async_test())

    def test_kafka_executor_integration(self):
        """Test executing a batch operation via KafkaBatchExecutor"""

        async def async_test():
            batch_messages = [
                {'value': 'test1', 'callback': Mock()},
                {'value': 'test2', 'callback': Mock()},
            ]

            # Mock the executor to return a completed future
            future_result = self.loop.create_future()
            future_result.set_result("poll_result")

            with patch.object(self.loop, 'run_in_executor', return_value=future_result) as mock_run_in_executor:
                result = await self.kafka_executor.execute_batch('test-topic', batch_messages)

                # Verify run_in_executor was called
                mock_run_in_executor.assert_called_once()
                self.assertEqual(result, "poll_result")

        self.loop.run_until_complete(async_test())

    def _create_mixed_topic_messages(self):
        """Helper to create messages across multiple topics"""
        messages_data = []
        futures = []
        user_callbacks = []

        for i in range(4):
            future = Mock()
            future.done.return_value = False
            user_callback = Mock()
            msg_data = {
                'topic': f'topic{i % 2}',
                'value': f'unique_value_{i}',
                'key': f'unique_key_{i}',
                'user_callback': user_callback,
            }

            self.batch_processor.add_message(msg_data, future)
            messages_data.append(msg_data)
            futures.append(future)
            user_callbacks.append(user_callback)

        return messages_data, futures, user_callbacks

    def _add_alternating_topic_messages(self):
        """Helper to add messages alternating between two topics"""
        futures = []
        for i in range(5):
            future = self.loop.create_future()
            msg_data = {'topic': f'topic{i % 2}', 'value': f'test{i}', 'key': f'key{i}'}
            self.batch_processor.add_message(msg_data, future)
            futures.append(future)
        return futures

    def test_batch_cycle_buffer_state(self):
        """Test buffer state during batch cycle"""
        self._add_alternating_topic_messages()
        self.assertEqual(self.batch_processor.get_buffer_size(), 5)
        self.assertFalse(self.batch_processor.is_buffer_empty())

    def test_batch_cycle_topic_grouping(self):
        """Test topic grouping in batch cycle"""
        self._add_alternating_topic_messages()
        topic_groups = self.batch_processor._group_messages_by_topic_and_partition()

        self.assertEqual(len(topic_groups), 2)
        self.assertIn(('topic0', -1), topic_groups)
        self.assertIn(('topic1', -1), topic_groups)
        self.assertEqual(len(topic_groups[('topic0', -1)]['messages']), 3)
        self.assertEqual(len(topic_groups[('topic1', -1)]['messages']), 2)

    def test_batch_cycle_message_preparation(self):
        """Test message preparation in batch cycle"""
        self._add_alternating_topic_messages()
        topic_groups = self.batch_processor._group_messages_by_topic_and_partition()

        batch_messages = self.batch_processor._prepare_batch_messages(topic_groups[('topic0', -1)]['messages'])

        self.assertEqual(len(batch_messages), 3)
        for batch_msg in batch_messages:
            self.assertNotIn('topic', batch_msg)
            self.assertIn('value', batch_msg)
            self.assertIn('key', batch_msg)

    def test_batch_message_preparation_with_mixed_sizes(self):
        """Test batch message preparation with mixed message sizes"""
        # Create test messages with different sizes
        messages = [
            {'topic': 'test-topic', 'value': 'small message'},
            {'topic': 'test-topic', 'value': 'x' * (5 * 1024 * 1024)},  # Large message
            {'topic': 'test-topic', 'value': 'another small'},
        ]
        futures = [asyncio.Future(), asyncio.Future(), asyncio.Future()]

        for msg, future in zip(messages, futures):
            self.batch_processor.add_message(msg, future)

        topic_groups = self.batch_processor._group_messages_by_topic_and_partition()
        topic_data = topic_groups[('test-topic', -1)]
        batch_messages = self.batch_processor._prepare_batch_messages(topic_data['messages'])

        self.assertEqual(len(batch_messages), 3)
        large_msg = next((msg for msg in batch_messages if len(str(msg.get('value', ''))) > 1000), None)
        self.assertIsNotNone(large_msg)

    def test_future_based_usage_pattern(self):
        """Test the recommended Future-based usage pattern instead of callbacks."""
        # Create test messages without user callbacks
        messages = [
            {'topic': 'test-topic', 'value': 'test1', 'key': 'key1'},
            {'topic': 'test-topic', 'value': 'test2', 'key': 'key2'},
        ]
        futures = [asyncio.Future(), asyncio.Future()]

        # Add messages to batch processor
        for msg, future in zip(messages, futures):
            self.batch_processor.add_message(msg, future)

        # Verify messages are in buffer
        self.assertEqual(self.batch_processor.get_buffer_size(), 2)

        # Simulate successful delivery by resolving futures
        mock_msg1 = Mock()
        mock_msg1.topic.return_value = 'test-topic'
        mock_msg1.value.return_value = b'test1'

        mock_msg2 = Mock()
        mock_msg2.topic.return_value = 'test-topic'
        mock_msg2.value.return_value = b'test2'

        # Applications should await these futures to get delivery results
        futures[0].set_result(mock_msg1)
        futures[1].set_result(mock_msg2)

        # Verify futures are resolved
        self.assertTrue(futures[0].done())
        self.assertTrue(futures[1].done())
        self.assertEqual(futures[0].result(), mock_msg1)
        self.assertEqual(futures[1].result(), mock_msg2)

    def test_future_based_error_handling(self):
        """Test Future-based error handling pattern."""
        # Create test message
        message = {'topic': 'test-topic', 'value': 'test', 'key': 'key'}
        future = asyncio.Future()

        # Add message to batch processor
        self.batch_processor.add_message(message, future)

        # Simulate delivery error by setting exception on future
        mock_error = RuntimeError("Delivery failed")
        future.set_exception(mock_error)

        # Verify future is resolved with exception
        self.assertTrue(future.done())
        with self.assertRaises(RuntimeError):
            future.result()

    def test_add_batches_back_to_buffer_basic(self):
        """Test adding batches back to buffer with basic message data"""
        from confluent_kafka.experimental.aio.producer._message_batch import create_message_batch

        # Create test futures
        future1 = asyncio.Future()
        future2 = asyncio.Future()

        # Create test batch with basic message data
        batch = create_message_batch(
            topic='test-topic',
            messages=[{'value': 'test1', 'key': 'key1'}, {'value': 'test2', 'key': 'key2'}],
            futures=[future1, future2],
            partition=0,
        )

        # Ensure buffer is initially empty
        self.assertTrue(self.batch_processor.is_buffer_empty())

        # Add batch back to buffer
        self.batch_processor._add_batches_back_to_buffer([batch])

        # Verify buffer state
        self.assertEqual(self.batch_processor.get_buffer_size(), 2)
        self.assertFalse(self.batch_processor.is_buffer_empty())

        # Verify message data was reconstructed correctly
        self.assertEqual(self.batch_processor._message_buffer[0]['topic'], 'test-topic')
        self.assertEqual(self.batch_processor._message_buffer[0]['value'], 'test1')
        self.assertEqual(self.batch_processor._message_buffer[0]['key'], 'key1')

        self.assertEqual(self.batch_processor._message_buffer[1]['topic'], 'test-topic')
        self.assertEqual(self.batch_processor._message_buffer[1]['value'], 'test2')
        self.assertEqual(self.batch_processor._message_buffer[1]['key'], 'key2')

        # Verify futures are preserved
        self.assertEqual(self.batch_processor._buffer_futures[0], future1)
        self.assertEqual(self.batch_processor._buffer_futures[1], future2)

    def test_add_batches_back_to_buffer_empty_batch(self):
        """Test adding empty batch back to buffer"""
        from confluent_kafka.experimental.aio.producer._message_batch import create_message_batch

        # Create empty batch
        batch = create_message_batch(topic='test-topic', messages=[], futures=[], partition=0)

        initial_size = self.batch_processor.get_buffer_size()

        # Add empty batch back
        self.batch_processor._add_batches_back_to_buffer([batch])

        # Buffer size should remain unchanged
        self.assertEqual(self.batch_processor.get_buffer_size(), initial_size)


if __name__ == '__main__':
    # Run all tests
    unittest.main(verbosity=2)
