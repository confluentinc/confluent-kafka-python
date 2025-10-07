#!/usr/bin/env python3
"""
Unit tests for the KafkaBatchExecutor class (_kafka_batch_executor.py)

This module tests the KafkaBatchExecutor class to ensure proper
Kafka batch execution and partial failure handling.
"""

from confluent_kafka.experimental.aio.producer._kafka_batch_executor import ProducerBatchExecutor as KafkaBatchExecutor
import confluent_kafka
import asyncio
import unittest
from unittest.mock import Mock, patch
import sys
import os
import concurrent.futures

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))


class TestKafkaBatchExecutor(unittest.TestCase):

    def setUp(self):
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)

        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=2)
        self.mock_producer = Mock(spec=confluent_kafka.Producer)
        self.kafka_executor = KafkaBatchExecutor(self.mock_producer, self.executor)

    def tearDown(self):
        self.executor.shutdown(wait=True)
        self.loop.close()

    def test_initialization(self):
        self.assertEqual(self.kafka_executor._producer, self.mock_producer)
        self.assertEqual(self.kafka_executor._executor, self.executor)

    def test_execute_batch_success(self):
        async def async_test():
            batch_messages = [
                {'value': 'test1', 'callback': Mock()},
                {'value': 'test2', 'callback': Mock()},
            ]

            self.mock_producer.produce_batch.return_value = None
            self.mock_producer.poll.return_value = 2

            result = await self.kafka_executor.execute_batch('test-topic', batch_messages)

            self.mock_producer.produce_batch.assert_called_once_with('test-topic', batch_messages, partition=-1)
            self.mock_producer.poll.assert_called_once_with(0)
            self.assertEqual(result, 2)

        self.loop.run_until_complete(async_test())

    def test_partial_failure_handling(self):
        async def async_test():
            callback1 = Mock()
            callback2 = Mock()
            batch_messages = [
                {'value': 'test1', 'callback': callback1},
                {'value': 'test2', 'callback': callback2, '_error': 'MSG_SIZE_TOO_LARGE'},
            ]

            self.mock_producer.produce_batch.return_value = None
            self.mock_producer.poll.return_value = 1

            result = await self.kafka_executor.execute_batch('test-topic', batch_messages)

            self.mock_producer.produce_batch.assert_called_once_with('test-topic', batch_messages, partition=-1)
            self.mock_producer.poll.assert_called_once_with(0)

            callback1.assert_not_called()
            callback2.assert_called_once_with('MSG_SIZE_TOO_LARGE', None)

            self.assertEqual(result, 1)

        self.loop.run_until_complete(async_test())

    def test_batch_execution_exception(self):
        async def async_test():
            batch_messages = [{'value': 'test1', 'callback': Mock()}]

            self.mock_producer.produce_batch.side_effect = Exception("Kafka error")

            with self.assertRaises(Exception) as context:
                await self.kafka_executor.execute_batch('test-topic', batch_messages)

            self.assertEqual(str(context.exception), "Kafka error")
            self.mock_producer.produce_batch.assert_called_once_with('test-topic', batch_messages, partition=-1)

        self.loop.run_until_complete(async_test())

    def test_callback_exception_handling(self):
        async def async_test():
            failing_callback = Mock(side_effect=Exception("Callback error"))

            batch_messages = [
                {'value': 'test1', 'callback': failing_callback, '_error': 'TEST_ERROR'},
            ]

            self.mock_producer.produce_batch.return_value = None
            self.mock_producer.poll.return_value = 0

            # Expect the callback exception to be raised
            with self.assertRaises(Exception) as context:
                await self.kafka_executor.execute_batch('test-topic', batch_messages)

            # Verify the callback was called before the exception
            failing_callback.assert_called_once_with('TEST_ERROR', None)
            self.assertEqual(str(context.exception), "Callback error")

        self.loop.run_until_complete(async_test())

    def test_thread_pool_execution(self):
        async def async_test():
            batch_messages = [{'value': 'test1', 'callback': Mock()}]

            with patch.object(self.loop, 'run_in_executor') as mock_run_in_executor:
                future_result = self.loop.create_future()
                future_result.set_result(1)
                mock_run_in_executor.return_value = future_result

                result = await self.kafka_executor.execute_batch('test-topic', batch_messages)

                mock_run_in_executor.assert_called_once()
                args = mock_run_in_executor.call_args
                self.assertEqual(args[0][0], self.executor)
                self.assertTrue(callable(args[0][1]))

                self.assertEqual(result, 1)

        self.loop.run_until_complete(async_test())


if __name__ == '__main__':
    unittest.main()
