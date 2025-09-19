#!/usr/bin/env python3
"""
Unit tests for the ProducerBatchExecutor class (_kafka_batch_executor.py)

This module tests the ProducerBatchExecutor class to ensure proper
Kafka batch execution and partial failure handling.
"""

import asyncio
import unittest
from unittest.mock import Mock, patch
import sys
import os
import concurrent.futures
import confluent_kafka

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from confluent_kafka.aio.producer._kafka_batch_executor import ProducerBatchExecutor


class TestProducerBatchExecutor(unittest.TestCase):
    """Test cases for ProducerBatchExecutor class"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        
        # Create a real ThreadPoolExecutor
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=2)
        
        # Create a mock producer
        self.mock_producer = Mock(spec=confluent_kafka.Producer)
        
        # Create the executor
        self.kafka_executor = ProducerBatchExecutor(self.mock_producer, self.executor)
    
    def tearDown(self):
        """Clean up test fixtures"""
        self.executor.shutdown(wait=True)
        self.loop.close()
    
    def test_initialization(self):
        """Test ProducerBatchExecutor initialization"""
        self.assertEqual(self.kafka_executor._producer, self.mock_producer)
        self.assertEqual(self.kafka_executor._executor, self.executor)
    
    def test_execute_batch_success(self):
        """Test successful batch execution"""
        async def async_test():
            # Create test batch messages
            batch_messages = [
                {'value': 'test1', 'callback': Mock()},
                {'value': 'test2', 'callback': Mock()},
            ]
            
            # Mock producer methods
            self.mock_producer.produce_batch.return_value = None
            self.mock_producer.poll.return_value = 2  # 2 delivery reports processed
            
            # Execute batch
            result = await self.kafka_executor.execute_batch('test-topic', batch_messages)
            
            # Verify producer methods were called
            self.mock_producer.produce_batch.assert_called_once_with('test-topic', batch_messages)
            self.mock_producer.poll.assert_called_once_with(0)
            self.assertEqual(result, 2)
        
        self.loop.run_until_complete(async_test())
    
    def test_partial_failure_handling(self):
        """Test handling of partial batch failures"""
        async def async_test():
            # Create test batch messages with one that will fail
            callback1 = Mock()
            callback2 = Mock()
            batch_messages = [
                {'value': 'test1', 'callback': callback1},
                {'value': 'test2', 'callback': callback2, '_error': 'MSG_SIZE_TOO_LARGE'},
            ]
            
            # Mock producer methods
            self.mock_producer.produce_batch.return_value = None
            self.mock_producer.poll.return_value = 1  # Only 1 successful message
            
            # Execute batch
            result = await self.kafka_executor.execute_batch('test-topic', batch_messages)
            
            # Verify producer methods were called
            self.mock_producer.produce_batch.assert_called_once_with('test-topic', batch_messages)
            self.mock_producer.poll.assert_called_once_with(0)
            
            # Verify the failed callback was invoked
            callback1.assert_not_called()  # Successful message callback not called here (called by librdkafka)
            callback2.assert_called_once_with('MSG_SIZE_TOO_LARGE', None)  # Failed message callback called
            
            self.assertEqual(result, 1)
        
        self.loop.run_until_complete(async_test())
    
    def test_batch_execution_exception(self):
        """Test handling of exceptions during batch execution"""
        async def async_test():
            batch_messages = [{'value': 'test1', 'callback': Mock()}]
            
            # Mock producer to raise exception
            self.mock_producer.produce_batch.side_effect = Exception("Kafka error")
            
            # Execute batch and expect exception to be propagated
            with self.assertRaises(Exception) as context:
                await self.kafka_executor.execute_batch('test-topic', batch_messages)
            
            self.assertEqual(str(context.exception), "Kafka error")
            self.mock_producer.produce_batch.assert_called_once_with('test-topic', batch_messages)
        
        self.loop.run_until_complete(async_test())
    
    def test_callback_exception_handling(self):
        """Test that exceptions in callbacks during partial failure handling are caught"""
        async def async_test():
            # Create callback that raises exception
            failing_callback = Mock(side_effect=Exception("Callback error"))
            
            batch_messages = [
                {'value': 'test1', 'callback': failing_callback, '_error': 'TEST_ERROR'},
            ]
            
            # Mock producer methods
            self.mock_producer.produce_batch.return_value = None
            self.mock_producer.poll.return_value = 0
            
            # Execute batch - should not raise exception despite callback failure
            result = await self.kafka_executor.execute_batch('test-topic', batch_messages)
            
            # Verify callback was called and exception was caught
            failing_callback.assert_called_once_with('TEST_ERROR', None)
            self.assertEqual(result, 0)
        
        self.loop.run_until_complete(async_test())
    
    def test_thread_pool_execution(self):
        """Test that batch execution uses thread pool"""
        async def async_test():
            batch_messages = [{'value': 'test1', 'callback': Mock()}]
            
            # Mock the loop's run_in_executor method
            with patch.object(self.loop, 'run_in_executor') as mock_run_in_executor:
                future_result = self.loop.create_future()
                future_result.set_result(1)
                mock_run_in_executor.return_value = future_result
                
                result = await self.kafka_executor.execute_batch('test-topic', batch_messages)
                
                # Verify run_in_executor was called with correct arguments
                mock_run_in_executor.assert_called_once()
                args = mock_run_in_executor.call_args
                self.assertEqual(args[0][0], self.executor)  # First argument should be the executor
                self.assertTrue(callable(args[0][1]))  # Second argument should be the function
                
                self.assertEqual(result, 1)
        
        self.loop.run_until_complete(async_test())


if __name__ == '__main__':
    unittest.main()
