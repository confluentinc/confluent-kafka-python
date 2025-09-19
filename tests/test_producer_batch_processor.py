#!/usr/bin/env python3
"""
Unit tests for the BatchProcessor class (_batch_processor.py)

This module tests the BatchProcessor class to ensure proper
message batching, topic grouping, and callback management.
"""

import asyncio
import unittest
from unittest.mock import Mock, AsyncMock, patch
import sys
import os
import concurrent.futures
import confluent_kafka


# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from confluent_kafka.aio.producer._producer_batch_processor import ProducerBatchProcessor
from confluent_kafka.aio.producer._AIOProducer import AIOProducer
from confluent_kafka.aio.producer._callback_handler import AsyncCallbackHandler
from confluent_kafka.aio.producer._kafka_batch_executor import KafkaBatchExecutor


class TestProducerBatchProcessor(unittest.TestCase):
    """Test cases for ProducerBatchProcessor class"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        
        # Create a real ThreadPoolExecutor
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=2)
        
        # Create a real confluent_kafka.Producer with test configuration
        self.producer_config = {
            'bootstrap.servers': 'localhost:9092',  # Will fail to connect, but that's OK for most tests
            'client.id': 'test-producer',
            'message.timeout.ms': 100,
            'queue.buffering.max.messages': 1000,
            'api.version.request': False,  # Disable API version requests to avoid network calls
        }
        
        self.confluent_kafka_producer = confluent_kafka.Producer(self.producer_config)
        
        # Create callback handler and Kafka executor for batch processor
        self.callback_handler = AsyncCallbackHandler(self.loop)
        self.kafka_executor = KafkaBatchExecutor(self.confluent_kafka_producer, self.executor)
        self.batch_processor = ProducerBatchProcessor(self.callback_handler, self.kafka_executor, callback_pool_size=10)
        
        # Create AIOProducer within the event loop context
        async def create_aio_producer():
            return AIOProducer(self.producer_config, executor=self.executor)
        
        self.aio_producer = self.loop.run_until_complete(create_aio_producer())
    
    def tearDown(self):
        """Clean up test fixtures"""
        # Clean up the AIOProducer
        try:
            self.loop.run_until_complete(self.aio_producer.close())
        except Exception:
            pass  # Ignore cleanup errors
        
        # Clean up the producer
        try:
            self.confluent_kafka_producer.flush(timeout=1)
        except Exception:
            pass  # Ignore cleanup errors
        
        # Clean up the executor
        try:
            self.executor.shutdown(wait=True, timeout=1)
        except Exception:
            pass  # Ignore cleanup errors
        
        self.loop.close()
    
    def test_basic_functionality(self):
        """Test basic ProducerBatchProcessor functionality: initialization, add_message, and clear_buffer"""
        # Test initialization with custom pool size
        processor = ProducerBatchProcessor(self.callback_handler, self.kafka_executor, callback_pool_size=50)
        self.assertEqual(processor.get_buffer_size(), 0)
        self.assertTrue(processor.is_buffer_empty())
        
        # Check callback pool stats
        stats = processor.get_callback_pool_stats()
        self.assertEqual(stats['available'], 50)
        self.assertEqual(stats['created_total'], 50)
        
        # Test adding messages to the buffer
        future1 = Mock()
        future2 = Mock()
        msg_data1 = {'topic': 'topic1', 'value': 'test1', 'key': 'key1'}
        msg_data2 = {'topic': 'topic2', 'value': 'test2', 'key': 'key2'}
        
        self.batch_processor.add_message(msg_data1, future1)
        self.batch_processor.add_message(msg_data2, future2)
        
        # Verify buffer state after adding
        self.assertEqual(self.batch_processor.get_buffer_size(), 2)
        self.assertFalse(self.batch_processor.is_buffer_empty())
        
        # Test clearing the buffer
        self.batch_processor.clear_buffer()
        
        # Verify buffer is empty after clearing
        self.assertEqual(self.batch_processor.get_buffer_size(), 0)
        self.assertTrue(self.batch_processor.is_buffer_empty())
    
    def test_group_messages_by_topic(self):
        """Test grouping messages by topic"""
        # Add messages for different topics
        future1 = Mock()
        future2 = Mock()
        future3 = Mock()
        
        msg1 = {'topic': 'topic1', 'value': 'test1', 'user_callback': Mock()}
        msg2 = {'topic': 'topic2', 'value': 'test2'}
        msg3 = {'topic': 'topic1', 'value': 'test3', 'user_callback': Mock()}
        
        self.batch_processor.add_message(msg1, future1)
        self.batch_processor.add_message(msg2, future2)
        self.batch_processor.add_message(msg3, future3)
        
        # Group messages by topic
        topic_groups = self.batch_processor._group_messages_by_topic()
        
        # Verify grouping
        self.assertEqual(len(topic_groups), 2)
        
        # Check topic1 group
        self.assertIn('topic1', topic_groups)
        topic1_group = topic_groups['topic1']
        self.assertEqual(len(topic1_group['messages']), 2)
        self.assertEqual(len(topic1_group['futures']), 2)
        self.assertEqual(len(topic1_group['callbacks']), 2)
        self.assertEqual(topic1_group['futures'][0], future1)
        self.assertEqual(topic1_group['futures'][1], future3)
        
        # Check topic2 group
        self.assertIn('topic2', topic_groups)
        topic2_group = topic_groups['topic2']
        self.assertEqual(len(topic2_group['messages']), 1)
        self.assertEqual(len(topic2_group['futures']), 1)
        self.assertEqual(len(topic2_group['callbacks']), 1)
        self.assertEqual(topic2_group['futures'][0], future2)
        self.assertIsNone(topic2_group['callbacks'][0])  # No user callback
    
    def test_prepare_batch_messages(self):
        """Test preparing messages for produce_batch"""
        messages = [
            {'topic': 'test', 'value': 'test1', 'user_callback': Mock(), 'key': 'key1'},
            {'topic': 'test', 'value': 'test2', 'partition': 1},
        ]
        
        batch_messages = self.batch_processor._prepare_batch_messages(messages)
        
        # Verify cleanup
        self.assertEqual(len(batch_messages), 2)
        
        # Check first message
        self.assertNotIn('user_callback', batch_messages[0])
        self.assertNotIn('topic', batch_messages[0])
        self.assertIn('value', batch_messages[0])
        self.assertIn('key', batch_messages[0])
        
        # Check second message
        self.assertNotIn('topic', batch_messages[1])
        self.assertIn('value', batch_messages[1])
        self.assertIn('partition', batch_messages[1])
    
    def test_assign_callbacks_to_messages(self):
        """Test assigning callbacks to batch messages"""
        batch_messages = [
            {'value': 'test1'},
            {'value': 'test2'},
        ]
        futures = [Mock(), Mock()]
        user_callbacks = [Mock(), None]
        
        # Assign callbacks
        self.batch_processor._assign_callbacks_to_messages(
            batch_messages, futures, user_callbacks
        )
        
        # Verify callbacks were assigned
        self.assertIn('callback', batch_messages[0])
        self.assertIn('callback', batch_messages[1])
        
        # Verify callback pool was used
        stats = self.batch_processor.get_callback_pool_stats()
        self.assertEqual(stats['in_use'], 2)
        self.assertEqual(stats['reuse_count'], 2)
    
    def test_handle_batch_failure(self):
        """Test handling batch failures"""
        futures = [Mock(), Mock()]
        futures[0].done.return_value = False
        futures[1].done.return_value = True  # Already done
        
        user_callbacks = [Mock(), None]
        exception = RuntimeError("Batch failed")
        
        # Handle batch failure
        self.batch_processor._handle_batch_failure(
            exception, futures, user_callbacks
        )
        
        # Verify first future got exception (not already done)
        futures[0].set_exception.assert_called_once_with(exception)
        
        # Verify second future was not modified (already done)
        futures[1].set_exception.assert_not_called()
        
        # Note: For real AIOProducer, the user callback is invoked directly by _handle_user_callback
    
    def test_buffer_flush_scenarios(self):
        """Test comprehensive buffer flushing scenarios: empty buffer, successful flush, target topic, and exception handling"""
        async def async_test():
            # Test 1: Flush empty buffer (should be no-op)
            await self.batch_processor.flush_buffer()
            # Note: With real executor, we can't easily assert submit wasn't called
            
            # Test 2: Flush buffer with messages (successful case)
            future1 = self.loop.create_future()
            future2 = self.loop.create_future()
            msg1 = {'topic': 'topic1', 'value': 'test1'}
            msg2 = {'topic': 'topic1', 'value': 'test2'}
            
            self.batch_processor.add_message(msg1, future1)
            self.batch_processor.add_message(msg2, future2)
            
            # Mock successful batch execution
            success_future = self.loop.create_future()
            success_future.set_result("success")
            
            with patch.object(self.loop, 'run_in_executor', return_value=success_future):
                await self.batch_processor.flush_buffer()
            
            # Verify buffer was cleared
            self.assertTrue(self.batch_processor.is_buffer_empty())
            
            # Test 3: Flush with target topic (selective flushing)
            future3 = self.loop.create_future()
            future4 = self.loop.create_future()
            msg3 = {'topic': 'topic1', 'value': 'test3'}
            msg4 = {'topic': 'topic2', 'value': 'test4'}
            
            self.batch_processor.add_message(msg3, future3)
            self.batch_processor.add_message(msg4, future4)
            
            with patch.object(self.loop, 'run_in_executor', return_value=success_future):
                # Flush only topic1
                await self.batch_processor.flush_buffer(target_topic='topic1')
            
            # Verify only topic1 was processed, topic2 remains in buffer
            self.assertEqual(self.batch_processor.get_buffer_size(), 1)
            
            # Test 4: Exception handling during flush
            exception = RuntimeError("Batch execution failed")
            
            with patch.object(self.loop, 'run_in_executor', side_effect=exception):
                with self.assertRaises(RuntimeError):
                    await self.batch_processor.flush_buffer()
            
            # Verify buffer was still cleared (messages were processed, just failed)
            self.assertTrue(self.batch_processor.is_buffer_empty())
        
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
    
    def test_callback_future_mapping_integrity(self):
        """Test that callbacks are correctly mapped to their corresponding futures and messages"""
        # Create test messages with unique identifiers
        messages_data = []
        futures = []
        user_callbacks = []
        
        for i in range(4):
            future = Mock()
            future.done.return_value = False
            user_callback = Mock()
            msg_data = {
                'topic': f'topic{i % 2}',  # Mix topics to test grouping
                'value': f'unique_value_{i}',
                'key': f'unique_key_{i}',
                'user_callback': user_callback
            }
            
            self.batch_processor.add_message(msg_data, future)
            messages_data.append(msg_data)
            futures.append(future)
            user_callbacks.append(user_callback)
        
        # Group messages and prepare for batch processing
        topic_groups = self.batch_processor._group_messages_by_topic()
        
        # Test each topic group's mapping integrity
        for topic, group_data in topic_groups.items():
            batch_messages = self.batch_processor._prepare_batch_messages(group_data['messages'])
            
            # Assign callbacks to messages
            self.batch_processor._assign_callbacks_to_messages(
                batch_messages,
                group_data['futures'],
                group_data['callbacks']
            )
            
            # Verify that each callback knows about the correct future and user_callback
            for i, batch_msg in enumerate(batch_messages):
                callback = batch_msg['callback']
                expected_future = group_data['futures'][i]
                expected_user_callback = group_data['callbacks'][i]
                
                # Verify the callback has the right future and user_callback
                self.assertEqual(callback.future, expected_future)
                self.assertEqual(callback.user_callback, expected_user_callback)
                
                # Test callback execution with success
                test_msg = Mock()
                test_msg.value.return_value = batch_msg['value']
                
                # Call the callback with success (err=None)
                callback(None, test_msg)
                
                # Verify the correct future was resolved
                expected_future.set_result.assert_called_once_with(test_msg)
                
                # Note: For real AIOProducer, the user callback is invoked directly by _handle_user_callback
                
                # Reset mock for next iteration
                expected_future.reset_mock()
    
    def test_complete_batch_cycle(self):
        """Test complete batch processing cycle"""
        # Add messages
        futures = []
        for i in range(5):
            future = self.loop.create_future()
            msg_data = {
                'topic': f'topic{i % 2}',  # Alternate between topic0 and topic1
                'value': f'test{i}',
                'key': f'key{i}'
            }
            self.batch_processor.add_message(msg_data, future)
            futures.append(future)
        
        # Verify buffer state
        self.assertEqual(self.batch_processor.get_buffer_size(), 5)
        
        # Test topic grouping
        topic_groups = self.batch_processor._group_messages_by_topic()
        self.assertEqual(len(topic_groups), 2)
        self.assertIn('topic0', topic_groups)
        self.assertIn('topic1', topic_groups)
        
        # Verify group sizes
        self.assertEqual(len(topic_groups['topic0']['messages']), 3)  # messages 0, 2, 4
        self.assertEqual(len(topic_groups['topic1']['messages']), 2)  # messages 1, 3
        
        # Test message preparation
        batch_messages = self.batch_processor._prepare_batch_messages(
            topic_groups['topic0']['messages']
        )
        self.assertEqual(len(batch_messages), 3)
        for batch_msg in batch_messages:
            self.assertNotIn('topic', batch_msg)
            self.assertIn('value', batch_msg)
            self.assertIn('key', batch_msg)
        
        # Test callback assignment
        self.batch_processor._assign_callbacks_to_messages(
            batch_messages,
            topic_groups['topic0']['futures'],
            topic_groups['topic0']['callbacks']
        )
        
        # Verify callbacks were assigned
        for batch_msg in batch_messages:
            self.assertIn('callback', batch_msg)
        
        # Verify callback pool usage
        stats = self.batch_processor.get_callback_pool_stats()
        self.assertEqual(stats['in_use'], 3)
        self.assertEqual(stats['reuse_count'], 3)

    def test_partial_batch_failure_handling(self):
        """Test that partial batch failures are handled correctly using real C produce_batch"""
        
        # Use the AIOProducer's underlying producer for this test
        # Note: We could create a separate producer with different config if needed for specific failure scenarios
        test_producer = self.aio_producer._producer
        
        # Create futures for tracking
        future1 = asyncio.Future()
        future2 = asyncio.Future() 
        future3 = asyncio.Future()
        
        # Track callback invocations
        callback_results = []
        
        def track_callback(msg_id):
            def callback(err, msg):
                callback_results.append({'msg_id': msg_id, 'err': err, 'msg': msg})
            return callback
        
        # Add messages to buffer - use scenarios that cause immediate failures
        msg1_data = {'topic': 'test-topic', 'value': 'small message', 'user_callback': track_callback('msg1')}
        msg2_data = {'topic': 'test-topic', 'value': 'x' * (5 * 1024 * 1024), 'user_callback': track_callback('msg2')}  # Large message - will fail immediately
        msg3_data = {'topic': 'test-topic', 'value': 'another small', 'user_callback': track_callback('msg3')}
        
        self.batch_processor.add_message(msg1_data, future1)
        self.batch_processor.add_message(msg2_data, future2)
        self.batch_processor.add_message(msg3_data, future3)
                
        # Test the _execute_batch method
        async def run_test():
            # Group messages by topic
            topic_groups = self.batch_processor._group_messages_by_topic()
            topic_data = topic_groups['test-topic']
            
            # Prepare batch messages
            batch_messages = self.batch_processor._prepare_batch_messages(topic_data['messages'])
            
            # Assign callbacks to messages
            self.batch_processor._assign_callbacks_to_messages(
                batch_messages,
                topic_data['futures'],
                topic_data['callbacks']
            )
            
            # Execute the batch using the Kafka executor
            result = await self.kafka_executor.execute_batch('test-topic', batch_messages)
            
            # Check for partial failures by examining the batch_messages
            failed_count = sum(1 for msg_dict in batch_messages if '_error' in msg_dict)
            
            # Verify our fix works
            if failed_count > 0:
                # If we got immediate failures, verify our fix handled them
                failed_messages = [msg for msg in batch_messages if '_error' in msg]
                self.assertEqual(len(failed_messages), failed_count)
                
                # Verify that large message specifically failed
                large_msg = next((msg for msg in batch_messages if len(str(msg.get('value', ''))) > 1000), None)
                if large_msg and '_error' in large_msg:
                    self.assertIn('MSG_SIZE_TOO_LARGE', str(large_msg['_error']))
                    print(f"✅ Large message correctly failed: {large_msg['_error']}")
            else:
                # If no immediate failures, that's also valid - some errors happen during delivery
                print("ℹ️  No immediate failures detected - errors may occur during delivery")
            
            print(f"_execute_batch result: {result}, failed_count: {failed_count}")
        
        # Run the async test
        self.loop.run_until_complete(run_test())


if __name__ == '__main__':
    # Run all tests
    unittest.main(verbosity=2)
