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

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from confluent_kafka.aio._producer_batch_processor import ProducerBatchProcessor


class TestProducerBatchProcessor(unittest.TestCase):
    """Test cases for ProducerBatchProcessor class"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        self.batch_processor = ProducerBatchProcessor(callback_pool_size=10)
        self.mock_producer = Mock()
        self.mock_executor = Mock()
        self.mock_aio_producer = Mock()
        self.mock_aio_producer.executor = self.mock_executor
        self.mock_aio_producer._producer = self.mock_producer
    
    def tearDown(self):
        """Clean up test fixtures"""
        self.loop.close()
    
    def test_initialization(self):
        """Test ProducerBatchProcessor initialization"""
        processor = ProducerBatchProcessor(callback_pool_size=50)
        
        self.assertEqual(processor.get_buffer_size(), 0)
        self.assertTrue(processor.is_buffer_empty())
        
        # Check callback pool stats
        stats = processor.get_callback_pool_stats()
        self.assertEqual(stats['available'], 50)
        self.assertEqual(stats['created_total'], 50)
    
    def test_add_message(self):
        """Test adding messages to the buffer"""
        future1 = Mock()
        future2 = Mock()
        msg_data1 = {'topic': 'topic1', 'value': 'test1', 'key': 'key1'}
        msg_data2 = {'topic': 'topic2', 'value': 'test2', 'key': 'key2'}
        
        # Add messages
        self.batch_processor.add_message(msg_data1, future1)
        self.batch_processor.add_message(msg_data2, future2)
        
        # Verify buffer state
        self.assertEqual(self.batch_processor.get_buffer_size(), 2)
        self.assertFalse(self.batch_processor.is_buffer_empty())
    
    def test_clear_buffer(self):
        """Test clearing the buffer"""
        # Add some messages
        self.batch_processor.add_message({'topic': 'test'}, Mock())
        self.batch_processor.add_message({'topic': 'test'}, Mock())
        
        # Verify buffer has messages
        self.assertEqual(self.batch_processor.get_buffer_size(), 2)
        
        # Clear buffer
        self.batch_processor.clear_buffer()
        
        # Verify buffer is empty
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
            batch_messages, futures, user_callbacks, self.mock_aio_producer
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
            exception, futures, user_callbacks, self.mock_aio_producer
        )
        
        # Verify first future got exception (not already done)
        futures[0].set_exception.assert_called_once_with(exception)
        
        # Verify second future was not modified (already done)
        futures[1].set_exception.assert_not_called()
        
        # Verify user callback was called
        self.mock_aio_producer._handle_user_callback.assert_called_once_with(
            user_callbacks[0], exception, None
        )
    
    def test_flush_empty_buffer(self):
        """Test flushing empty buffer"""
        async def async_test():
            # Should return immediately without doing anything
            await self.batch_processor.flush_buffer(
                self.mock_aio_producer
            )
            
            # Verify no operations were performed
            self.mock_executor.submit.assert_not_called()
        
        self.loop.run_until_complete(async_test())
    
    def test_execute_batch(self):
        """Test executing a batch operation"""
        async def async_test():
            batch_messages = [
                {'value': 'test1', 'callback': Mock()},
                {'value': 'test2', 'callback': Mock()},
            ]
            
            # Mock the executor to return a completed future
            future_result = self.loop.create_future()
            future_result.set_result("poll_result")
            
            with patch.object(self.loop, 'run_in_executor', return_value=future_result) as mock_run_in_executor:
                result = await self.batch_processor._execute_batch(
                    self.mock_executor, self.mock_producer, 'test-topic', batch_messages
                )
                
                # Verify run_in_executor was called
                mock_run_in_executor.assert_called_once()
                self.assertEqual(result, "poll_result")
        
        self.loop.run_until_complete(async_test())
    
    def test_flush_buffer_with_messages(self):
        """Test flushing buffer with actual messages"""
        async def async_test():
            # Add messages to buffer
            future1 = self.loop.create_future()
            future2 = self.loop.create_future()
            
            msg1 = {'topic': 'topic1', 'value': 'test1'}
            msg2 = {'topic': 'topic1', 'value': 'test2'}
            
            self.batch_processor.add_message(msg1, future1)
            self.batch_processor.add_message(msg2, future2)
            
            # Mock successful batch execution
            future_result = self.loop.create_future()
            future_result.set_result("success")
            
            with patch.object(self.loop, 'run_in_executor', return_value=future_result):
                await self.batch_processor.flush_buffer(
                    self.mock_aio_producer
                )
            
            # Verify buffer was cleared
            self.assertTrue(self.batch_processor.is_buffer_empty())
        
        self.loop.run_until_complete(async_test())
    
    def test_flush_buffer_with_target_topic(self):
        """Test flushing buffer with specific target topic"""
        async def async_test():
            # Add messages for different topics
            future1 = self.loop.create_future()
            future2 = self.loop.create_future()
            
            msg1 = {'topic': 'topic1', 'value': 'test1'}
            msg2 = {'topic': 'topic2', 'value': 'test2'}
            
            self.batch_processor.add_message(msg1, future1)
            self.batch_processor.add_message(msg2, future2)
            
            # Mock successful batch execution
            future_result = self.loop.create_future()
            future_result.set_result("success")
            
            with patch.object(self.loop, 'run_in_executor', return_value=future_result):
                # Flush only topic1
                await self.batch_processor.flush_buffer(
                    self.mock_aio_producer, target_topic='topic1'
                )
            
            # Verify only topic1 was processed, topic2 remains in buffer
            self.assertEqual(self.batch_processor.get_buffer_size(), 1)
        
        self.loop.run_until_complete(async_test())
    
    def test_flush_buffer_with_exception(self):
        """Test flushing buffer when batch execution fails"""
        async def async_test():
            # Add message to buffer
            future1 = self.loop.create_future()
            msg1 = {'topic': 'topic1', 'value': 'test1'}
            self.batch_processor.add_message(msg1, future1)
            
            # Mock failed batch execution
            exception = RuntimeError("Batch execution failed")
            
            with patch.object(self.loop, 'run_in_executor', side_effect=exception):
                with self.assertRaises(RuntimeError):
                    await self.batch_processor.flush_buffer(
                        self.mock_aio_producer
                    )
            
            # Verify buffer was still cleared (messages were processed, just failed)
            self.assertTrue(self.batch_processor.is_buffer_empty())
        
        self.loop.run_until_complete(async_test())
    
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
            topic_groups['topic0']['callbacks'],
            self.mock_aio_producer
        )
        
        # Verify callbacks were assigned
        for batch_msg in batch_messages:
            self.assertIn('callback', batch_msg)
        
        # Verify callback pool usage
        stats = self.batch_processor.get_callback_pool_stats()
        self.assertEqual(stats['in_use'], 3)
        self.assertEqual(stats['reuse_count'], 3)


if __name__ == '__main__':
    # Run all tests
    unittest.main(verbosity=2)
