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

from confluent_kafka.aio.producer._producer_batch_processor import ProducerBatchManager as ProducerBatchProcessor
from confluent_kafka.aio.producer._AIOProducer import AIOProducer
from confluent_kafka.aio.producer._kafka_batch_executor import ProducerBatchExecutor as KafkaBatchExecutor


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
        
        topic_groups = self.batch_processor._group_messages_by_topic()
        
        self.assertEqual(len(topic_groups), 2)
        
        self.assertIn('topic1', topic_groups)
        topic1_group = topic_groups['topic1']
        self.assertEqual(len(topic1_group['messages']), 2)
        self.assertEqual(len(topic1_group['futures']), 2)
        self.assertEqual(topic1_group['futures'][0], future1)
        self.assertEqual(topic1_group['futures'][1], future3)
        
        self.assertIn('topic2', topic_groups)
        topic2_group = topic_groups['topic2']
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
        
        self.assertNotIn('user_callback', batch_messages[0])
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
                'user_callback': user_callback
            }
            
            self.batch_processor.add_message(msg_data, future)
            messages_data.append(msg_data)
            futures.append(future)
            user_callbacks.append(user_callback)
        
        return messages_data, futures, user_callbacks
    
    def test_callback_future_mapping_setup(self):
        """Test callback-future mapping during message setup"""
        messages_data, futures, user_callbacks = self._create_mixed_topic_messages()
        topic_groups = self.batch_processor._group_messages_by_topic()
        
        self.assertEqual(len(topic_groups), 2)
        for topic, group_data in topic_groups.items():
            self.assertIn('messages', group_data)
            self.assertIn('futures', group_data)
            self.assertIn('callbacks', group_data)
    
    def test_callback_assignment_correctness(self):
        """Test that callbacks are assigned to correct futures"""
        self._create_mixed_topic_messages()
        topic_groups = self.batch_processor._group_messages_by_topic()
        
        for topic, group_data in topic_groups.items():
            batch_messages = self.batch_processor._prepare_batch_messages(group_data['messages'])
            self.batch_processor._assign_future_callbacks(
                batch_messages, group_data['futures']
            )
            
            for i, batch_msg in enumerate(batch_messages):
                callback = batch_msg['callback']
                expected_future = group_data['futures'][i]
                expected_user_callback = group_data['callbacks'][i]
                
                self.assertEqual(callback.future, expected_future)
                self.assertEqual(callback.user_callback, expected_user_callback)
    
    def test_callback_execution_with_success(self):
        """Test callback execution resolves correct futures"""
        self._create_mixed_topic_messages()
        topic_groups = self.batch_processor._group_messages_by_topic()
        
        for topic, group_data in topic_groups.items():
            batch_messages = self.batch_processor._prepare_batch_messages(group_data['messages'])
            self.batch_processor._assign_future_callbacks(
                batch_messages, group_data['futures']
            )
            
            for i, batch_msg in enumerate(batch_messages):
                callback = batch_msg['callback']
                expected_future = group_data['futures'][i]
                
                test_msg = Mock()
                test_msg.value.return_value = batch_msg['value']
                
                callback(None, test_msg)
                expected_future.set_result.assert_called_once_with(test_msg)
    
    def _add_alternating_topic_messages(self):
        """Helper to add messages alternating between two topics"""
        futures = []
        for i in range(5):
            future = self.loop.create_future()
            msg_data = {
                'topic': f'topic{i % 2}',
                'value': f'test{i}',
                'key': f'key{i}'
            }
            self.batch_processor.add_message(msg_data, future)
            futures.append(future)
        return futures
    
    def test_batch_cycle_buffer_state(self):
        """Test buffer state during batch cycle"""
        futures = self._add_alternating_topic_messages()
        self.assertEqual(self.batch_processor.get_buffer_size(), 5)
        self.assertFalse(self.batch_processor.is_buffer_empty())
    
    def test_batch_cycle_topic_grouping(self):
        """Test topic grouping in batch cycle"""
        self._add_alternating_topic_messages()
        topic_groups = self.batch_processor._group_messages_by_topic()
        
        self.assertEqual(len(topic_groups), 2)
        self.assertIn('topic0', topic_groups)
        self.assertIn('topic1', topic_groups)
        self.assertEqual(len(topic_groups['topic0']['messages']), 3)
        self.assertEqual(len(topic_groups['topic1']['messages']), 2)
    
    def test_batch_cycle_message_preparation(self):
        """Test message preparation in batch cycle"""
        self._add_alternating_topic_messages()
        topic_groups = self.batch_processor._group_messages_by_topic()
        
        batch_messages = self.batch_processor._prepare_batch_messages(
            topic_groups['topic0']['messages']
        )
        
        self.assertEqual(len(batch_messages), 3)
        for batch_msg in batch_messages:
            self.assertNotIn('topic', batch_msg)
            self.assertIn('value', batch_msg)
            self.assertIn('key', batch_msg)
    
    def test_batch_cycle_callback_pool_usage(self):
        """Test callback pool usage in batch cycle"""
        self._add_alternating_topic_messages()
        topic_groups = self.batch_processor._group_messages_by_topic()
        
        batch_messages = self.batch_processor._prepare_batch_messages(
            topic_groups['topic0']['messages']
        )
        
        self.batch_processor._assign_future_callbacks(
            batch_messages,
            topic_groups['topic0']['futures']
        )
        
        for batch_msg in batch_messages:
            self.assertIn('callback', batch_msg)
        
        # Callback pool stats not available in actual implementation

    def _create_test_messages_with_tracking(self):
        """Helper to create test messages with callback tracking"""
        callback_results = []
        
        def track_callback(msg_id):
            def callback(err, msg):
                callback_results.append({'msg_id': msg_id, 'err': err, 'msg': msg})
            return callback
        
        msg1_data = {'topic': 'test-topic', 'value': 'small message', 'user_callback': track_callback('msg1')}
        msg2_data = {'topic': 'test-topic', 'value': 'x' * (5 * 1024 * 1024), 'user_callback': track_callback('msg2')}
        msg3_data = {'topic': 'test-topic', 'value': 'another small', 'user_callback': track_callback('msg3')}
        
        return [msg1_data, msg2_data, msg3_data], callback_results
    
    def test_batch_message_preparation_with_mixed_sizes(self):
        """Test batch message preparation with mixed message sizes"""
        messages, _ = self._create_test_messages_with_tracking()
        futures = [asyncio.Future(), asyncio.Future(), asyncio.Future()]
        
        for msg, future in zip(messages, futures):
            self.batch_processor.add_message(msg, future)
        
        topic_groups = self.batch_processor._group_messages_by_topic()
        topic_data = topic_groups['test-topic']
        batch_messages = self.batch_processor._prepare_batch_messages(topic_data['messages'])
        
        self.assertEqual(len(batch_messages), 3)
        large_msg = next((msg for msg in batch_messages if len(str(msg.get('value', ''))) > 1000), None)
        self.assertIsNotNone(large_msg)
    
    def test_batch_callback_assignment_with_futures(self):
        """Test callback assignment to batch messages with futures"""
        messages, _ = self._create_test_messages_with_tracking()
        futures = [asyncio.Future(), asyncio.Future(), asyncio.Future()]
        
        for msg, future in zip(messages, futures):
            self.batch_processor.add_message(msg, future)
        
        topic_groups = self.batch_processor._group_messages_by_topic()
        topic_data = topic_groups['test-topic']
        batch_messages = self.batch_processor._prepare_batch_messages(topic_data['messages'])
        
        self.batch_processor._assign_future_callbacks(
            batch_messages, topic_data['futures'], topic_data['callbacks']
        )
        
        for batch_msg in batch_messages:
            self.assertIn('callback', batch_msg)
    
    def test_batch_executor_with_oversized_messages(self):
        """Test batch executor handles oversized messages correctly"""
        messages, callback_results = self._create_test_messages_with_tracking()
        futures = [asyncio.Future(), asyncio.Future(), asyncio.Future()]
        
        for msg, future in zip(messages, futures):
            self.batch_processor.add_message(msg, future)
        
        async def run_test():
            topic_groups = self.batch_processor._group_messages_by_topic()
            topic_data = topic_groups['test-topic']
            batch_messages = self.batch_processor._prepare_batch_messages(topic_data['messages'])
            
            self.batch_processor._assign_future_callbacks(
                batch_messages, topic_data['futures']
            )
            
            result = await self.kafka_executor.execute_batch('test-topic', batch_messages)
            failed_count = sum(1 for msg_dict in batch_messages if '_error' in msg_dict)
            
            if failed_count > 0:
                failed_messages = [msg for msg in batch_messages if '_error' in msg]
                self.assertEqual(len(failed_messages), failed_count)
                
                large_msg = next((msg for msg in batch_messages if len(str(msg.get('value', ''))) > 1000), None)
                if large_msg and '_error' in large_msg:
                    self.assertIn('MSG_SIZE_TOO_LARGE', str(large_msg['_error']))
        
        self.loop.run_until_complete(run_test())


if __name__ == '__main__':
    # Run all tests
    unittest.main(verbosity=2)
