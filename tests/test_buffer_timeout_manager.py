#!/usr/bin/env python3
"""
Unit tests for the BufferTimeoutManager class (_buffer_timeout_manager.py)

This module tests the BufferTimeoutManager class to ensure proper
buffer timeout monitoring and automatic flush handling.
"""

from confluent_kafka.experimental.aio.producer._buffer_timeout_manager import BufferTimeoutManager
import asyncio
import unittest
from unittest.mock import Mock, AsyncMock, patch
import sys
import os
import time

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))


class TestBufferTimeoutManager(unittest.TestCase):
    """Test cases for BufferTimeoutManager class"""

    def setUp(self):
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)

        # Create mock dependencies
        self.mock_batch_processor = Mock()
        self.mock_kafka_executor = Mock()
        
        # Configure mock methods as async
        self.mock_batch_processor.flush_buffer = AsyncMock()
        self.mock_batch_processor.is_buffer_empty = Mock(return_value=False)
        self.mock_kafka_executor.flush_librdkafka_queue = AsyncMock(return_value=0)

        # Create timeout manager with 1 second timeout
        self.timeout_manager = BufferTimeoutManager(
            self.mock_batch_processor,
            self.mock_kafka_executor,
            timeout=1.0
        )

    def tearDown(self):
        # Stop any running timeout monitoring tasks
        if hasattr(self.timeout_manager, '_timeout_task') and self.timeout_manager._timeout_task:
            self.timeout_manager.stop_timeout_monitoring()
        self.loop.close()

    def test_initialization(self):
        """Test that BufferTimeoutManager initializes correctly"""
        self.assertEqual(self.timeout_manager._batch_processor, self.mock_batch_processor)
        self.assertEqual(self.timeout_manager._kafka_executor, self.mock_kafka_executor)
        self.assertEqual(self.timeout_manager._timeout, 1.0)
        self.assertFalse(self.timeout_manager._running)
        self.assertIsNone(self.timeout_manager._timeout_task)
        self.assertIsInstance(self.timeout_manager._last_activity, float)
        self.assertGreater(self.timeout_manager._last_activity, 0)

    def test_mark_activity(self):
        """Test that mark_activity updates the last activity timestamp"""
        initial_time = self.timeout_manager._last_activity
        time.sleep(0.01)  # Ensure time difference
        
        self.timeout_manager.mark_activity()
        
        self.assertGreater(self.timeout_manager._last_activity, initial_time)

    def test_start_timeout_monitoring(self):
        """Test that timeout monitoring task starts correctly"""
        async def async_test():
            self.timeout_manager.start_timeout_monitoring()
            
            self.assertTrue(self.timeout_manager._running)
            self.assertIsNotNone(self.timeout_manager._timeout_task)
            self.assertFalse(self.timeout_manager._timeout_task.done())
            
            # Clean up
            self.timeout_manager.stop_timeout_monitoring()
        
        self.loop.run_until_complete(async_test())

    def test_stop_timeout_monitoring(self):
        """Test that timeout monitoring task stops correctly"""
        async def async_test():
            self.timeout_manager.start_timeout_monitoring()
            self.assertTrue(self.timeout_manager._running)
            
            self.timeout_manager.stop_timeout_monitoring()
            
            self.assertFalse(self.timeout_manager._running)
            # Task should be cancelled or None
            self.assertTrue(
                self.timeout_manager._timeout_task is None or 
                self.timeout_manager._timeout_task.done()
            )
        
        self.loop.run_until_complete(async_test())

    def test_start_timeout_monitoring_disabled(self):
        """Test that timeout monitoring doesn't start when timeout is 0"""
        manager = BufferTimeoutManager(
            self.mock_batch_processor,
            self.mock_kafka_executor,
            timeout=0  # Disabled
        )
        
        manager.start_timeout_monitoring()
        
        self.assertFalse(manager._running)
        self.assertIsNone(manager._timeout_task)

    def test_flush_buffer_due_to_timeout(self):
        """Test that _flush_buffer_due_to_timeout calls both flush methods"""
        async def async_test():
            # Call the flush method
            await self.timeout_manager._flush_buffer_due_to_timeout()
            
            # Verify both flush operations were called
            self.mock_batch_processor.flush_buffer.assert_called_once()
            self.mock_kafka_executor.flush_librdkafka_queue.assert_called_once()

        self.loop.run_until_complete(async_test())

    def test_flush_buffer_due_to_timeout_order(self):
        """Test that flush operations are called in the correct order"""
        call_order = []
        
        async def track_batch_flush():
            call_order.append('batch_processor')
        
        async def track_kafka_flush():
            call_order.append('kafka_executor')
        
        self.mock_batch_processor.flush_buffer.side_effect = track_batch_flush
        self.mock_kafka_executor.flush_librdkafka_queue.side_effect = track_kafka_flush
        
        async def async_test():
            await self.timeout_manager._flush_buffer_due_to_timeout()
            
            # Verify order: batch processor first, then kafka executor
            self.assertEqual(call_order, ['batch_processor', 'kafka_executor'])

        self.loop.run_until_complete(async_test())

    def test_flush_buffer_due_to_timeout_batch_processor_error(self):
        """Test that errors from batch processor are propagated"""
        self.mock_batch_processor.flush_buffer.side_effect = Exception("Batch flush failed")
        
        async def async_test():
            with self.assertRaises(Exception) as context:
                await self.timeout_manager._flush_buffer_due_to_timeout()
            
            self.assertEqual(str(context.exception), "Batch flush failed")
            # Batch processor flush was attempted
            self.mock_batch_processor.flush_buffer.assert_called_once()
            # Kafka executor flush should not be called if batch processor fails
            self.mock_kafka_executor.flush_librdkafka_queue.assert_not_called()

        self.loop.run_until_complete(async_test())

    def test_flush_buffer_due_to_timeout_kafka_executor_error(self):
        """Test that errors from kafka executor are propagated"""
        self.mock_kafka_executor.flush_librdkafka_queue.side_effect = Exception("Kafka flush failed")
        
        async def async_test():
            with self.assertRaises(Exception) as context:
                await self.timeout_manager._flush_buffer_due_to_timeout()
            
            self.assertEqual(str(context.exception), "Kafka flush failed")
            # Both flush operations were attempted
            self.mock_batch_processor.flush_buffer.assert_called_once()
            self.mock_kafka_executor.flush_librdkafka_queue.assert_called_once()

        self.loop.run_until_complete(async_test())

    def test_monitor_timeout_triggers_flush(self):
        """Test that timeout monitoring triggers flush when buffer is inactive"""
        async def async_test():
            # Set last activity to past time (simulating inactivity)
            self.timeout_manager._last_activity = time.time() - 2.0  # 2 seconds ago
            
            # Start monitoring
            self.timeout_manager.start_timeout_monitoring()
            
            # Wait for timeout check to trigger
            # Check interval for 1s timeout should be 0.5s, add buffer time
            await asyncio.sleep(0.7)
            
            # Stop monitoring
            self.timeout_manager.stop_timeout_monitoring()
            
            # Verify flush was called
            self.mock_batch_processor.flush_buffer.assert_called()
            self.mock_kafka_executor.flush_librdkafka_queue.assert_called()

        self.loop.run_until_complete(async_test())

    def test_monitor_timeout_does_not_flush_empty_buffer(self):
        """Test that timeout monitoring doesn't flush if buffer is empty"""
        # Configure buffer as empty
        self.mock_batch_processor.is_buffer_empty.return_value = True
        
        async def async_test():
            # Set last activity to past time
            self.timeout_manager._last_activity = time.time() - 2.0
            
            # Start monitoring
            self.timeout_manager.start_timeout_monitoring()
            
            # Wait for potential timeout check
            await asyncio.sleep(0.7)
            
            # Stop monitoring
            self.timeout_manager.stop_timeout_monitoring()
            
            # Verify flush was NOT called for empty buffer
            self.mock_batch_processor.flush_buffer.assert_not_called()
            self.mock_kafka_executor.flush_librdkafka_queue.assert_not_called()

        self.loop.run_until_complete(async_test())

    def test_monitor_timeout_does_not_flush_recent_activity(self):
        """Test that timeout monitoring doesn't flush if buffer has recent activity"""
        async def async_test():
            # Set last activity to recent time (within timeout)
            self.timeout_manager._last_activity = time.time() - 0.3  # 0.3 seconds ago
            
            # Start monitoring
            self.timeout_manager.start_timeout_monitoring()
            
            # Wait for potential timeout check (less than timeout duration)
            # 0.3s (initial) + 0.4s (sleep) = 0.7s < 1.0s timeout
            await asyncio.sleep(0.4)
            
            # Stop monitoring
            self.timeout_manager.stop_timeout_monitoring()
            
            # Verify flush was NOT called since activity is recent
            self.mock_batch_processor.flush_buffer.assert_not_called()
            self.mock_kafka_executor.flush_librdkafka_queue.assert_not_called()

        self.loop.run_until_complete(async_test())

    def test_monitor_timeout_updates_activity_after_flush(self):
        """Test that timeout monitoring updates activity timestamp after successful flush"""
        async def async_test():
            # Set last activity to past time
            self.timeout_manager._last_activity = time.time() - 2.0
            initial_time = self.timeout_manager._last_activity
            
            # Start monitoring
            self.timeout_manager.start_timeout_monitoring()
            
            # Wait for timeout check to trigger
            await asyncio.sleep(0.7)
            
            # Stop monitoring
            self.timeout_manager.stop_timeout_monitoring()
            
            # Verify activity was updated after flush
            self.assertGreater(self.timeout_manager._last_activity, initial_time)

        self.loop.run_until_complete(async_test())

    def test_weak_reference_cleanup(self):
        """Test that weak references allow proper garbage collection"""
        async def async_test():
            # Create a manager with timeout monitoring
            manager = BufferTimeoutManager(
                self.mock_batch_processor,
                self.mock_kafka_executor,
                timeout=1.0
            )
            manager.start_timeout_monitoring()
            
            # Wait a bit for task to start
            await asyncio.sleep(0.1)
            
            # Delete the manager
            task = manager._timeout_task
            del manager
            
            # Wait for task to notice manager is gone
            await asyncio.sleep(0.6)
            
            # Task should stop itself when manager is garbage collected
            # In practice, the task checks if manager_ref() is None
            # and breaks out of the loop
            # This test mainly verifies the pattern is in place

        self.loop.run_until_complete(async_test())


if __name__ == '__main__':
    unittest.main()

