#!/usr/bin/env python3
"""
Unit tests for the callback pool module (_callback_pool.py)

This module tests the ReusableMessageCallback and CallbackPool classes
to ensure proper callback pooling, reuse, and memory management.
"""

import unittest
from unittest.mock import Mock, MagicMock
import sys
import os

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from confluent_kafka.aio.producer._callback_manager import ReusableMessageCallback, CallbackManager
from confluent_kafka import KafkaException


class TestCallbackManager(unittest.TestCase):
    """Test cases for ReusableMessageCallback and CallbackManager classes"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.callback = ReusableMessageCallback()
        self.mock_future = Mock()
        self.mock_future.done.return_value = False
        self.mock_user_callback = Mock()
        self.mock_pool = Mock()
        self.mock_producer = Mock()
    
    def test_reusable_callback_initialization(self):
        """Test ReusableMessageCallback initialization"""
        callback = ReusableMessageCallback()
        self.assertIsNone(callback.future)
        self.assertIsNone(callback.user_callback)
        self.assertIsNone(callback.pool)
        self.assertIsNone(callback.producer)
    
    def test_reusable_callback_reset(self):
        """Test ReusableMessageCallback reset functionality"""
        self.callback.reset(
            self.mock_future,
            self.mock_user_callback,
            self.mock_pool,
            self.mock_producer
        )
        
        self.assertEqual(self.callback.future, self.mock_future)
        self.assertEqual(self.callback.user_callback, self.mock_user_callback)
        self.assertEqual(self.callback.pool, self.mock_pool)
        self.assertEqual(self.callback.producer, self.mock_producer)
    
    def test_reusable_callback_successful_delivery(self):
        """Test ReusableMessageCallback execution for successful message delivery"""
        # Setup
        self.callback.reset(
            self.mock_future,
            self.mock_user_callback,
            self.mock_pool,
            self.mock_producer
        )
        
        mock_msg = Mock()
        
        # Call the callback with successful delivery (err=None)
        self.callback(None, mock_msg)
        
        # Verify future was set with result
        self.mock_future.set_result.assert_called_once_with(mock_msg)
        self.mock_future.set_exception.assert_not_called()
        
        # Verify user callback was called
        self.mock_producer._handle_user_callback.assert_called_once_with(
            self.mock_user_callback, None, mock_msg
        )
        
        # Verify callback was returned to pool
        self.mock_pool.return_callback.assert_called_once_with(self.callback)
        
        # Verify callback was cleared
        self.assertIsNone(self.callback.future)
        self.assertIsNone(self.callback.user_callback)
        self.assertIsNone(self.callback.producer)
    
    def test_reusable_callback_failed_delivery(self):
        """Test ReusableMessageCallback execution for failed message delivery"""
        # Setup
        self.callback.reset(
            self.mock_future,
            self.mock_user_callback,
            self.mock_pool,
            self.mock_producer
        )
        
        mock_error = Mock()
        mock_msg = Mock()
        
        # Call the callback with failed delivery
        self.callback(mock_error, mock_msg)
        
        # Verify future was set with exception
        self.mock_future.set_exception.assert_called_once()
        # Check that a KafkaException was passed
        exception_arg = self.mock_future.set_exception.call_args[0][0]
        self.assertIsInstance(exception_arg, KafkaException)
        
        self.mock_future.set_result.assert_not_called()
        
        # Verify user callback was called with error
        self.mock_producer._handle_user_callback.assert_called_once_with(
            self.mock_user_callback, mock_error, mock_msg
        )
        
        # Verify callback was returned to pool
        self.mock_pool.return_callback.assert_called_once_with(self.callback)
    
    def test_reusable_callback_without_user_callback(self):
        """Test ReusableMessageCallback execution when no user callback is provided"""
        # Setup without user callback
        self.callback.reset(
            self.mock_future,
            None,  # No user callback
            self.mock_pool,
            self.mock_producer
        )
        
        mock_msg = Mock()
        
        # Call the callback with successful delivery
        self.callback(None, mock_msg)
        
        # Verify future was set
        self.mock_future.set_result.assert_called_once_with(mock_msg)
        
        # Verify user callback was not called (since it's None)
        self.mock_producer._handle_user_callback.assert_not_called()
        
        # Verify callback was still returned to pool
        self.mock_pool.return_callback.assert_called_once_with(self.callback)
    
    def test_reusable_callback_with_already_done_future(self):
        """Test ReusableMessageCallback execution when future is already done"""
        # Setup with done future
        self.mock_future.done.return_value = True
        self.callback.reset(
            self.mock_future,
            self.mock_user_callback,
            self.mock_pool,
            self.mock_producer
        )
        
        mock_msg = Mock()
        
        # Call the callback
        self.callback(None, mock_msg)
        
        # Verify future methods were not called (already done)
        self.mock_future.set_result.assert_not_called()
        self.mock_future.set_exception.assert_not_called()
        
        # Verify user callback was still called
        self.mock_producer._handle_user_callback.assert_called_once()
        
        # Verify callback was returned to pool
        self.mock_pool.return_callback.assert_called_once_with(self.callback)
    
    def test_reusable_callback_exception_handling(self):
        """Test ReusableMessageCallback handles exceptions during processing"""
        # Setup
        self.callback.reset(
            self.mock_future,
            self.mock_user_callback,
            self.mock_pool,
            self.mock_producer
        )
        
        # Make producer._handle_user_callback raise an exception
        self.mock_producer._handle_user_callback.side_effect = RuntimeError("Test error")
        
        mock_msg = Mock()
        
        # Call the callback - should not raise exception
        self.callback(None, mock_msg)
        
        # Verify future was still set despite the exception
        self.mock_future.set_result.assert_called_once_with(mock_msg)
        
        # Verify callback was still returned to pool despite exception
        self.mock_pool.return_callback.assert_called_once_with(self.callback)
    
    def test_pool_initialization_with_default_size(self):
        """Test CallbackPool initialization with default size"""
        pool = CallbackPool()
        
        # Default size should be 1000
        self.assertEqual(len(pool._available), 1000)
        self.assertEqual(len(pool._in_use), 0)
        self.assertEqual(pool._created_count, 1000)
        self.assertEqual(pool._reuse_count, 0)
        
        # All callbacks should be ReusableMessageCallback instances
        for callback in pool._available:
            self.assertIsInstance(callback, ReusableMessageCallback)
    
    def test_pool_initialization_with_custom_size(self):
        """Test CallbackPool initialization with custom size"""
        pool = CallbackPool(initial_size=50)
        
        self.assertEqual(len(pool._available), 50)
        self.assertEqual(len(pool._in_use), 0)
        self.assertEqual(pool._created_count, 50)
        self.assertEqual(pool._reuse_count, 0)
    
    def test_pool_get_callback_from_pool(self):
        """Test getting a callback from CallbackPool (reuse scenario)"""
        pool = CallbackPool(initial_size=10)
        initial_available = len(pool._available)
        
        # Get a callback
        callback = pool.get_callback(
            self.mock_future,
            self.mock_user_callback,
            self.mock_producer
        )
        
        # Verify callback is properly configured
        self.assertIsInstance(callback, ReusableMessageCallback)
        self.assertEqual(callback.future, self.mock_future)
        self.assertEqual(callback.user_callback, self.mock_user_callback)
        self.assertEqual(callback.producer, self.mock_producer)
        self.assertEqual(callback.pool, pool)
        
        # Verify pool state
        self.assertEqual(len(pool._available), initial_available - 1)
        self.assertEqual(len(pool._in_use), 1)
        self.assertIn(callback, pool._in_use)
        self.assertEqual(pool._reuse_count, 1)  # First reuse
        self.assertEqual(pool._created_count, 10)  # No new creation
    
    def test_pool_get_callback_pool_expansion(self):
        """Test getting a callback from CallbackPool when pool is empty (expansion scenario)"""
        pool = CallbackPool(initial_size=0)  # Empty pool
        
        # Get a callback - should trigger pool expansion
        callback = pool.get_callback(
            self.mock_future,
            self.mock_user_callback,
            self.mock_producer
        )
        
        # Verify callback is properly configured
        self.assertIsInstance(callback, ReusableMessageCallback)
        self.assertEqual(callback.future, self.mock_future)
        
        # Verify pool state - new callback was created
        self.assertEqual(len(pool._available), 0)
        self.assertEqual(len(pool._in_use), 1)
        self.assertEqual(pool._created_count, 1)  # New callback created
        self.assertEqual(pool._reuse_count, 0)  # No reuse, new creation
    
    def test_pool_return_callback_to_pool(self):
        """Test returning a callback to CallbackPool"""
        pool = CallbackPool(initial_size=5)
        
        # Get a callback
        callback = pool.get_callback(
            self.mock_future,
            self.mock_user_callback,
            self.mock_producer
        )
        
        initial_available = len(pool._available)
        
        # Return the callback
        pool.return_callback(callback)
        
        # Verify pool state
        self.assertEqual(len(pool._available), initial_available + 1)
        self.assertEqual(len(pool._in_use), 0)
        self.assertNotIn(callback, pool._in_use)
        self.assertIn(callback, pool._available)
    
    def test_pool_return_callback_not_in_use(self):
        """Test returning a callback to CallbackPool that's not in use (should be ignored)"""
        pool = CallbackPool(initial_size=5)
        callback = ReusableMessageCallback()  # Not from pool
        
        initial_available = len(pool._available)
        initial_in_use = len(pool._in_use)
        
        # Try to return callback not in use
        pool.return_callback(callback)
        
        # Pool state should be unchanged
        self.assertEqual(len(pool._available), initial_available)
        self.assertEqual(len(pool._in_use), initial_in_use)
    
    def test_pool_reuse_cycle(self):
        """Test complete CallbackPool get/return cycle for callback reuse"""
        pool = CallbackPool(initial_size=2)
        
        # Get callback
        callback1 = pool.get_callback(self.mock_future, None, self.mock_producer)
        self.assertEqual(pool._reuse_count, 1)
        
        # Return callback
        pool.return_callback(callback1)
        
        # Get another callback (should reuse the same one)
        callback2 = pool.get_callback(self.mock_future, None, self.mock_producer)
        self.assertEqual(pool._reuse_count, 2)
        
        # Should be the same callback object
        self.assertEqual(callback1, callback2)
        
        # Verify it was properly reset
        self.assertEqual(callback2.future, self.mock_future)
        self.assertEqual(callback2.producer, self.mock_producer)
    
    def test_pool_get_stats(self):
        """Test CallbackPool statistics reporting"""
        pool = CallbackPool(initial_size=10)
        
        # Initial stats
        stats = pool.get_stats()
        expected_stats = {
            'available': 10,
            'in_use': 0,
            'created_total': 10,
            'reuse_count': 0,
            'reuse_ratio': 0.0
        }
        self.assertEqual(stats, expected_stats)
        
        # Get some callbacks
        callback1 = pool.get_callback(self.mock_future, None, self.mock_producer)
        callback2 = pool.get_callback(self.mock_future, None, self.mock_producer)
        
        # Check stats after getting callbacks
        stats = pool.get_stats()
        expected_stats = {
            'available': 8,
            'in_use': 2,
            'created_total': 10,
            'reuse_count': 2,
            'reuse_ratio': 0.2  # 2/10
        }
        self.assertEqual(stats, expected_stats)
        
        # Return callbacks and get again (increase reuse)
        pool.return_callback(callback1)
        pool.return_callback(callback2)
        pool.get_callback(self.mock_future, None, self.mock_producer)
        
        # Check final stats
        stats = pool.get_stats()
        expected_stats = {
            'available': 9,
            'in_use': 1,
            'created_total': 10,
            'reuse_count': 3,
            'reuse_ratio': 0.3  # 3/10
        }
        self.assertEqual(stats, expected_stats)
    
    def test_pool_high_concurrency_simulation(self):
        """Test CallbackPool behavior under high concurrency simulation"""
        pool = CallbackPool(initial_size=5)
        
        # Simulate getting many callbacks (more than initial size)
        callbacks = []
        for i in range(20):
            callback = pool.get_callback(
                Mock(),  # Different future each time
                None,
                self.mock_producer
            )
            callbacks.append(callback)
        
        # Verify pool expanded correctly
        stats = pool.get_stats()
        self.assertEqual(stats['in_use'], 20)
        self.assertEqual(stats['available'], 0)
        self.assertEqual(stats['created_total'], 20)  # 5 initial + 15 expanded
        self.assertEqual(stats['reuse_count'], 5)  # Only initial 5 were reused
        
        # Return all callbacks
        for callback in callbacks:
            pool.return_callback(callback)
        
        # Verify all returned
        final_stats = pool.get_stats()
        self.assertEqual(final_stats['in_use'], 0)
        self.assertEqual(final_stats['available'], 20)


if __name__ == '__main__':
    # Run all tests
    unittest.main(verbosity=2)
