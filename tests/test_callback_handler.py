#!/usr/bin/env python3
"""
Unit tests for the AsyncCallbackHandler class (_callback_handler.py)

This module tests the AsyncCallbackHandler class to ensure proper
callback execution for both sync and async user callbacks.
"""

import asyncio
import unittest
from unittest.mock import Mock, patch
import sys
import os

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from confluent_kafka.aio.producer._callback_handler import AsyncCallbackHandler


class TestAsyncCallbackHandler(unittest.TestCase):
    """Test cases for AsyncCallbackHandler class"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        self.handler = AsyncCallbackHandler(self.loop)
    
    def tearDown(self):
        """Clean up test fixtures"""
        self.loop.close()
    
    def test_handle_sync_callback(self):
        """Test handling of synchronous user callback"""
        # Create a mock sync callback
        sync_callback = Mock()
        
        # Call the handler
        self.handler.handle_user_callback(sync_callback, None, "test_msg")
        
        # Verify the callback was called directly
        sync_callback.assert_called_once_with(None, "test_msg")
    
    def test_handle_async_callback(self):
        """Test handling of asynchronous user callback"""
        # Create a mock async callback
        async def async_callback(err, msg):
            pass
        
        # Mock the loop's call_soon_threadsafe method
        with patch.object(self.loop, 'call_soon_threadsafe') as mock_call:
            self.handler.handle_user_callback(async_callback, None, "test_msg")
            
            # Verify call_soon_threadsafe was called
            mock_call.assert_called_once()
    
    def test_handle_none_callback(self):
        """Test handling when callback is None"""
        # Should not raise any exceptions
        self.handler.handle_user_callback(None, None, "test_msg")
        
        # Should handle gracefully
        self.handler.handle_user_callback(None, Exception("test"), "test_msg")
    
    def test_sync_callback_exception_handling(self):
        """Test that exceptions in sync callbacks are handled gracefully"""
        def failing_callback(err, msg):
            raise Exception("Test exception")
        
        # Should not raise exception
        try:
            self.handler.handle_user_callback(failing_callback, None, "test_msg")
        except Exception as e:
            self.fail(f"AsyncCallbackHandler should handle callback exceptions: {e}")
    
    def test_event_loop_closed_handling(self):
        """Test handling when event loop is closed"""
        async def async_callback(err, msg):
            pass
        
        # Mock call_soon_threadsafe to raise RuntimeError (event loop closed)
        with patch.object(self.loop, 'call_soon_threadsafe', side_effect=RuntimeError("Event loop is closed")):
            # Should not raise exception
            try:
                self.handler.handle_user_callback(async_callback, None, "test_msg")
            except Exception as e:
                self.fail(f"AsyncCallbackHandler should handle closed event loop gracefully: {e}")


if __name__ == '__main__':
    unittest.main()
