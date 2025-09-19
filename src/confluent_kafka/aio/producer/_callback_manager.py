# Copyright 2025 Confluent Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import asyncio
import logging

from confluent_kafka import KafkaException as _KafkaException

logger = logging.getLogger(__name__)


class ReusableMessageCallback:
    """A callback object that can be reset and reused for different messages
    
    This class implements callback pooling to reduce object creation overhead.
    Instead of creating new callback closures for every message, we reuse 
    callback objects by resetting their state.
    """
    
    def __init__(self):
        self.future = None
        self.user_callback = None
        self.pool = None  # Reference to pool for auto-return
    
    def reset(self, future, user_callback, pool):
        """Reset this callback for a new message"""
        self.future = future
        self.user_callback = user_callback
        self.pool = pool
    
    def __call__(self, err, msg):
        """Handle the delivery callback"""
        try:
            if err:
                # Message delivery failed
                if not self.future.done():  # Prevent double-setting
                    self.future.set_exception(_KafkaException(err))
                if self.user_callback and self.pool:
                    self.pool.handle_user_callback(self.user_callback, err, msg)
            else:
                # Message delivered successfully  
                if not self.future.done():  # Prevent double-setting
                    self.future.set_result(msg)
                if self.user_callback and self.pool:
                    self.pool.handle_user_callback(self.user_callback, err, msg)
                        
        except Exception as e:
            logger.error(f"Error in reusable message delivery callback: {e}", exc_info=True)
            # Ensure future gets resolved even if there's an error in callback processing
            if not self.future.done():
                self.future.set_exception(e)
        finally:
            # Auto-return to pool after use
            self._clear_and_return()
    
    def _clear_and_return(self):
        """Clear references and return to pool"""
        # Clear references to prevent memory leaks
        future = self.future  # Keep reference for logging
        self.future = None
        self.user_callback = None
        
        # Return to pool
        if self.pool:
            self.pool.return_callback(self)


class CallbackManager:
    """Unified callback management with pooling and execution handling
    
    This class is responsible for:
    - Handling both synchronous and asynchronous user callbacks
    - Properly scheduling async callbacks on the event loop
    - Managing thread-safe callback execution from librdkafka
    - Maintaining a pool of reusable callback objects
    - Reducing object allocation overhead
    - Minimizing garbage collection pressure
    
    """
    
    def __init__(self, loop: asyncio.AbstractEventLoop, initial_pool_size=1000):
        """Initialize the unified callback manager
        
        Args:
            loop: The asyncio event loop to use for scheduling async callbacks
            initial_pool_size: Initial size for the callback pool
        """
        # Callback execution components
        self._loop = loop
        
        # Callback pooling components
        self._available = []
        self._in_use = set()
        self._created_count = 0
        self._reuse_count = 0
        
        # Pre-create callback objects
        for _ in range(initial_pool_size):
            self._available.append(ReusableMessageCallback())
            self._created_count += 1
    
    def handle_user_callback(self, user_callback, err, msg):
        """Handle user callback execution, supporting both sync and async callbacks
        
        This method is called from librdkafka's C thread context and needs to properly
        handle both synchronous and asynchronous user callbacks.
        
        Args:
            user_callback: User-provided callback function (sync or async)
            err: Error object (None if successful)
            msg: Message object
        """
        if not user_callback:
            return
            
        if asyncio.iscoroutinefunction(user_callback):
            # Async callback - schedule it to run on the event loop
            # We're in librdkafka's C thread, so we need to schedule this safely
            try:
                # Schedule the async callback to run on the event loop
                self._loop.call_soon_threadsafe(
                    lambda: asyncio.create_task(user_callback(err, msg))
                )
            except RuntimeError:
                # Event loop might be closed - handle gracefully
                logger.warning("Event loop closed, cannot schedule async callback")
                pass
        else:
            # Sync callback - call directly
            try:
                user_callback(err, msg)
            except Exception as e:
                logger.error(f"Error in sync user callback: {e}", exc_info=True)
    
    def get_callback(self, future, user_callback):
        """Get a reusable callback from the pool
        
        Args:
            future: asyncio.Future to resolve when message is delivered
            user_callback: Optional user callback function for this message
            
        Returns:
            ReusableMessageCallback: Configured callback ready for use
        """
        if self._available:
            # Reuse existing callback
            callback = self._available.pop()
            self._reuse_count += 1
        else:
            # Create new one if pool is empty (pool expansion)
            callback = ReusableMessageCallback()
            self._created_count += 1
        
        callback.reset(future, user_callback, self)
        self._in_use.add(callback)
        return callback
    
    def return_callback(self, callback):
        """Return a callback to the pool for reuse
        
        Args:
            callback: ReusableMessageCallback to return to the pool
        """
        if callback in self._in_use:
            self._in_use.remove(callback)
            self._available.append(callback)
    
    def get_stats(self):
        """Get pool statistics for monitoring
        
        Returns:
            dict: Pool statistics including reuse ratios and counts
        """
        return {
            'available': len(self._available),
            'in_use': len(self._in_use),
            'created_total': self._created_count,
            'reuse_count': self._reuse_count,
            'reuse_ratio': self._reuse_count / max(1, self._created_count)
        }
