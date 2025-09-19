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
        self.callback_handler = None  # Reference to callback handler for user callback execution
    
    def reset(self, future, user_callback, pool, callback_handler):
        """Reset this callback for a new message"""
        self.future = future
        self.user_callback = user_callback
        self.pool = pool
        self.callback_handler = callback_handler
    
    def __call__(self, err, msg):
        """Handle the delivery callback"""
        try:
            if err:
                # Message delivery failed
                if not self.future.done():  # Prevent double-setting
                    self.future.set_exception(_KafkaException(err))
                if self.user_callback and self.callback_handler:
                    self.callback_handler.handle_user_callback(self.user_callback, err, msg)
            else:
                # Message delivered successfully  
                if not self.future.done():  # Prevent double-setting
                    self.future.set_result(msg)
                if self.user_callback and self.callback_handler:
                    self.callback_handler.handle_user_callback(self.user_callback, err, msg)
                        
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
        callback_handler = self.callback_handler
        self.callback_handler = None
        
        # Return to pool
        if self.pool:
            self.pool.return_callback(self)


class CallbackPool:
    """Pool of reusable callback objects for performance optimization
    
    This pool maintains a collection of ReusableMessageCallback objects that can
    be reused instead of creating new callback closures for every message.
    
    Benefits:
    - Reduces object allocation overhead
    - Minimizes garbage collection pressure  
    - Improves overall throughput
    """
    
    def __init__(self, callback_handler, initial_size=1000):
        self._callback_handler = callback_handler
        self._available = []
        self._in_use = set()
        self._created_count = 0
        self._reuse_count = 0
        
        # Pre-create callback objects
        for _ in range(initial_size):
            self._available.append(ReusableMessageCallback())
            self._created_count += 1
    
    def get_callback(self, future, user_callback):
        """Get a callback from the pool"""
        if self._available:
            # Reuse existing callback
            callback = self._available.pop()
            self._reuse_count += 1
        else:
            # Create new one if pool is empty (pool expansion)
            callback = ReusableMessageCallback()
            self._created_count += 1
        
        callback.reset(future, user_callback, self, self._callback_handler)
        self._in_use.add(callback)
        return callback
    
    def return_callback(self, callback):
        """Return a callback to the pool"""
        if callback in self._in_use:
            self._in_use.remove(callback)
            self._available.append(callback)
    
    def get_stats(self):
        """Get pool statistics for monitoring"""
        return {
            'available': len(self._available),
            'in_use': len(self._in_use),
            'created_total': self._created_count,
            'reuse_count': self._reuse_count,
            'reuse_ratio': self._reuse_count / max(1, self._created_count)
        }
