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

logger = logging.getLogger(__name__)


class AsyncCallbackHandler:
    """Handles execution of user callbacks, supporting both sync and async callbacks
    
    This class encapsulates the logic for properly executing user-provided callback
    functions in the correct execution context. It handles the complexity of:
    - Detecting whether a callback is synchronous or asynchronous
    - Scheduling async callbacks on the event loop from different thread contexts
    - Graceful handling of event loop shutdown scenarios
    
    This separation allows for:
    - Easier testing of callback execution logic
    - Reuse across different producer components
    - Clear separation of concerns
    """
    
    def __init__(self, loop: asyncio.AbstractEventLoop):
        """Initialize the callback handler
        
        Args:
            loop: The asyncio event loop to use for scheduling async callbacks
        """
        self._loop = loop
    
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
