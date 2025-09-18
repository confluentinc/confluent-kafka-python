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
import concurrent.futures
import copy
import logging
import time
import weakref

import confluent_kafka
from confluent_kafka import KafkaException as _KafkaException

import confluent_kafka.aio._common as _common


logger = logging.getLogger(__name__)


class AIOProducer:
    
    # ========================================================================
    # INITIALIZATION AND LIFECYCLE MANAGEMENT
    # ========================================================================
    
    def __init__(self, producer_conf, max_workers=4, executor=None, batch_size=1000, buffer_timeout=5.0):
        if executor is not None:
            self.executor = executor
        else:
            self.executor = concurrent.futures.ThreadPoolExecutor(
                max_workers=max_workers)
        # Store the event loop for callback handling
        self._loop = asyncio.get_running_loop()
        
        wrap_common_callbacks = _common.wrap_common_callbacks
        wrap_common_callbacks(self._loop, producer_conf)

        self._producer = confluent_kafka.Producer(producer_conf)
        
        # Batching configuration and buffer
        self._batch_size = batch_size
        self._message_buffer = []
        self._buffer_futures = []  # Track futures for each buffered message
    
        # Buffer timeout management
        self._buffer_timeout = buffer_timeout  # Timeout in seconds for buffer inactivity
        self._last_buffer_activity = time.time()  # Track last buffer activity
        self._buffer_timeout_task = None  # Background task for timeout management
        self._is_closed = False  # Track if producer is closed
        
        if buffer_timeout > 0:
        # Start the buffer timeout management task
            self._start_buffer_timeout_task()

    async def close(self):
        """Close the producer and cleanup resources
        
        This method performs a graceful shutdown sequence to ensure all resources
        are properly cleaned up and no messages are lost:
        
        1. **Signal Shutdown**: Sets the closed flag to signal the timeout task to stop
        2. **Cancel Timeout Task**: Immediately cancels the buffer timeout monitoring task
        3. **Flush Remaining Messages**: Flushes any buffered messages to ensure delivery
        4. **Shutdown ThreadPool**: Waits for all pending ThreadPool operations to complete
        5. **Cleanup**: Ensures the underlying librdkafka producer is properly closed. The shutdown 
            is designed to be safe and non-blocking for the asyncio event loop
            while ensuring all pending operations complete before the producer is closed.
        
        Raises:
            Exception: May raise exceptions from buffer flushing, but these are logged
                      and don't prevent the cleanup process from completing.
        """
        # Set closed flag to signal timeout task to stop
        self._is_closed = True
        
        # Stop the buffer timeout monitoring task
        self._stop_buffer_timeout_task()
        
        # Flush any remaining messages in the buffer
        if self._message_buffer:
            try:
                await self.flush_buffer()
            except Exception:
                logger.error("Error flushing buffer", exc_info=True)
                # Don't let flush errors prevent cleanup
                pass
        
        # Shutdown the ThreadPool executor and wait for any remaining tasks to complete
        # This ensures that all pending poll(), flush(), and other blocking operations
        # finish before the producer is considered fully closed
        if hasattr(self, 'executor'):
            # executor.shutdown(wait=True) is a blocking call that:
            # - Prevents new tasks from being submitted to the ThreadPool
            # - Waits for all currently executing and queued tasks to complete
            # - Returns only when all worker threads have finished
            # 
            # We run this in a separate thread (using None as executor) to avoid
            # blocking the asyncio event loop during the potentially long shutdown wait
            await asyncio.get_running_loop().run_in_executor(
                None, self.executor.shutdown, True
            )

    def __del__(self):
        """Cleanup method called during garbage collection
        
        This ensures that the timeout task is properly cancelled even if
        close() wasn't explicitly called.
        """
        if hasattr(self, '_is_closed'):
            self._is_closed = True
        if hasattr(self, '_buffer_timeout_task') and self._buffer_timeout_task:
            if not self._buffer_timeout_task.done():
                self._buffer_timeout_task.cancel()

    # ========================================================================
    # BUFFER TIMEOUT MANAGEMENT - Prevent messages from being held indefinitely
    # ========================================================================
    
    def _start_buffer_timeout_task(self):
        """Start the background task that monitors buffer inactivity and flushes stale messages
        
        This method creates an async task that runs in the background and periodically checks
        if messages have been sitting in the buffer for too long without being flushed.
        
        Key design decisions:
        1. **Weak Reference**: Uses weakref.ref(self) to prevent circular references that would
           prevent garbage collection of the AIOProducer instance.
        2. **Self-Canceling**: The task checks if the producer still exists and stops itself
           if the producer has been garbage collected.
        3. **Adaptive Check Interval**: Uses self._buffer_timeout to determine both the timeout
           threshold and the check frequency (checks every buffer_timeout/2, bounded by 0.1-1.0s).
        """
        async def timeout_monitor():
            # Use weak reference to avoid circular reference and allow garbage collection
            producer_ref = weakref.ref(self)
            
            while True:
                # Check interval should be proportional to buffer timeout for efficiency
                # Use half the buffer timeout, with reasonable min/max bounds
                producer = producer_ref()
                if producer is None:
                    break
                
                # Calculate adaptive check interval: buffer_timeout/2 with bounds
                # - Base: buffer_timeout/2 (check twice per timeout period)  
                # - Lower bound: 0.1s (prevent excessive CPU usage for tiny timeouts)
                # - Upper bound: 1.0s (ensure responsiveness for large timeouts)
                # Examples: 0.1s→0.1s, 1s→0.5s, 5s→1.0s, 30s→1.0s
                check_interval = max(0.1, min(1.0, producer._buffer_timeout / 2))
                await asyncio.sleep(check_interval)
                
                # Re-check producer after sleep (it might have been closed/garbage collected)
                producer = producer_ref()
                if producer is None or producer._is_closed:
                    # Producer has been garbage collected or closed, stop the task
                    break
                
                # Check if buffer has been inactive for too long
                time_since_activity = time.time() - producer._last_buffer_activity
                if (time_since_activity >= producer._buffer_timeout and 
                    producer._message_buffer):
                    
                    try:
                        # Flush the buffer due to timeout
                        await producer.flush_buffer()
                    except Exception:
                        logger.error("Error flushing buffer", exc_info=True)
                        # Don't let buffer flush errors crash the timeout task
                        pass
        
        # Create and store the timeout task
        self._buffer_timeout_task = asyncio.create_task(timeout_monitor())
    
    def _update_buffer_activity(self):
        """Update the timestamp of the last buffer activity
        
        This method should be called whenever:
        1. Messages are added to the buffer (in produce())
        2. Buffer is manually flushed 
        3. Buffer is purged/cleared
        
        It helps the timeout task know when the buffer was last active.
        """
        self._last_buffer_activity = time.time()
    
    def _stop_buffer_timeout_task(self):
        """Stop and cleanup the buffer timeout monitoring task"""
        if self._buffer_timeout_task and not self._buffer_timeout_task.done():
            self._buffer_timeout_task.cancel()
            self._buffer_timeout_task = None

    # ========================================================================
    # CORE PRODUCER OPERATIONS - Main public API
    # ========================================================================

    async def poll(self, timeout=0, *args, **kwargs):
        """Processes delivery callbacks from librdkafka - blocking behavior depends on timeout
        
        This method triggers any pending delivery callbacks (on_delivery) that have been
        queued by librdkafka when messages are delivered or fail to deliver.
        
        Args:
            timeout: Timeout in seconds for waiting for callbacks:
                    - 0 = non-blocking, return immediately after processing available callbacks
                    - >0 = block up to timeout seconds waiting for new callbacks to arrive
                    - -1 = block indefinitely until callbacks are available
        
        Returns:
            Number of callbacks processed during this call
        """
        if timeout > 0 or timeout == -1:
            # Blocking call - use ThreadPool to avoid blocking event loop
            return await self._call(self._producer.poll, timeout, *args, **kwargs)
        else:
            # Non-blocking call (timeout=0) - can run directly
            return await self._call(self._producer.poll, 0, *args, **kwargs)


    async def produce(self, topic, value=None, key=None, *args, **kwargs):
        """Batched produce: Accumulates messages in buffer and flushes when threshold reached
        
        Args:
            topic: Kafka topic name (required)
            value: Message payload (optional)
            key: Message key (optional)
            *args, **kwargs: Additional parameters like partition, timestamp, headers
        """
        result = asyncio.get_running_loop().create_future()
        user_callback = kwargs.get('on_delivery')
        
        msg_data = {
            'topic': topic,
            'value': value,
            'key': key
        }
        
        # Add optional parameters to message data
        if 'partition' in kwargs:
            msg_data['partition'] = kwargs['partition']
        if 'timestamp' in kwargs:
            msg_data['timestamp'] = kwargs['timestamp']
        if 'headers' in kwargs:
            msg_data['headers'] = kwargs['headers']
        
        # Store user callback in message data for later execution
        if user_callback:
            msg_data['user_callback'] = user_callback
        
        self._message_buffer.append(msg_data)
        self._buffer_futures.append(result)
        
        # Update buffer activity timestamp since we added a message
        self._update_buffer_activity()
        
        should_flush = len(self._message_buffer) >= self._batch_size
        
        if should_flush:
            await self._flush_buffer()
        
        return result

    async def flush(self, *args, **kwargs):
        """Waits until all messages are delivered or timeout"""
        # First, flush any remaining messages in the buffer for all topics
        if self._message_buffer:
            # Flush all topics - the _flush_buffer method now properly handles multiple topics
            await self._flush_buffer()
        
        # Then flush the producer
        return await self._call(self._producer.flush, *args, **kwargs)

    async def purge(self, *args, **kwargs):
        """Purges messages from internal queues - may block during cleanup"""
        # Clear local message buffer and futures
        self._message_buffer.clear()
        self._buffer_futures.clear()
        
        # Update buffer activity since we cleared the buffer
        self._update_buffer_activity()
        
        return await self._call(self._producer.purge, *args, **kwargs)

    async def flush_buffer(self):
        """Manually flush the current message buffer for all topics
        
        This is the public API method for manual buffer flushing.
        It always flushes all topics and updates buffer activity.
        """
        if self._message_buffer:
            # Flush all topics (don't specify target_topic)
            await self._flush_buffer()
            # Update buffer activity since we just flushed
            self._update_buffer_activity()

    # ========================================================================
    # TRANSACTION OPERATIONS - Kafka transaction support
    # ========================================================================

    async def init_transactions(self, *args, **kwargs):
        """Network call to initialize transactions"""
        return await self._call(self._producer.init_transactions,
                                *args, **kwargs)

    async def begin_transaction(self, *args, **kwargs):
        """Network call to begin transaction"""
        return await self._call(self._producer.begin_transaction,
                                *args, **kwargs)

    async def send_offsets_to_transaction(self, *args, **kwargs):
        """Network call to send offsets to transaction"""
        return await self._call(self._producer.send_offsets_to_transaction,
                                *args, **kwargs)

    async def commit_transaction(self, *args, **kwargs):
        """Network call to commit transaction"""
        return await self._call(self._producer.commit_transaction,
                                *args, **kwargs)

    async def abort_transaction(self, *args, **kwargs):
        """Network call to abort transaction"""
        return await self._call(self._producer.abort_transaction,
                                *args, **kwargs)

    # ========================================================================
    # AUTHENTICATION AND SECURITY
    # ========================================================================

    async def set_sasl_credentials(self, *args, **kwargs):
        """Authentication operation that may involve network calls"""
        return await self._call(self._producer.set_sasl_credentials,
                                *args, **kwargs)
      
    # ========================================================================
    # BATCH PROCESSING OPERATIONS - Internal batching implementation
    # ========================================================================

    def _produce_batch_and_poll(self, target_topic, batch_messages, batch_delivery_callback):
        """Helper method to run produce_batch and poll in the same thread pool worker
        
        This optimization combines both operations in a single thread pool execution:
        1. produce_batch() - queues messages in librdkafka
        2. poll(0) - immediately processes delivery callbacks
        
        Benefits:
        - Reduces thread pool overhead (1 call instead of 2)
        - Eliminates context switching between operations
        - Ensures immediate callback processing
        - Lower latency for delivery confirmation
        """
        # Call produce_batch first
        self._producer.produce_batch(target_topic, batch_messages, on_delivery=batch_delivery_callback)
        
        # Immediately poll to process delivery callbacks in the same worker
        poll_result = self._producer.poll(0)
        
        return poll_result

    async def _flush_buffer(self, target_topic=None):
        """Flush the current message buffer using produce_batch via thread pool
        
        This method now properly handles messages for different topics by grouping
        them and calling produce_batch separately for each topic.
        """
        if not self._message_buffer:
            return
        
        # Group messages by topic for batch processing
        topic_groups = self._group_messages_by_topic()
        
        # Determine which topics to process and which to keep in buffer
        topics_to_process = []
        messages_to_keep = []
        futures_to_keep = []
        
        for topic, group_data in topic_groups.items():
            if target_topic is None or topic == target_topic:
                # This topic should be flushed
                topics_to_process.append((topic, group_data))
            else:
                # Keep messages for non-target topics in buffer
                messages_to_keep.extend(group_data['messages'])
                futures_to_keep.extend(group_data['futures'])
        
        # Update buffers: clear all, then add back what should be kept
        self._message_buffer.clear()
        self._buffer_futures.clear()
        self._message_buffer.extend(messages_to_keep)
        self._buffer_futures.extend(futures_to_keep)
        
        # Process each selected topic group
        for topic, group_data in topics_to_process:
            
            # Prepare batch messages for librdkafka (remove fields not needed by produce_batch)
            batch_messages = []
            for msg_data in group_data['messages']:
                # Create a shallow copy and remove the user_callback and topic fields
                batch_msg = copy.copy(msg_data)
                batch_msg.pop('user_callback', None)  # Remove callback, keep everything else
                batch_msg.pop('topic', None)  # Remove topic since it's passed separately
                batch_messages.append(batch_msg)
            
            # Create delivery callback that handles both futures and user callbacks
            # Futures: Used for async/await pattern - each produce() call returns a future
            # User callbacks: Optional user-provided functions called on delivery/error
            batch_delivery_callback, callback_state = self._create_batch_callback(
                group_data['futures'], 
                group_data['callbacks']
            )
            
            try:
                # Call produce_batch and poll for this topic
                await self._call(self._produce_batch_and_poll, topic, batch_messages, batch_delivery_callback)
                        
            except Exception as e:
                # Handle batch failure by failing all remaining futures for this topic
                self._handle_batch_failure(e, group_data['futures'], group_data['callbacks'], callback_state)
                # Re-raise the exception so caller knows the batch operation failed
                raise

    def _group_messages_by_topic(self):
        """Group buffered messages by topic for batch processing
        
        This function efficiently organizes the mixed-topic message buffer into
        topic-specific groups, since librdkafka's produce_batch requires separate
        calls for each topic.
        
        Algorithm:
        - Single O(n) pass through message buffer
        - Groups related data (messages, futures, callbacks) by topic
        - Maintains index relationships between buffer arrays
        
        Returns:
            dict: Topic groups with structure:
                {
                    'topic_name': {
                        'messages': [msg_data1, msg_data2, ...],     # Message dictionaries
                        'futures': [future1, future2, ...],         # Corresponding asyncio.Future objects  
                        'callbacks': [callback1, callback2, ...],   # User delivery callbacks (optional)
                    }
                }
        """
        topic_groups = {}
        
        # Iterate through buffer once - O(n) complexity
        for i, msg_data in enumerate(self._message_buffer):
            topic = msg_data['topic']
            
            # Create new topic group if this is first message for this topic
            if topic not in topic_groups:
                topic_groups[topic] = {
                    'messages': [],    # Message data for produce_batch
                    'futures': [],     # Futures to resolve on delivery
                    'callbacks': [],   # User callbacks to invoke on delivery  
                }
            
            # Add message and related data to appropriate topic group
            # Note: All arrays stay synchronized by index
            topic_groups[topic]['messages'].append(msg_data)
            topic_groups[topic]['futures'].append(self._buffer_futures[i])
            topic_groups[topic]['callbacks'].append(msg_data.get('user_callback'))
            
        return topic_groups

    def _create_batch_callback(self, batch_futures, batch_user_callbacks):
        """Create a stateful delivery callback for batch processing
        
        This function creates a closure that serves as the delivery callback for
        librdkafka's produce_batch operation. The callback is called once for each
        message in the batch when it's delivered (or fails to deliver).
        
        Why this function is needed:
        1. **Stateful Processing**: librdkafka calls the callback sequentially for each
           message, but doesn't provide message index. We need to track which message
           is being processed using an internal counter.
           
        2. **Dual Callback Handling**: Each message has two callbacks to handle:
           - asyncio.Future: For async/await pattern (always present) - allows callers to await delivery
           - User callback: Optional user-provided function (sync or async) - custom delivery notification
           
        3. **Thread Safety**: The callback runs in librdkafka's C thread, so we need
           to safely bridge to Python's asyncio event loop for async user callbacks.
           
        4. **Error Propagation**: Both success and failure need to be propagated to
           both the future and user callback with appropriate error wrapping.
        
        Callback Execution Flow:
        1. librdkafka delivers message 0 → callback called with (err, msg)
        2. callback processes index 0, increments counter to 1
        3. librdkafka delivers message 1 → callback called with (err, msg)  
        4. callback processes index 1, increments counter to 2
        5. ... continues for all messages in batch
        
        Args:
            batch_futures: List of asyncio.Future objects to resolve/reject
            batch_user_callbacks: List of user callback functions (can be None)
            
        Returns:
            tuple: (callback_function, callback_state) where:
                - callback_function: Stateful delivery callback function
                - callback_state: Dictionary with callback state for error handling
        """
        callback_state = {'index': 0, 'futures': batch_futures, 'callbacks': batch_user_callbacks}
        
        def batch_delivery_callback(err, msg):
            """librdkafka delivery callback - called once per message in batch"""
            idx = callback_state['index']
            if idx < len(callback_state['futures']):
                future = callback_state['futures'][idx]
                user_callback = callback_state['callbacks'][idx]
                
                if err:
                    # Message delivery failed
                    future.set_exception(_KafkaException(err))
                    if user_callback:
                        self._handle_user_callback(user_callback, err, msg)
                else:
                    # Message delivered successfully
                    future.set_result(msg)
                    if user_callback:
                        self._handle_user_callback(user_callback, err, msg)
            
            # Move to next message in batch
            callback_state['index'] += 1
        
        return batch_delivery_callback, callback_state

    def _handle_batch_failure(self, exception, batch_futures, batch_callbacks, callback_state):
        """Handle batch operation failure by failing all remaining futures
        
        When a batch operation fails, we need to:
        1. Determine which futures haven't been processed yet
        2. Fail those futures with the batch exception
        3. Call user callbacks for failed messages
        
        Args:
            exception: The exception that caused the batch to fail
            batch_futures: List of futures for this batch
            batch_callbacks: List of user callbacks for this batch  
            callback_state: Dictionary containing callback state with current index
        """
        # Get the current index to determine which messages were already processed
        start_idx = callback_state.get('index', 0)
        
        # Fail all futures that haven't been processed yet
        for i in range(start_idx, len(batch_futures)):
            future = batch_futures[i]
            user_callback = batch_callbacks[i] if i < len(batch_callbacks) else None
            
            # Only set exception if future isn't already done
            if not future.done():
                future.set_exception(exception)
            
            # Call user callback to notify of failure
            if user_callback:
                self._handle_user_callback(user_callback, exception, None)

    # ========================================================================
    # UTILITY METHODS - Helper functions and internal utilities
    # ========================================================================

    def _handle_user_callback(self, user_callback, err, msg):
        """Handle user callback execution, supporting both sync and async callbacks
        
        This method is called from librdkafka's C thread context and needs to properly
        handle both synchronous and asynchronous user callbacks.
        
        Args:
            user_callback: User-provided callback function (sync or async)
            err: Error object (None if successful)
            msg: Message object
        """
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
                pass
        else:
            # Sync callback - call directly
            user_callback(err, msg)

    async def _call(self, blocking_task, *args, **kwargs):
        """Helper method for blocking operations that need ThreadPool execution"""
        return await _common.async_call(self.executor, blocking_task, *args, **kwargs)


