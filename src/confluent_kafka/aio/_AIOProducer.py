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
from confluent_kafka.aio._callback_pool import CallbackPool


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
        
        # Initialize callback pool for performance optimization
        # Pool size should be larger than typical batch size to handle bursts
        pool_size = max(1000, batch_size * 2)
        self._callback_pool = CallbackPool(initial_size=pool_size)

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
                await self._flush_buffer()
                # Update buffer activity since we just flushed
                self._update_buffer_activity()
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
                        await producer._flush_buffer()
                        # Update buffer activity since we just flushed
                        producer._update_buffer_activity()
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
        
        self._update_buffer_activity()
        
        # Check if we should flush the buffer
        if len(self._message_buffer) >= self._batch_size:
            await self._flush_buffer()
        
        return result


    async def flush(self, *args, **kwargs):
        """Waits until all messages are delivered or timeout
        
        This method performs a complete flush:
        1. Flushes any buffered messages from local buffer to librdkafka
        2. Waits for librdkafka to deliver/acknowledge all messages
        """
        # First, flush any remaining messages in the buffer for all topics
        if self._message_buffer:
            await self._flush_buffer()
            # Update buffer activity since we just flushed
            self._update_buffer_activity()
        
        # Then flush the underlying producer and wait for delivery confirmation
        return await self._call(self._producer.flush, *args, **kwargs)

    async def purge(self, *args, **kwargs):
        """Purges messages from internal queues - may block during cleanup"""
        # Clear local message buffer and futures
        self._message_buffer.clear()
        self._buffer_futures.clear()
        
        # Update buffer activity since we cleared the buffer
        self._update_buffer_activity()
        
        return await self._call(self._producer.purge, *args, **kwargs)


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

    async def _flush_buffer(self, target_topic=None):
        """Flush the current message buffer using produce_batch via thread pool
        
        This method now properly handles messages for different topics by grouping
        them and calling produce_batch separately for each topic.
        """
        if not self._message_buffer:
            return
        
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
            
            # Prepare batch messages
            batch_messages = []
            for i, msg_data in enumerate(group_data['messages']):
                # Create a shallow copy and remove fields not needed by produce_batch
                batch_msg = copy.copy(msg_data)
                batch_msg.pop('user_callback', None)  # Remove user callback, we'll handle it in our callback
                batch_msg.pop('topic', None)  # Remove topic since it's passed separately
                batch_messages.append(batch_msg)
            
            # Create callbacks for each message using pool for performance
            for i, batch_msg in enumerate(batch_messages):
                # Get reusable callback from pool instead of creating new one
                future = group_data['futures'][i]
                user_callback = group_data['callbacks'][i]
                message_callback = self._callback_pool.get_callback(future, user_callback, self)
                
                # Assign the pooled callback to this message
                batch_msg['callback'] = message_callback
            
            try:
                # Call produce_batch with individual callbacks (no batch callback needed)
                await self._call(self._produce_batch_and_poll, topic, batch_messages)
                        
            except Exception as e:
                # Handle batch failure by failing all unresolved futures for this topic
                self._handle_batch_failure_individual(e, group_data['futures'], group_data['callbacks'])
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

    def _create_message_callback(self, future, user_callback):
        """Create an individual callback for a specific message that knows its future
        
        Each message gets its own callback function that:
        1. Knows exactly which future to resolve (via closure)
        2. Knows which user callback to invoke (via closure) 
        3. Can be called in any order without confusion
        
        Args:
            future: The asyncio.Future for this specific message
            user_callback: Optional user callback function for this message
            
        Returns:
            callable: Delivery callback function for this specific message
        """
        def message_delivery_callback(err, msg):
            """Individual message delivery callback - knows its exact future"""
            try:
                if err:
                    # Message delivery failed
                    if not future.done():  # Prevent double-setting
                        future.set_exception(_KafkaException(err))
                    if user_callback:
                        self._handle_user_callback(user_callback, err, msg)
                else:
                    # Message delivered successfully  
                    if not future.done():  # Prevent double-setting
                        future.set_result(msg)
                    if user_callback:
                        self._handle_user_callback(user_callback, err, msg)
                        
            except Exception as e:
                logger.error(f"Error in message delivery callback: {e}", exc_info=True)
                # Ensure future gets resolved even if there's an error in callback processing
                if not future.done():
                    future.set_exception(e)
        
        return message_delivery_callback

    def _handle_batch_failure(self, exception, batch_futures, batch_callbacks):
        """Handle batch operation failure for individual callback approach
        
        When a batch operation fails before any individual callbacks are invoked,
        we need to fail all futures for this batch since none of the per-message
        callbacks will be called by librdkafka.
        
        Args:
            exception: The exception that caused the batch to fail
            batch_futures: List of futures for this batch
            batch_callbacks: List of user callbacks for this batch
        """
        # Fail all futures since no individual callbacks will be invoked
        for i, future in enumerate(batch_futures):
            user_callback = batch_callbacks[i] if i < len(batch_callbacks) else None
            
            # Only set exception if future isn't already done
            if not future.done():
                future.set_exception(exception)
            
            # Call user callback to notify of failure
            if user_callback:
                self._handle_user_callback(user_callback, exception, None)

    def _produce_batch_and_poll(self, target_topic, batch_messages):
        """Helper method to run produce_batch with individual callbacks and poll
        
        This method uses the per-message callback approach where each message has
        its own callback that knows exactly which future to resolve. No batch-level
        callback is needed since each message is self-contained.
        
        Benefits:
        - Perfect message-to-future mapping regardless of callback order
        - No ordering assumptions or dependencies
        - Each callback knows its exact target future via closure
        - Robust against out-of-order delivery reports
        """
        # Call produce_batch with individual callbacks (no batch callback)
        self._producer.produce_batch(target_topic, batch_messages)
        
        # Immediately poll to process delivery callbacks in the same worker
        poll_result = self._producer.poll(0)
        
        return poll_result

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


