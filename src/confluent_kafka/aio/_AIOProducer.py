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
from confluent_kafka.aio._producer_batch_processor import ProducerBatchProcessor
from confluent_kafka.aio._callback_handler import AsyncCallbackHandler
from confluent_kafka.aio._kafka_batch_executor import KafkaBatchExecutor
from confluent_kafka.aio._buffer_timeout_manager import BufferTimeoutManager


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
        
        # Initialize callback handler for user callback execution
        self._callback_handler = AsyncCallbackHandler(self._loop)
        
        wrap_common_callbacks = _common.wrap_common_callbacks
        wrap_common_callbacks(self._loop, producer_conf)

        self._producer = confluent_kafka.Producer(producer_conf)
        
        # Batching configuration
        self._batch_size = batch_size
    
        # Producer state management
        self._is_closed = False  # Track if producer is closed
        
        # Initialize Kafka batch executor for handling Kafka operations
        self._kafka_executor = KafkaBatchExecutor(self._producer, self.executor)
        
        # Initialize batch processor for message batching and processing
        # Pool size should be larger than typical batch size to handle bursts
        pool_size = max(1000, batch_size * 2)
        self._batch_processor = ProducerBatchProcessor(self._callback_handler, self._kafka_executor, callback_pool_size=pool_size)
        
        # Initialize buffer timeout manager for timeout handling
        self._buffer_timeout_manager = BufferTimeoutManager(self._batch_processor, self._kafka_executor, buffer_timeout)
        if buffer_timeout > 0:
            self._buffer_timeout_manager.start_timeout_monitoring()

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
        self._buffer_timeout_manager.stop_timeout_monitoring()
        
        # Flush any remaining messages in the buffer
        if not self._batch_processor.is_buffer_empty():
            try:
                await self._flush_buffer()
                # Update buffer activity since we just flushed
                self._buffer_timeout_manager.mark_activity()
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
        if hasattr(self, '_buffer_timeout_manager'):
            self._buffer_timeout_manager.stop_timeout_monitoring()


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
        return await self._call(self._producer.poll, timeout, *args, **kwargs)


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
        
        self._batch_processor.add_message(msg_data, result)
        
        self._buffer_timeout_manager.mark_activity()
        
        # Check if we should flush the buffer
        if self._batch_processor.get_buffer_size() >= self._batch_size:
            await self._flush_buffer()
        
        return result


    async def flush(self, *args, **kwargs):
        """Waits until all messages are delivered or timeout
        
        This method performs a complete flush:
        1. Flushes any buffered messages from local buffer to librdkafka
        2. Waits for librdkafka to deliver/acknowledge all messages
        """
        # First, flush any remaining messages in the buffer for all topics
        if not self._batch_processor.is_buffer_empty():
            await self._flush_buffer()
            # Update buffer activity since we just flushed
            self._buffer_timeout_manager.mark_activity()
        
        # Then flush the underlying producer and wait for delivery confirmation
        return await self._call(self._producer.flush, *args, **kwargs)

    async def purge(self, *args, **kwargs):
        """Purges messages from internal queues - may block during cleanup"""
        # Clear local message buffer and futures
        self._batch_processor.clear_buffer()
        
        # Update buffer activity since we cleared the buffer
        self._buffer_timeout_manager.mark_activity()
        
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
    # BATCH PROCESSING OPERATIONS - Delegated to BatchProcessor
    # ========================================================================

    async def _flush_buffer(self, target_topic=None):
        """Flush the current message buffer using clean batch processing workflow
        
        This method demonstrates the new architecture where AIOProducer simply
        orchestrates the workflow between components:
        1. BatchProcessor creates immutable MessageBatch objects
        2. KafkaBatchExecutor executes each batch
        3. BufferTimeoutManager handles activity tracking
        """
        await self._batch_processor.flush_buffer(target_topic)

    def get_batch_processor_stats(self):
        """Get statistics from the batch processor's callback pool
        
        Returns:
            dict: Callback pool statistics for monitoring performance
        """
        return self._batch_processor.get_callback_pool_stats()
    
    def create_batches_preview(self, target_topic=None):
        """Create a preview of batches that would be created from current buffer
        
        This method demonstrates the clean separation: AIOProducer can easily
        inspect what batches would be created without executing them.
        
        Args:
            target_topic: Optional topic to preview (None for all topics)
            
        Returns:
            List[MessageBatch]: List of immutable MessageBatch objects that would be processed
        """
        return self._batch_processor.create_batches(target_topic)

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

    # ========================================================================
    # UTILITY METHODS - Helper functions and internal utilities
    # ========================================================================

    def _handle_user_callback(self, user_callback, err, msg):
        """Handle user callback execution, supporting both sync and async callbacks
        
        This method delegates to AsyncCallbackHandler for proper callback execution.
        
        Args:
            user_callback: User-provided callback function (sync or async)
            err: Error object (None if successful)
            msg: Message object
        """
        return self._callback_handler.handle_user_callback(user_callback, err, msg)

    async def _call(self, blocking_task, *args, **kwargs):
        """Helper method for blocking operations that need ThreadPool execution"""
        return await _common.async_call(self.executor, blocking_task, *args, **kwargs)


