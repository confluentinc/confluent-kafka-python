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

import copy
import logging

from confluent_kafka.aio._callback_pool import CallbackPool

logger = logging.getLogger(__name__)


class ProducerBatchProcessor:
    """Handles batching and processing of Kafka messages for AIOProducer
    
    This class encapsulates all the logic for:
    - Grouping messages by topic
    - Managing message buffers and futures
    - Creating and managing callbacks
    - Executing batch operations via librdkafka
    
    Benefits of separation:
    - Single responsibility principle
    - Easier testing and maintenance
    - Cleaner AIOProducer code
    - Reusable batch processing logic
    """
    
    def __init__(self, callback_pool_size=1000):
        """Initialize the batch processor
        
        Args:
            callback_pool_size: Initial size for the callback pool
        """
        self._callback_pool = CallbackPool(initial_size=callback_pool_size)
        self._message_buffer = []
        self._buffer_futures = []
    
    def add_message(self, msg_data, future):
        """Add a message to the batch buffer
        
        Args:
            msg_data: Dictionary containing message data
            future: asyncio.Future to resolve when message is delivered
        """
        self._message_buffer.append(msg_data)
        self._buffer_futures.append(future)
    
    def get_buffer_size(self):
        """Get the current number of messages in the buffer"""
        return len(self._message_buffer)
    
    def is_buffer_empty(self):
        """Check if the buffer is empty"""
        return len(self._message_buffer) == 0
    
    def clear_buffer(self):
        """Clear the entire buffer"""
        self._message_buffer.clear()
        self._buffer_futures.clear()
    
    async def flush_buffer(self, aio_producer, target_topic=None):
        """Flush the current message buffer using produce_batch
        
        Args:
            aio_producer: The AIOProducer instance
            target_topic: Optional topic to flush (None for all topics)
            
        Returns:
            None
        """
        if self.is_buffer_empty():
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
            batch_messages = self._prepare_batch_messages(group_data['messages'])
            
            # Create callbacks for each message using pool for performance
            self._assign_callbacks_to_messages(
                batch_messages, 
                group_data['futures'], 
                group_data['callbacks'],
                aio_producer  # Pass AIOProducer instance for _handle_user_callback
            )
            
            try:
                # Call produce_batch with individual callbacks
                await self._execute_batch(aio_producer.executor, aio_producer._producer, topic, batch_messages)
                        
            except Exception as e:
                # Handle batch failure by failing all unresolved futures for this topic
                self._handle_batch_failure(e, group_data['futures'], group_data['callbacks'], aio_producer)
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
    
    def _prepare_batch_messages(self, messages):
        """Prepare messages for produce_batch by removing internal fields
        
        Args:
            messages: List of message dictionaries
            
        Returns:
            List of cleaned message dictionaries ready for produce_batch
        """
        batch_messages = []
        for msg_data in messages:
            # Create a shallow copy and remove fields not needed by produce_batch
            batch_msg = copy.copy(msg_data)
            batch_msg.pop('user_callback', None)  # Remove user callback, we'll handle it in our callback
            batch_msg.pop('topic', None)  # Remove topic since it's passed separately
            batch_messages.append(batch_msg)
        
        return batch_messages
    
    def _assign_callbacks_to_messages(self, batch_messages, futures, user_callbacks, aio_producer):
        """Assign individual callbacks to each message in the batch
        
        Args:
            batch_messages: List of message dictionaries for produce_batch
            futures: List of asyncio.Future objects to resolve
            user_callbacks: List of user callback functions (can be None)
            aio_producer: AIOProducer instance for _handle_user_callback method
        """
        for i, batch_msg in enumerate(batch_messages):
            # Get reusable callback from pool instead of creating new one
            future = futures[i]
            user_callback = user_callbacks[i]
            message_callback = self._callback_pool.get_callback(future, user_callback, aio_producer)
            
            # Assign the pooled callback to this message
            batch_msg['callback'] = message_callback
    
    async def _execute_batch(self, executor, producer, topic, batch_messages):
        """Execute the batch operation via thread pool
        
        Args:
            executor: ThreadPoolExecutor for running blocking operations
            producer: confluent_kafka.Producer instance
            topic: Target topic for the batch
            batch_messages: List of prepared messages with callbacks
        """
        import asyncio
        
        def _produce_batch_and_poll():
            """Helper function to run in thread pool"""
            # Call produce_batch with individual callbacks (no batch callback)
            producer.produce_batch(topic, batch_messages)
            
            # Immediately poll to process delivery callbacks in the same worker
            poll_result = producer.poll(0)
            
            return poll_result
        
        # Execute in thread pool to avoid blocking event loop
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(executor, _produce_batch_and_poll)
    
    def _handle_batch_failure(self, exception, batch_futures, batch_callbacks, aio_producer):
        """Handle batch operation failure by failing all unresolved futures
        
        When a batch operation fails before any individual callbacks are invoked,
        we need to fail all futures for this batch since none of the per-message
        callbacks will be called by librdkafka.
        
        Args:
            exception: The exception that caused the batch to fail
            batch_futures: List of futures for this batch
            batch_callbacks: List of user callbacks for this batch
            aio_producer: AIOProducer instance for _handle_user_callback method
        """
        # Fail all futures since no individual callbacks will be invoked
        for i, future in enumerate(batch_futures):
            user_callback = batch_callbacks[i] if i < len(batch_callbacks) else None
            
            # Only set exception if future isn't already done
            if not future.done():
                future.set_exception(exception)
            
            # Call user callback to notify of failure
            if user_callback:
                aio_producer._handle_user_callback(user_callback, exception, None)
    
    def get_callback_pool_stats(self):
        """Get statistics about the callback pool
        
        Returns:
            dict: Pool statistics including reuse ratios and counts
        """
        return self._callback_pool.get_stats()
