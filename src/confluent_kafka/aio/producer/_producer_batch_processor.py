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

from confluent_kafka import KafkaException as _KafkaException
from confluent_kafka.aio.producer._message_batch import create_message_batch

logger = logging.getLogger(__name__)


class ProducerBatchManager:
    """Handles batching and processing of Kafka messages for AIOProducer

    This class encapsulates all the logic for:
    - Grouping messages by topic
    - Managing message buffers and futures
    - Creating simple future-resolving callbacks
    - Executing batch operations via librdkafka
    """

    def __init__(self, kafka_executor):
        """Initialize the batch processor

        Args:
            kafka_executor: KafkaBatchExecutor instance for Kafka operations
        """
        self._kafka_executor = kafka_executor
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

    def create_batches(self, target_topic=None):
        """Create MessageBatch objects from the current buffer

        Args:
            target_topic: Optional topic to create batches for (None for all topics)

        Returns:
            List[MessageBatch]: List of immutable MessageBatch objects
        """
        if self.is_buffer_empty():
            return []

        topic_groups = self._group_messages_by_topic()
        batches = []

        for topic, group_data in topic_groups.items():
            if target_topic is None or topic == target_topic:
                # Prepare batch messages
                batch_messages = self._prepare_batch_messages(group_data['messages'])

                # Assign simple future-resolving callbacks to messages
                self._assign_future_callbacks(batch_messages, group_data['futures'])

                # Create immutable MessageBatch object
                batch = create_message_batch(
                    topic=topic,
                    messages=batch_messages,
                    futures=group_data['futures'],
                    callbacks=None  # No user callbacks anymore
                )
                batches.append(batch)

        return batches

    def _clear_topic_from_buffer(self, target_topic):
        """Remove messages for a specific topic from the buffer

        Args:
            target_topic: Topic to remove from buffer
        """
        messages_to_keep = []
        futures_to_keep = []

        for i, msg_data in enumerate(self._message_buffer):
            if msg_data['topic'] != target_topic:
                messages_to_keep.append(msg_data)
                futures_to_keep.append(self._buffer_futures[i])

        self._message_buffer = messages_to_keep
        self._buffer_futures = futures_to_keep

    async def flush_buffer(self, target_topic=None):
        """Flush the current message buffer using produce_batch

        Args:
            target_topic: Optional topic to flush (None for all topics)

        Returns:
            None
        """
        if self.is_buffer_empty():
            return

        # Create batches for processing
        batches = self.create_batches(target_topic)

        # Clear processed messages from buffer
        if target_topic is None:
            # Clear entire buffer
            self.clear_buffer()
        else:
            # Clear only messages for the target topic
            self._clear_topic_from_buffer(target_topic)

        # Execute each batch
        for batch in batches:
            try:
                # Execute batch using the Kafka executor
                await self._kafka_executor.execute_batch(batch.topic, batch.messages)

            except Exception as e:
                # Handle batch failure by failing all unresolved futures for this batch
                self._handle_batch_failure(e, batch.futures)
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
                }

            # Add message and related data to appropriate topic group
            # Note: All arrays stay synchronized by index
            topic_groups[topic]['messages'].append(msg_data)
            topic_groups[topic]['futures'].append(self._buffer_futures[i])

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
            batch_msg.pop('topic', None)  # Remove topic since it's passed separately
            batch_messages.append(batch_msg)

        return batch_messages

    def _assign_future_callbacks(self, batch_messages, futures):
        """Assign simple future-resolving callbacks to each message in the batch

        Args:
            batch_messages: List of message dictionaries for produce_batch
            futures: List of asyncio.Future objects to resolve
        """
        for i, batch_msg in enumerate(batch_messages):
            future = futures[i]

            def create_simple_callback(fut):
                """Create a simple callback that only resolves the future"""
                def simple_callback(err, msg):
                    if err:
                        if not fut.done():
                            fut.set_exception(_KafkaException(err))
                    else:
                        if not fut.done():
                            fut.set_result(msg)
                return simple_callback

            # Assign the simple callback to this message
            batch_msg['callback'] = create_simple_callback(future)

    def _handle_batch_failure(self, exception, batch_futures):
        """Handle batch operation failure by failing all unresolved futures

        When a batch operation fails before any individual callbacks are invoked,
        we need to fail all futures for this batch since none of the per-message
        callbacks will be called by librdkafka.

        Args:
            exception: The exception that caused the batch to fail
            batch_futures: List of futures for this batch
        """
        # Fail all futures since no individual callbacks will be invoked
        for future in batch_futures:
            # Only set exception if future isn't already done
            if not future.done():
                future.set_exception(exception)
