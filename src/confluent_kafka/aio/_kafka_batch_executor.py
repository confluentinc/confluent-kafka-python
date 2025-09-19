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


class KafkaBatchExecutor:
    """Executes Kafka batch operations via thread pool
    
    This class is responsible for:
    - Executing produce_batch operations against confluent_kafka.Producer
    - Handling partial batch failures from librdkafka
    - Managing thread pool execution to avoid blocking the event loop
    - Processing delivery callbacks for successful messages
    
    Benefits of separation:
    - Single responsibility: only handles Kafka operations
    - Clean interface between batch processing and Kafka execution
    - Easier testing with mock Kafka producers
    - Reusable across different batch processing strategies
    """
    
    def __init__(self, producer, executor):
        """Initialize the Kafka batch executor
        
        Args:
            producer: confluent_kafka.Producer instance for Kafka operations
            executor: ThreadPoolExecutor for running blocking operations
        """
        self._producer = producer
        self._executor = executor
    
    async def execute_batch(self, topic, batch_messages):
        """Execute a batch operation via thread pool
        
        This method handles the complete batch execution workflow:
        1. Execute produce_batch in thread pool to avoid blocking event loop
        2. Handle partial failures that occur during produce_batch
        3. Poll for delivery reports of successful messages
        
        Args:
            topic: Target topic for the batch
            batch_messages: List of prepared messages with callbacks assigned
            
        Returns:
            Result from producer.poll() indicating number of delivery reports processed
            
        Raises:
            Exception: Any exception from the batch operation is propagated
        """
        def _produce_batch_and_poll():
            """Helper function to run in thread pool
            
            This function encapsulates all the blocking Kafka operations:
            - Call produce_batch with individual message callbacks
            - Handle partial batch failures for messages that fail immediately
            - Poll for delivery reports to trigger callbacks for successful messages
            """
            # Call produce_batch with individual callbacks (no batch callback)
            self._producer.produce_batch(topic, batch_messages)
            
            # Handle partial batch failures: Check for messages that failed during produce_batch
            # These messages have their msgstates destroyed in Producer.c and won't get callbacks
            # from librdkafka, so we need to manually invoke their callbacks
            self._handle_partial_failures(batch_messages)
            
            # Immediately poll to process delivery callbacks for successful messages
            poll_result = self._producer.poll(0)
            
            return poll_result
        
        # Execute in thread pool to avoid blocking event loop
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(self._executor, _produce_batch_and_poll)
    
    def _handle_partial_failures(self, batch_messages):
        """Handle messages that failed during produce_batch
        
        When produce_batch encounters messages that fail immediately (e.g., message too large,
        invalid topic, etc.), librdkafka destroys their msgstates and won't call their callbacks.
        We detect these failures by checking for '_error' in the message dict (set by Producer.c)
        and manually invoke their callbacks.
        
        Args:
            batch_messages: List of message dictionaries that were passed to produce_batch
        """
        for msg_dict in batch_messages:
            if '_error' in msg_dict:
                # This message failed during produce_batch - its callback won't be called by librdkafka
                callback = msg_dict.get('callback')
                if callback:
                    # Extract the error from the message dict (set by Producer.c)
                    error = msg_dict['_error']
                    # Manually invoke the callback with the error
                    # Note: msg is None since the message failed before being queued
                    try:
                        callback(error, None)
                    except Exception as e:
                        logger.error(f"Error in callback for failed message: {e}", exc_info=True)
