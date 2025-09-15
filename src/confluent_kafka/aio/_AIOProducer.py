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
import confluent_kafka
from confluent_kafka import KafkaException as _KafkaException
import functools
import time
import confluent_kafka.aio._common as _common


class AIOProducer:
    
    def __init__(self, producer_conf, max_workers=4, executor=None, batch_size=1000):
        if executor is not None:
            self.executor = executor
        else:
            self.executor = concurrent.futures.ThreadPoolExecutor(
                max_workers=max_workers)
        wrap_common_callbacks = _common.wrap_common_callbacks
        wrap_common_callbacks(asyncio.get_running_loop(), producer_conf)

        self._producer = confluent_kafka.Producer(producer_conf)
        
        # Metrics for latency tracking
        self._latency_metrics = []
        self._total_calls = 0
        self._total_latency = 0.0
        self._min_latency = float('inf')
        self._max_latency = 0.0
        
        # Batching configuration and buffer
        self._batch_size = batch_size
        self._message_buffer = []
        self._buffer_futures = []  # Track futures for each buffered message
        self._buffer_lock = asyncio.Lock()  # Protect buffer access
    

    # ========================================================================
    # BLOCKING OPERATIONS - Use ThreadPool to avoid blocking event loop
    # These operations may block waiting for network I/O or other resources
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


    async def produce(self, topic, value=None, key=None, *args, **kwargs):
        """Batched produce: Accumulates messages in buffer and flushes when threshold reached
        
        Args:
            topic: Kafka topic name (required)
            value: Message payload (optional)
            key: Message key (optional)
            *args, **kwargs: Additional parameters like partition, timestamp, headers
        """
        # Start Time
        start_time = time.perf_counter()
        
        result = asyncio.get_running_loop().create_future()
        user_callback = kwargs.get('on_delivery')
        
        await self._buffer_lock.acquire()
        
        try:
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
            
            should_flush = len(self._message_buffer) >= self._batch_size
            
        finally:
            self._buffer_lock.release()
        
        if should_flush:
            await self._flush_buffer()
        end_time = time.perf_counter()
        latency = end_time - start_time
        
        # Update metrics
        self._latency_metrics.append(latency)
        self._total_calls += 1
        self._total_latency += latency
        self._min_latency = min(self._min_latency, latency)
        self._max_latency = max(self._max_latency, latency)
        
        return result
 
    async def _call(self, blocking_task, *args, **kwargs):
        """Helper method for blocking operations that need ThreadPool execution"""
        return (await asyncio.gather(
            asyncio.get_running_loop().run_in_executor(self.executor,
                                                       functools.partial(
                                                           blocking_task,
                                                           *args,
                                                           **kwargs))

        ))[0]

    async def flush(self, *args, **kwargs):
        """Waits until all messages are delivered or timeout"""
        # First, flush any remaining messages in the buffer for all topics
        async with self._buffer_lock:
            if self._message_buffer:
                # Flush all topics - the _flush_buffer method now properly handles multiple topics
                await self._flush_buffer()
        
        # Then flush the producer
        return await self._call(self._producer.flush, *args, **kwargs)

    async def purge(self, *args, **kwargs):
        """Purges messages from internal queues - may block during cleanup"""
        return await self._call(self._producer.purge, *args, **kwargs)

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

    async def set_sasl_credentials(self, *args, **kwargs):
        """Authentication operation that may involve network calls"""
        return await self._call(self._producer.set_sasl_credentials,
                                *args, **kwargs)

    def print_latency_metrics(self):
        """Print aggregated latency metrics for produce method calls"""
        if self._total_calls == 0:
            print("No produce calls have been made yet.")
            return
        
        # Calculate average latency
        avg_latency = self._total_latency / self._total_calls
        
        # Calculate median latency
        sorted_latencies = sorted(self._latency_metrics)
        n = len(sorted_latencies)
        if n % 2 == 0:
            median_latency = (sorted_latencies[n//2 - 1] + sorted_latencies[n//2]) / 2
        else:
            median_latency = sorted_latencies[n//2]
        
        # Calculate 95th percentile
        p95_index = int(0.95 * n)
        p95_latency = sorted_latencies[min(p95_index, n-1)]
        
        # Calculate 99th percentile
        p99_index = int(0.99 * n)
        p99_latency = sorted_latencies[min(p99_index, n-1)]
        
        print("=" * 60)
        print("AIOPRODUCER PRODUCE LATENCY METRICS")
        print("=" * 60)
        print(f"Total produce() calls:     {self._total_calls:,}")
        print(f"Total latency:            {self._total_latency:.6f} seconds")
        print(f"Average latency:          {avg_latency:.6f} seconds ({avg_latency*1000:.3f} ms)")
        print(f"Median latency:           {median_latency:.6f} seconds ({median_latency*1000:.3f} ms)")
        print(f"Min latency:              {self._min_latency:.6f} seconds ({self._min_latency*1000:.3f} ms)")
        print(f"Max latency:              {self._max_latency:.6f} seconds ({self._max_latency*1000:.3f} ms)")
        print(f"95th percentile latency:  {p95_latency:.6f} seconds ({p95_latency*1000:.3f} ms)")
        print(f"99th percentile latency:  {p99_latency:.6f} seconds ({p99_latency*1000:.3f} ms)")
        print("=" * 60)

    def get_latency_stats(self):
        """Return latency statistics as a dictionary"""
        if self._total_calls == 0:
            return {
                'total_calls': 0,
                'total_latency': 0.0,
                'avg_latency': 0.0,
                'median_latency': 0.0,
                'min_latency': 0.0,
                'max_latency': 0.0,
                'p95_latency': 0.0,
                'p99_latency': 0.0
            }
        
        # Calculate average latency
        avg_latency = self._total_latency / self._total_calls
        
        # Calculate median latency
        sorted_latencies = sorted(self._latency_metrics)
        n = len(sorted_latencies)
        if n % 2 == 0:
            median_latency = (sorted_latencies[n//2 - 1] + sorted_latencies[n//2]) / 2
        else:
            median_latency = sorted_latencies[n//2]
        
        # Calculate percentiles
        p95_index = int(0.95 * n)
        p95_latency = sorted_latencies[min(p95_index, n-1)]
        
        p99_index = int(0.99 * n)
        p99_latency = sorted_latencies[min(p99_index, n-1)]
        
        return {
            'total_calls': self._total_calls,
            'total_latency': self._total_latency,
            'avg_latency': avg_latency,
            'median_latency': median_latency,
            'min_latency': self._min_latency if self._min_latency != float('inf') else 0.0,
            'max_latency': self._max_latency,
            'p95_latency': p95_latency,
            'p99_latency': p99_latency
        }

      
    # ========================================================================
    # BATCHING OPERATIONS - Buffer management and batch processing
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
        
        # Group messages by topic
        topic_groups = {}
        for i, msg_data in enumerate(self._message_buffer):
            topic = msg_data['topic']
            if topic not in topic_groups:
                topic_groups[topic] = {
                    'messages': [],
                    'futures': [],
                    'callbacks': [],
                    'indices': []
                }
            topic_groups[topic]['messages'].append(msg_data)
            topic_groups[topic]['futures'].append(self._buffer_futures[i])
            topic_groups[topic]['callbacks'].append(msg_data.get('user_callback'))
            topic_groups[topic]['indices'].append(i)
        
        # Clear buffers immediately (reduce memory pressure)
        self._message_buffer.clear()
        self._buffer_futures.clear()
        
        # Process each topic group separately
        for topic, group_data in topic_groups.items():
            # Skip if target_topic is specified and doesn't match
            if target_topic is not None and topic != target_topic:
                # Re-add messages for other topics back to buffer
                for j, msg_data in enumerate(group_data['messages']):
                    self._message_buffer.append(msg_data)
                    self._buffer_futures.append(group_data['futures'][j])
                continue
            
            # Convert messages to batch format
            batch_messages = []
            for msg_data in group_data['messages']:
                batch_msg = {
                    'value': msg_data['value'],
                    'key': msg_data['key']
                }
                
                # Add optional fields if present
                if 'partition' in msg_data:
                    batch_msg['partition'] = msg_data['partition']
                if 'timestamp' in msg_data:
                    batch_msg['timestamp'] = msg_data['timestamp']
                if 'headers' in msg_data:
                    batch_msg['headers'] = msg_data['headers']
                
                batch_messages.append(batch_msg)
            
            # Setup callback with pre-allocated state
            batch_futures = group_data['futures']
            batch_user_callbacks = group_data['callbacks']
            callback_state = {'index': 0, 'futures': batch_futures, 'callbacks': batch_user_callbacks}
            
            def batch_delivery_callback(err, msg):
                idx = callback_state['index']
                if idx < len(callback_state['futures']):
                    future = callback_state['futures'][idx]
                    user_callback = callback_state['callbacks'][idx]
                    
                    if err:
                        future.set_exception(_KafkaException(err))
                    else:
                        future.set_result(msg)
                        if user_callback:
                            try:
                                user_callback(err, msg)
                            except Exception:
                                pass
                    
                    callback_state['index'] += 1
            
            try:
                # Call produce_batch and poll for this topic
                await self._call(self._produce_batch_and_poll, topic, batch_messages, batch_delivery_callback)
                        
            except Exception as e:
                # If batch fails, fail all remaining futures for this topic
                start_idx = callback_state['index']
                for i in range(start_idx, len(batch_futures)):
                    if not batch_futures[i].done():
                        batch_futures[i].set_exception(e)

    async def flush_buffer(self):
        """Manually flush the current message buffer for all topics"""
        async with self._buffer_lock:
            if self._message_buffer:
                # Flush all topics (don't specify target_topic)
                await self._flush_buffer()


