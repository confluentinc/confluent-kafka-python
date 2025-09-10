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
import confluent_kafka.aio._common as _common


class AIOProducer:
    
    def __init__(self, producer_conf, max_workers=1, executor=None):
        if executor is not None:
            self.executor = executor
        else:
            self.executor = concurrent.futures.ThreadPoolExecutor(
                max_workers=max_workers)
        wrap_common_callbacks = _common.wrap_common_callbacks
        wrap_common_callbacks(asyncio.get_running_loop(), producer_conf)

        self._producer = confluent_kafka.Producer(producer_conf)
        
        # Internal metrics tracking
        self._metrics_callback = None
        self._send_times = {}  # Track send times for latency calculation

    def set_metrics_callback(self, callback):
        """Set callback for delivery metrics: callback(latency_ms, topic, partition, success)"""
        self._metrics_callback = callback

    # ========================================================================
    # HYBRID OPERATIONS - Blocking behavior depends on parameters
    # ========================================================================

    async def poll(self, timeout=0, *args, **kwargs):
        """Processes callbacks - blocking behavior depends on timeout
        
        Args:
            timeout: 0 = non-blocking, >0 = block up to timeout seconds, -1 = block indefinitely
        """
        if timeout > 0:
            # Blocking call - use ThreadPool to avoid blocking event loop
            return await self._call(self._producer.poll, timeout, *args, **kwargs)
        else:
            # Non-blocking call (timeout=0) - direct call is safe
            return self._producer.poll(timeout, *args, **kwargs)

    # ========================================================================
    # NON-BLOCKING OPERATIONS - Direct calls (no ThreadPool overhead)
    # These operations are already async-safe in librdkafka
    # ========================================================================

    async def produce(self, topic, value=None, key=None, *args, **kwargs):
        """Non-blocking: Queues message and returns immediately
        
        Args:
            topic: Kafka topic name (required)
            value: Message payload (optional)
            key: Message key (optional)
            *args, **kwargs: Additional parameters like partition, timestamp, headers
        """
        # Get current running event loop
        result = asyncio.get_running_loop().create_future()
        
        # Track send time for latency calculation if metrics are enabled
        send_time = None
        message_id = None
        if self._metrics_callback:
            import time
            send_time = time.time()
            message_id = f"{topic}:{key}:{id(result)}"  # Unique message identifier
            self._send_times[message_id] = send_time

        # Pre-bind variables to avoid closure overhead
        def on_delivery(err, msg):
            if err:
                # Handle delivery failure
                if self._metrics_callback and message_id:
                    latency_ms = 0.0
                    if message_id in self._send_times:
                        latency_ms = (time.time() - self._send_times[message_id]) * 1000
                        del self._send_times[message_id]
                    self._metrics_callback(latency_ms, topic, 0, False)  # success=False
                result.set_exception(_KafkaException(err))
            else:
                # Handle delivery success
                if self._metrics_callback and message_id:
                    latency_ms = 0.0
                    if message_id in self._send_times:
                        latency_ms = (time.time() - self._send_times[message_id]) * 1000
                        del self._send_times[message_id]
                    self._metrics_callback(latency_ms, msg.topic(), msg.partition(), True)  # success=True
                result.set_result(msg)
        
        kwargs['on_delivery'] = on_delivery
        self._producer.produce(topic, value, key, *args, **kwargs)
        return result
    

    # ========================================================================
    # BLOCKING OPERATIONS - Use ThreadPool to avoid blocking event loop
    # These operations may block waiting for network I/O or other resources
    # ========================================================================

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
