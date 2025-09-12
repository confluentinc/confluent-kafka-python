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
    
    def __init__(self, producer_conf, max_workers=1, executor=None):
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

    # ========================================================================
    # HYBRID OPERATIONS - Blocking behavior depends on parameters
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
        # Start Time
        start_time = time.perf_counter()
        
        # Get current running event loop
        result = asyncio.get_running_loop().create_future()

        # Store user's original callback if provided
        user_callback = kwargs.get('on_delivery')
        
        # Pre-bind variables to avoid closure overhead
        def on_delivery(err, msg):
            if err:
                # Handle delivery failure
                result.set_exception(_KafkaException(err))
            else:
                # Handle delivery success
                result.set_result(msg)
                
                # Call user's callback on successful delivery // Use Different ThreadPool for this 
                if user_callback:
                    try:
                        user_callback(err, msg)  # err is None here
                    except Exception:
                        # Log but don't propagate user callback errors to avoid breaking delivery confirmation
                        pass
        
        kwargs['on_delivery'] = on_delivery
        self._producer.produce(topic, value, key, *args, **kwargs) #  No TheadPool
        # await self._call(self._producer.produce, topic, value, key, *args, **kwargs) # ThreadPool
        
        # END Time - Calculate and store latency
        end_time = time.perf_counter()
        latency = end_time - start_time
        
        # Update metrics
        self._latency_metrics.append(latency)
        self._total_calls += 1
        self._total_latency += latency
        self._min_latency = min(self._min_latency, latency)
        self._max_latency = max(self._max_latency, latency)
        
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
