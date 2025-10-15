"""
Consumer benchmark metrics collection and validation for Kafka performance testing.

Implements consumer-specific metrics tracking including poll latencies,
consumption rates, per-topic breakdowns, and reliability analysis.
"""
import time
import statistics
from typing import List, Dict, Any
from collections import defaultdict
import psutil


class ConsumerMetricsCollector:
    """Collects comprehensive performance metrics for consumer testing."""

    def __init__(self, operation_type: str = "poll", serialization_type: str = None):
        # Basic timing
        self.start_time = None
        self.end_time = None

        # Operation type: "poll" or "consume"
        self.operation_type = operation_type
        self.serialization_type = serialization_type

        # Consumer metrics (generic for both poll/consume)
        self.messages_consumed = 0
        self.operation_attempts = 0  # poll_attempts or consume_attempts
        self.operation_timeouts = 0  # poll_timeouts or consume_timeouts
        self.operation_errors = 0    # poll_errors or consume_errors
        self.operation_latencies = []  # in milliseconds
        self.error_messages = []

        # Data tracking
        self.total_bytes = 0
        self.message_sizes = []

        # Simple offset tracking for single topic/partition scenarios
        self.offsets_consumed = defaultdict(dict)  # topic -> partition -> last_offset

        # Memory tracking
        self.initial_memory_mb = None
        self.peak_memory_mb = 0

        # Batch efficiency tracking
        self.consume_call_count = 0
        self.empty_consume_count = 0

        # Process object for efficient memory monitoring
        self._process = None

    def start(self):
        """Start metrics collection"""
        self.start_time = time.time()
        self.initial_memory_mb = self._get_memory_usage()

    def record_api_call(self, operation_latency_ms: float):
        """Record a consumer API call (poll or consume) with its latency"""
        self.operation_attempts += 1
        self.operation_latencies.append(operation_latency_ms)

    def record_timeout(self, topic: str = "unknown"):
        """Record a consumer operation timeout"""
        self.operation_timeouts += 1

    def record_error(self, error_msg: str, topic: str = "unknown"):
        """Record a consumer error"""
        self.operation_errors += 1
        self.error_messages.append(error_msg)

    def record_processed_message(self, message_size: int, topic: str, partition: int,
                                 offset: int, operation_latency_ms: float):
        """Record a successfully processed message"""
        self.messages_consumed += 1
        self.total_bytes += message_size
        self.message_sizes.append(message_size)

        # Track offsets
        self.offsets_consumed[topic][partition] = offset

        # Update peak memory usage
        self._update_memory_usage()

    def finalize(self):
        """Finalize metrics collection"""
        self.end_time = time.time()

    def record_batch_operation(self, messages_returned: int):
        """Record a batch operation (consume() call) and how many messages it returned"""
        self.consume_call_count += 1
        if messages_returned == 0:
            self.empty_consume_count += 1

    def _get_memory_usage(self) -> float:
        """Get current memory usage in MB"""
        if psutil is None:
            return 0.0
        try:
            if self._process is None:
                self._process = psutil.Process()
            return self._process.memory_info().rss / (1024 * 1024)
        except (psutil.Error, OSError):
            return 0.0

    def _update_memory_usage(self):
        """Update peak memory usage"""
        current_memory = self._get_memory_usage()
        if current_memory > self.peak_memory_mb:
            self.peak_memory_mb = current_memory

    def _percentile(self, data, percentile):
        """Calculate percentile for datasets where quantiles() fails"""
        if not data:
            return 0
        sorted_data = sorted(data)
        k = (len(sorted_data) - 1) * percentile / 100.0
        f = int(k)
        c = k - f
        if f == len(sorted_data) - 1:
            return sorted_data[f]
        return sorted_data[f] * (1 - c) + sorted_data[f + 1] * c

    def get_summary(self) -> Dict[str, Any]:
        """Get consumer-specific metrics summary"""
        if not self.start_time or not self.end_time:
            return {}

        duration = self.end_time - self.start_time

        # Consumer-specific calculations
        consumption_rate = self.messages_consumed / duration if duration > 0 else 0
        operation_rate = self.operation_attempts / duration if duration > 0 else 0

        # Operation metrics (generic for poll/consume)
        operation_error_rate = (self.operation_errors / self.operation_attempts
                                if self.operation_attempts > 0 else 0)
        operation_success_rate = ((self.operation_attempts - self.operation_timeouts -
                                   self.operation_errors) / self.operation_attempts
                                  if self.operation_attempts > 0 else 0)

        # Operation latency analysis
        if self.operation_latencies:
            avg_operation_latency = statistics.mean(self.operation_latencies)
            p50_operation_latency = statistics.median(self.operation_latencies)
            max_operation_latency = max(self.operation_latencies)

            try:
                quantiles = statistics.quantiles(self.operation_latencies, n=100)
                p95_operation_latency = quantiles[94]  # 95th percentile
                p99_operation_latency = quantiles[98]  # 99th percentile
            except statistics.StatisticsError:
                p95_operation_latency = self._percentile(self.operation_latencies, 95)
                p99_operation_latency = self._percentile(self.operation_latencies, 99)
        else:
            avg_operation_latency = p50_operation_latency = p95_operation_latency = (
                p99_operation_latency) = max_operation_latency = 0

        # Message size analysis
        if self.message_sizes:
            avg_message_size = statistics.mean(self.message_sizes)
            min_message_size = min(self.message_sizes)
            max_message_size = max(self.message_sizes)
            median_message_size = statistics.median(self.message_sizes)
        else:
            avg_message_size = min_message_size = max_message_size = median_message_size = 0

        # Memory usage analysis
        memory_growth_mb = 0
        if self.initial_memory_mb and self.peak_memory_mb:
            memory_growth_mb = self.peak_memory_mb - self.initial_memory_mb

        # Batch efficiency analysis
        messages_per_consume = self.messages_consumed / self.consume_call_count if self.consume_call_count > 0 else 0
        empty_consume_rate = self.empty_consume_count / self.consume_call_count if self.consume_call_count > 0 else 0

        # Base summary
        base_summary = {
            'start_time': self.start_time,
            'end_time': self.end_time,
            'duration_seconds': duration
        }

        # Consumer metrics
        # Dynamic metric names based on operation type
        op_name = self.operation_type  # "poll" or "consume"

        consumer_metrics = {
            # Basic metrics
            'messages_consumed': self.messages_consumed,
            'consumption_rate_msg_per_sec': consumption_rate,
            'data_throughput_mb_per_sec': (self.total_bytes / (1024 * 1024)) / duration if duration > 0 else 0,
            'avg_latency_ms': avg_operation_latency,
            'p50_latency_ms': p50_operation_latency,
            'p95_latency_ms': p95_operation_latency,
            'p99_latency_ms': p99_operation_latency,
            'max_latency_ms': max_operation_latency,
            'error_rate': operation_error_rate,
            'success_rate': operation_success_rate,
            'total_bytes': self.total_bytes,

            # Enhanced metrics
            'avg_message_size_bytes': avg_message_size,
            'median_message_size_bytes': median_message_size,
            'min_message_size_bytes': min_message_size,
            'max_message_size_bytes': max_message_size,
            'memory_growth_mb': memory_growth_mb,
            'peak_memory_mb': self.peak_memory_mb,

            # Operation-specific metrics (dynamic naming)
            f'{op_name}_attempts': self.operation_attempts,
            f'{op_name}_rate_per_sec': operation_rate,
            f'{op_name}_timeouts': self.operation_timeouts,
            f'{op_name}_errors': self.operation_errors,
            f'messages_per_{op_name}': messages_per_consume,  # Will be messages_per_poll or messages_per_consume
            f'empty_{op_name}_rate': empty_consume_rate,  # Will be empty_poll_rate or empty_consume_rate
            f'{op_name}_call_count': self.consume_call_count,  # Generic call count
            f'empty_{op_name}_count': self.empty_consume_count,
        }

        base_summary.update(consumer_metrics)
        return base_summary


class ConsumerMetricsBounds:
    """Performance bounds for consumer metrics validation"""

    def __init__(self,
                 min_consumption_rate: float = 1.0,
                 max_avg_latency_ms: float = 5000.0,
                 max_p95_latency_ms: float = 10000.0,
                 min_success_rate: float = 0.90,
                 max_error_rate: float = 0.05,
                 max_memory_growth_mb: float = 600.0,
                 min_messages_per_consume: float = 0.5,
                 max_empty_consume_rate: float = 0.5):
        self.min_consumption_rate = min_consumption_rate
        self.max_avg_latency_ms = max_avg_latency_ms
        self.max_p95_latency_ms = max_p95_latency_ms
        self.min_success_rate = min_success_rate
        self.max_error_rate = max_error_rate
        # Enhanced bounds
        self.max_memory_growth_mb = max_memory_growth_mb
        self.min_messages_per_consume = min_messages_per_consume
        self.max_empty_consume_rate = max_empty_consume_rate


def validate_consumer_metrics(metrics: Dict[str, Any], bounds: ConsumerMetricsBounds) -> tuple[bool, List[str]]:
    """Validate consumer metrics against performance bounds"""
    violations = []

    # Consumption rate check
    consumption_rate = metrics.get('consumption_rate_msg_per_sec', 0)
    if consumption_rate < bounds.min_consumption_rate:
        violations.append(f"Consumption rate {consumption_rate:.2f} msg/s below minimum {bounds.min_consumption_rate}")

    # Latency checks
    avg_latency = metrics.get('avg_latency_ms', 0)
    if avg_latency > bounds.max_avg_latency_ms:
        violations.append(f"Average latency {avg_latency:.2f}ms exceeds maximum {bounds.max_avg_latency_ms}ms")

    p95_latency = metrics.get('p95_latency_ms', 0)
    if p95_latency > bounds.max_p95_latency_ms:
        violations.append(f"P95 latency {p95_latency:.2f}ms exceeds maximum {bounds.max_p95_latency_ms}ms")

    # Success rate check
    success_rate = metrics.get('success_rate', 0)
    if success_rate < bounds.min_success_rate:
        violations.append(f"Success rate {success_rate:.4f} below minimum {bounds.min_success_rate}")

    # Error rate check
    error_rate = metrics.get('error_rate', 0)
    if error_rate > bounds.max_error_rate:
        violations.append(f"Error rate {error_rate:.4f} exceeds maximum {bounds.max_error_rate}")

    # Enhanced metrics validation
    memory_growth = metrics.get('memory_growth_mb', 0)
    if memory_growth > bounds.max_memory_growth_mb:
        violations.append(f"Memory growth {memory_growth:.2f}MB exceeds maximum {bounds.max_memory_growth_mb}MB")

    # Batch efficiency validation (only for consume operations, poll operations don't have batch metrics)
    # Check if this is a consume operation by looking for consume-specific metrics
    if 'messages_per_consume' in metrics:
        messages_per_consume = metrics.get('messages_per_consume', 0)
        if messages_per_consume < bounds.min_messages_per_consume:
            violations.append(f"Messages per consume {messages_per_consume:.2f} "
                              f"below minimum {bounds.min_messages_per_consume}")

        empty_consume_rate = metrics.get('empty_consume_rate', 0)
        if empty_consume_rate > bounds.max_empty_consume_rate:
            violations.append(f"Empty consume rate {empty_consume_rate:.3f} "
                              f"exceeds maximum {bounds.max_empty_consume_rate}")

    # For poll operations, we skip batch efficiency validation since they're single-message operations
    is_valid = len(violations) == 0
    return is_valid, violations


def print_consumer_metrics_report(metrics: Dict[str, Any], is_valid: bool, violations: List[str],
                                  consumer_type: str = None, batch_size: int = None, serialization_type: str = None):
    """Print simplified consumer metrics report"""
    # Detect operation type from metrics keys
    op_name = "consume"  # default
    if any(key.startswith('poll_') for key in metrics.keys()):
        op_name = "poll"

    # Build informative header
    header_parts = ["Consumer Performance Report"]
    if consumer_type:
        header_parts.append(f"{consumer_type.upper()}")
    if op_name == "consume" and batch_size is not None:
        header_parts.append(f"{op_name.upper()}(batch_size={batch_size})")
    if serialization_type:
        header_parts.append(f"serialization_type={serialization_type}")
    else:
        header_parts.append(f"{op_name.upper()}()")

    header = " - ".join(header_parts)
    print(f"\n=== {header} ===")

    # Basic metrics
    print(f"Duration: {metrics.get('duration_seconds', 0):.2f}s")
    print(f"Messages consumed: {metrics.get('messages_consumed', 0)}")
    print(f"Consumption throughput: {metrics.get('consumption_rate_msg_per_sec', 0):.2f} msg/s")
    print(f"Data throughput: {metrics.get('data_throughput_mb_per_sec', 0):.4f} MB/s")

    # Latency metrics
    print("\nLatency Metrics:")
    print(f"  Average: {metrics.get('avg_latency_ms', 0):.2f}ms")
    print(f"  P50: {metrics.get('p50_latency_ms', 0):.2f}ms")
    print(f"  P95: {metrics.get('p95_latency_ms', 0):.2f}ms")
    print(f"  P99: {metrics.get('p99_latency_ms', 0):.2f}ms")
    print(f"  Max: {metrics.get('max_latency_ms', 0):.2f}ms")

    # Reliability metrics
    print("\nReliability:")
    print(f"  Error rate: {metrics.get('error_rate', 0):.4f}")

    # Message size analysis
    print("\nMessage Size Analysis:")
    print(f"  Average: {metrics.get('avg_message_size_bytes', 0):.0f} bytes")
    print(f"  Median: {metrics.get('median_message_size_bytes', 0):.0f} bytes")
    print(f"  Range: {metrics.get('min_message_size_bytes', 0):.0f} - "
          f"{metrics.get('max_message_size_bytes', 0):.0f} bytes")

    # Memory usage (if available)
    if metrics.get('peak_memory_mb', 0) > 0:
        print("\nMemory Usage:")
        print(f"  Peak: {metrics.get('peak_memory_mb', 0):.2f}MB")
        print(f"  Growth: {metrics.get('memory_growth_mb', 0):.2f}MB")

    # Batch efficiency (only for consume operations, not poll)
    # Try to detect operation type from metric keys
    op_name = "consume"  # default
    if any(key.startswith('poll_') for key in metrics.keys()):
        op_name = "poll"

    # Only show efficiency metrics for batch operations (consume), not single-message operations (poll)
    if op_name == "consume":
        print(f"\n{op_name.title()} Efficiency:")
        print(f"  Messages per {op_name}(): {metrics.get(f'messages_per_{op_name}', 0):.2f}")
        print(f"  Empty {op_name} rate: {metrics.get(f'empty_{op_name}_rate', 0):.3f}")
        print(f"  Total {op_name}() calls: {metrics.get(f'{op_name}_call_count', 0)}")

    # Validation
    print("\nValidation:")
    if is_valid:
        print("  All performance bounds satisfied")
    else:
        print("  Performance bounds violations:")
        for violation in violations:
            print(f"    - {violation}")

    print("=" * 50)
