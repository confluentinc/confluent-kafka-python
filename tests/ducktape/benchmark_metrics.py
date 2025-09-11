"""
Producer benchmark metrics collection and validation for Kafka performance testing.

Implements comprehensive metrics tracking including latency percentiles,
per-topic/partition breakdowns, memory monitoring, and batch efficiency analysis.
"""
import time
import statistics
import os
import json
from typing import List, Dict, Any, Optional
from collections import defaultdict
import psutil


class MetricsCollector:
    """Collects comprehensive performance metrics for producer testing."""

    def __init__(self):
        # Basic timing
        self.start_time = None
        self.end_time = None

        # Message counts
        self.messages_sent = 0
        self.messages_delivered = 0
        self.messages_failed = 0
        self.delivery_latencies = []  # in milliseconds

        # Data tracking
        self.total_bytes = 0
        self.message_sizes = []

        # Per-topic/partition metrics
        self.topic_metrics = defaultdict(lambda: {
            'sent': 0, 'delivered': 0, 'failed': 0, 'bytes': 0, 'latencies': []
        })
        self.partition_metrics = defaultdict(lambda: {
            'sent': 0, 'delivered': 0, 'failed': 0, 'bytes': 0, 'latencies': []
        })

        # Memory tracking
        self.initial_memory_mb = None
        self.peak_memory_mb = 0

        # Batch efficiency tracking
        self.poll_count = 0
        self.buffer_full_count = 0

        # Process object for efficient memory monitoring
        self._process = None

    def start(self):
        """Start metrics collection"""
        self.start_time = time.time()
        self.initial_memory_mb = self._get_memory_usage()

    def record_sent(self, message_size: int, topic: str = "unknown", partition: int = 0):
        """Record a message being sent"""
        self.messages_sent += 1
        self.total_bytes += message_size
        self.message_sizes.append(message_size)

        # Track per-topic/partition
        self.topic_metrics[topic]['sent'] += 1
        self.topic_metrics[topic]['bytes'] += message_size

        partition_key = f"{topic}-{partition}"
        self.partition_metrics[partition_key]['sent'] += 1
        self.partition_metrics[partition_key]['bytes'] += message_size

    def record_delivered(self, latency_ms: float, topic: str = "unknown", partition: int = 0):
        """Record a message being delivered"""
        self.messages_delivered += 1
        self.delivery_latencies.append(latency_ms)

        # Track per-topic/partition latencies
        self.topic_metrics[topic]['delivered'] += 1
        self.topic_metrics[topic]['latencies'].append(latency_ms)

        partition_key = f"{topic}-{partition}"
        self.partition_metrics[partition_key]['delivered'] += 1
        self.partition_metrics[partition_key]['latencies'].append(latency_ms)

    def record_failed(self, topic: str = "unknown", partition: int = 0):
        """Record a message delivery failure"""
        self.messages_failed += 1

        # Track per-topic/partition failures
        self.topic_metrics[topic]['failed'] += 1

        partition_key = f"{topic}-{partition}"
        self.partition_metrics[partition_key]['failed'] += 1

    def record_poll(self):
        """Record a producer poll operation for batch efficiency tracking"""
        self.poll_count += 1

        # Update peak memory usage
        current_memory = self._get_memory_usage()
        if current_memory and current_memory > self.peak_memory_mb:
            self.peak_memory_mb = current_memory

    def record_buffer_full(self):
        """Record a buffer full event for batch efficiency tracking"""
        self.buffer_full_count += 1

    def finalize(self):
        """Finalize metrics collection"""
        self.end_time = time.time()

    def get_summary(self) -> Dict[str, Any]:
        """Get comprehensive metrics summary"""
        if not self.start_time or not self.end_time:
            return {}

        duration = self.end_time - self.start_time

        # Basic calculations
        send_throughput = self.messages_sent / duration if duration > 0 else 0
        delivery_throughput = self.messages_delivered / duration if duration > 0 else 0
        data_throughput_mb = (self.total_bytes / (1024 * 1024)) / duration if duration > 0 else 0

        # Enhanced latency calculations using built-in functions
        if self.delivery_latencies:
            avg_latency = statistics.mean(self.delivery_latencies)
            p50_latency = statistics.median(self.delivery_latencies)
            max_latency = max(self.delivery_latencies)

            # Use quantiles for P95, P99 (more accurate than custom implementation)
            try:
                quantiles = statistics.quantiles(self.delivery_latencies, n=100)
                p95_latency = quantiles[94]  # 95th percentile
                p99_latency = quantiles[98]  # 99th percentile
            except statistics.StatisticsError:
                # Fallback for small datasets
                p95_latency = self._percentile(self.delivery_latencies, 95)
                p99_latency = self._percentile(self.delivery_latencies, 99)
        else:
            avg_latency = p50_latency = p95_latency = p99_latency = max_latency = 0

        # Error rates
        total_attempts = self.messages_sent
        error_rate = self.messages_failed / total_attempts if total_attempts > 0 else 0
        success_rate = self.messages_delivered / total_attempts if total_attempts > 0 else 0

        # Message size analysis using built-in functions
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
        messages_per_poll = self.messages_delivered / self.poll_count if self.poll_count > 0 else 0
        buffer_full_rate = self.buffer_full_count / self.messages_sent if self.messages_sent > 0 else 0

        # Per-topic metrics summary
        topic_summary = {}
        for topic, metrics in self.topic_metrics.items():
            if metrics['sent'] > 0:
                topic_summary[topic] = {
                    'messages_sent': metrics['sent'],
                    'messages_delivered': metrics['delivered'],
                    'success_rate': metrics['delivered'] / metrics['sent'],
                    'avg_latency_ms': statistics.mean(metrics['latencies']) if metrics['latencies'] else 0,
                    'total_bytes': metrics['bytes']
                }

        # Per-partition metrics summary
        partition_summary = {}
        for partition_key, metrics in self.partition_metrics.items():
            if metrics['sent'] > 0:
                partition_summary[partition_key] = {
                    'messages_sent': metrics['sent'],
                    'messages_delivered': metrics['delivered'],
                    'success_rate': metrics['delivered'] / metrics['sent'],
                    'avg_latency_ms': statistics.mean(metrics['latencies']) if metrics['latencies'] else 0,
                    'total_bytes': metrics['bytes']
                }

        return {
            # Basic metrics
            'duration_seconds': duration,
            'messages_sent': self.messages_sent,
            'messages_delivered': self.messages_delivered,
            'messages_failed': self.messages_failed,
            'send_throughput_msg_per_sec': send_throughput,
            'delivery_throughput_msg_per_sec': delivery_throughput,
            'data_throughput_mb_per_sec': data_throughput_mb,
            'avg_latency_ms': avg_latency,
            'p95_latency_ms': p95_latency,
            'max_latency_ms': max_latency,
            'error_rate': error_rate,
            'success_rate': success_rate,
            'total_bytes': self.total_bytes,

            # Enhanced metrics
            'p50_latency_ms': p50_latency,
            'p99_latency_ms': p99_latency,
            'avg_message_size_bytes': avg_message_size,
            'median_message_size_bytes': median_message_size,
            'min_message_size_bytes': min_message_size,
            'max_message_size_bytes': max_message_size,
            'memory_growth_mb': memory_growth_mb,
            'peak_memory_mb': self.peak_memory_mb,
            'messages_per_poll': messages_per_poll,
            'buffer_full_rate': buffer_full_rate,
            'poll_count': self.poll_count,
            'buffer_full_count': self.buffer_full_count,
            'topic_metrics': topic_summary,
            'partition_metrics': partition_summary
        }

    def _percentile(self, data: List[float], percentile: float) -> float:
        """Calculate percentile of a dataset"""
        if not data:
            return 0.0
        sorted_data = sorted(data)
        index = (percentile / 100) * (len(sorted_data) - 1)
        if index.is_integer():
            return sorted_data[int(index)]
        else:
            lower = sorted_data[int(index)]
            upper = sorted_data[int(index) + 1]
            return lower + (upper - lower) * (index - int(index))

    def _get_memory_usage(self) -> Optional[float]:
        """Get current memory usage in MB"""
        try:
            # Initialize process object once for efficiency
            if self._process is None:
                self._process = psutil.Process(os.getpid())

            return self._process.memory_info().rss / (1024 * 1024)
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            # Handle edge cases where process might not exist or be accessible
            return None


class MetricsBounds:
    """Performance bounds for metrics validation"""

    def __init__(self):
        # Load bounds from config file
        config_path = os.getenv("BENCHMARK_BOUNDS_CONFIG")
        if config_path is None:
            # Default to config file in same directory as this module
            config_path = os.path.join(os.path.dirname(__file__), "benchmark_bounds.json")
        self._load_from_config_file(config_path)

    def _load_from_config_file(self, config_path: str):
        """Load bounds from a JSON configuration file."""
        if not os.path.exists(config_path):
            raise FileNotFoundError(f"Config file not found: {config_path}")

        try:
            with open(config_path, 'r') as f:
                config = json.load(f)

            # Set values from config file
            for key, value in config.items():
                if not key.startswith('_'):  # Skip comment fields like "_comment"
                    setattr(self, key, value)

        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON in config file {config_path}: {e}")
        except IOError as e:
            raise IOError(f"Failed to read config file {config_path}: {e}")

    @classmethod
    def from_config_file(cls, config_path: str) -> 'MetricsBounds':
        """Load bounds from a specific JSON configuration file."""
        bounds = cls.__new__(cls)  # Create without calling __init__
        bounds._load_from_config_file(config_path)
        return bounds


def validate_metrics(metrics: Dict[str, Any], bounds: MetricsBounds) -> tuple:
    """
    Validate metrics against performance bounds.

    Returns:
        tuple: (is_valid, list_of_violations)
    """
    violations = []

    # Basic validation
    if metrics['send_throughput_msg_per_sec'] < bounds.min_throughput_msg_per_sec:
        violations.append(
            f"Throughput too low: {metrics['send_throughput_msg_per_sec']:.2f} msg/s "
            f"(minimum: {bounds.min_throughput_msg_per_sec})"
        )

    if metrics['p95_latency_ms'] > bounds.max_p95_latency_ms:
        violations.append(
            f"P95 latency too high: {metrics['p95_latency_ms']:.2f}ms "
            f"(maximum: {bounds.max_p95_latency_ms}ms)"
        )

    if metrics['error_rate'] > bounds.max_error_rate:
        violations.append(
            f"Error rate too high: {metrics['error_rate']:.4f} "
            f"(maximum: {bounds.max_error_rate})"
        )

    if metrics['success_rate'] < bounds.min_success_rate:
        violations.append(
            f"Success rate too low: {metrics['success_rate']:.4f} "
            f"(minimum: {bounds.min_success_rate})"
        )

    # Enhanced validation
    if metrics['p99_latency_ms'] > bounds.max_p99_latency_ms:
        violations.append(
            f"P99 latency too high: {metrics['p99_latency_ms']:.2f}ms "
            f"(maximum: {bounds.max_p99_latency_ms}ms)"
        )

    if metrics['memory_growth_mb'] > bounds.max_memory_growth_mb:
        violations.append(
            f"Memory growth too high: {metrics['memory_growth_mb']:.2f}MB "
            f"(maximum: {bounds.max_memory_growth_mb}MB)"
        )

    if metrics['buffer_full_rate'] > bounds.max_buffer_full_rate:
        violations.append(
            f"Buffer full rate too high: {metrics['buffer_full_rate']:.3f} "
            f"(maximum: {bounds.max_buffer_full_rate})"
        )

    if metrics['messages_per_poll'] < bounds.min_messages_per_poll:
        violations.append(
            f"Batch efficiency too low: {metrics['messages_per_poll']:.2f} messages/poll "
            f"(minimum: {bounds.min_messages_per_poll})"
        )

    return len(violations) == 0, violations


def print_metrics_report(metrics: Dict[str, Any], is_valid: bool, violations: List[str]):
    """Print comprehensive metrics report"""
    print("\n=== Performance Metrics Report ===")

    # Basic metrics
    print(f"Duration: {metrics['duration_seconds']:.2f}s")
    print(f"Messages sent: {metrics['messages_sent']}")
    print(f"Messages delivered: {metrics['messages_delivered']}")
    print(f"Send throughput: {metrics['send_throughput_msg_per_sec']:.2f} msg/s")
    print(f"Data throughput: {metrics['data_throughput_mb_per_sec']:.2f} MB/s")

    # Latency metrics
    print("\nLatency Metrics:")
    print(f"  Average: {metrics['avg_latency_ms']:.2f}ms")
    print(f"  P50: {metrics['p50_latency_ms']:.2f}ms")
    print(f"  P95: {metrics['p95_latency_ms']:.2f}ms")
    print(f"  P99: {metrics['p99_latency_ms']:.2f}ms")
    print(f"  Max: {metrics['max_latency_ms']:.2f}ms")

    # Reliability metrics
    print("\nReliability:")
    print(f"  Success rate: {metrics['success_rate']:.4f}")
    print(f"  Error rate: {metrics['error_rate']:.4f}")

    # Message size analysis
    print("\nMessage Size Analysis:")
    print(f"  Average: {metrics['avg_message_size_bytes']:.0f} bytes")
    print(f"  Median: {metrics['median_message_size_bytes']:.0f} bytes")
    print(f"  Range: {metrics['min_message_size_bytes']:.0f} - {metrics['max_message_size_bytes']:.0f} bytes")

    # Memory usage (if available)
    if metrics['peak_memory_mb'] > 0:
        print("\nMemory Usage:")
        print(f"  Peak: {metrics['peak_memory_mb']:.2f}MB")
        print(f"  Growth: {metrics['memory_growth_mb']:.2f}MB")

    # Batch efficiency
    print("\nBatch Efficiency:")
    print(f"  Messages per poll: {metrics['messages_per_poll']:.2f}")
    print(f"  Buffer full rate: {metrics['buffer_full_rate']:.3f}")
    print(f"  Total polls: {metrics['poll_count']}")

    # Per-topic breakdown (if multiple topics)
    if len(metrics['topic_metrics']) > 1:
        print("\nPer-Topic Metrics:")
        for topic, topic_metrics in metrics['topic_metrics'].items():
            print(f"  {topic}: {topic_metrics['messages_delivered']} msgs, "
                  f"{topic_metrics['success_rate']:.3f} success rate, "
                  f"{topic_metrics['avg_latency_ms']:.2f}ms avg latency")

    # Per-partition breakdown (if multiple partitions)
    if len(metrics['partition_metrics']) > 1:
        print("\nPer-Partition Metrics:")
        for partition, partition_metrics in metrics['partition_metrics'].items():
            print(f"  {partition}: {partition_metrics['messages_delivered']} msgs, "
                  f"{partition_metrics['success_rate']:.3f} success rate")

    # Validation results
    print(f"\nValidation: {'PASS' if is_valid else 'FAIL'}")
    if violations:
        print("Violations:")
        for violation in violations:
            print(f"  - {violation}")

    print("=" * 55)
