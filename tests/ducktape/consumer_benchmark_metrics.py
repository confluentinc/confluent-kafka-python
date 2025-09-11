"""
Consumer benchmark metrics collection and validation for Kafka performance testing.

Implements consumer-specific metrics tracking including poll latencies,
consumption rates, per-topic breakdowns, and reliability analysis.
"""
import time
import statistics
from typing import List, Dict, Any
from collections import defaultdict


class ConsumerMetricsCollector:
    """Collects comprehensive performance metrics for consumer testing."""

    def __init__(self):
        # Basic timing
        self.start_time = None
        self.end_time = None

        # Consumer metrics
        self.messages_consumed = 0
        self.poll_attempts = 0
        self.poll_timeouts = 0
        self.poll_errors = 0
        self.poll_latencies = []  # in milliseconds
        self.error_messages = []

        # Data tracking
        self.total_bytes = 0
        self.message_sizes = []

        # Per-topic metrics
        self.topic_metrics = defaultdict(lambda: {
            'consumed': 0, 'poll_latencies': [], 'timeouts': 0, 'errors': 0, 'bytes': 0
        })

        # Consumer-specific offset tracking
        self.offsets_consumed = defaultdict(dict)  # topic -> partition -> last_offset
        self.partition_lag = defaultdict(dict)     # topic -> partition -> lag

    def start(self):
        """Start metrics collection"""
        self.start_time = time.time()

    def record_poll_attempt(self, poll_latency_ms: float):
        """Record a consumer poll attempt with its latency"""
        self.poll_attempts += 1
        self.poll_latencies.append(poll_latency_ms)

    def record_timeout(self, topic: str = "unknown"):
        """Record a consumer poll timeout"""
        self.poll_timeouts += 1
        self.topic_metrics[topic]['timeouts'] += 1

    def record_error(self, error_msg: str, topic: str = "unknown"):
        """Record a consumer error"""
        self.poll_errors += 1
        self.error_messages.append(error_msg)
        self.topic_metrics[topic]['errors'] += 1

    def record_consumed(self, message_size: int, topic: str, partition: int,
                       offset: int, poll_latency_ms: float):
        """Record a successfully consumed message"""
        self.messages_consumed += 1
        self.total_bytes += message_size
        self.message_sizes.append(message_size)

        # Track offsets
        self.offsets_consumed[topic][partition] = offset

        # Track per-topic metrics
        self.topic_metrics[topic]['consumed'] += 1
        self.topic_metrics[topic]['bytes'] += message_size
        self.topic_metrics[topic]['poll_latencies'].append(poll_latency_ms)

    def finalize(self):
        """Finalize metrics collection"""
        self.end_time = time.time()

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
        poll_rate = self.poll_attempts / duration if duration > 0 else 0

        # Poll metrics
        timeout_rate = self.poll_timeouts / self.poll_attempts if self.poll_attempts > 0 else 0
        poll_error_rate = self.poll_errors / self.poll_attempts if self.poll_attempts > 0 else 0
        poll_success_rate = (self.poll_attempts - self.poll_timeouts - self.poll_errors) / self.poll_attempts if self.poll_attempts > 0 else 0

        # Poll latency analysis
        if self.poll_latencies:
            avg_poll_latency = statistics.mean(self.poll_latencies)
            p50_poll_latency = statistics.median(self.poll_latencies)
            max_poll_latency = max(self.poll_latencies)

            try:
                quantiles = statistics.quantiles(self.poll_latencies, n=100)
                p95_poll_latency = quantiles[94]  # 95th percentile
                p99_poll_latency = quantiles[98]  # 99th percentile
            except statistics.StatisticsError:
                p95_poll_latency = self._percentile(self.poll_latencies, 95)
                p99_poll_latency = self._percentile(self.poll_latencies, 99)
        else:
            avg_poll_latency = p50_poll_latency = p95_poll_latency = p99_poll_latency = max_poll_latency = 0

        # Per-topic summary
        topic_summary = {}
        for topic, metrics in self.topic_metrics.items():
            if metrics['consumed'] > 0:
                topic_rate = metrics['consumed'] / duration if duration > 0 else 0
                topic_data_mb = (metrics['bytes'] / (1024 * 1024)) / duration if duration > 0 else 0

                topic_summary[topic] = {
                    'messages_consumed': metrics['consumed'],
                    'consumption_rate_msg_per_sec': topic_rate,
                    'data_throughput_mb_per_sec': topic_data_mb,
                    'timeouts': metrics['timeouts'],
                    'errors': metrics['errors']
                }

        # Base summary
        base_summary = {
            'start_time': self.start_time,
            'end_time': self.end_time,
            'duration_seconds': duration
        }

        # Consumer metrics
        consumer_metrics = {
            'messages_consumed': self.messages_consumed,
            'consumption_rate_msg_per_sec': consumption_rate,
            'data_throughput_mb_per_sec': (self.total_bytes / (1024 * 1024)) / duration if duration > 0 else 0,
            'avg_latency_ms': avg_poll_latency,
            'p50_latency_ms': p50_poll_latency,
            'p95_latency_ms': p95_poll_latency,
            'p99_latency_ms': p99_poll_latency,
            'max_latency_ms': max_poll_latency,
            'error_rate': poll_error_rate,
            'success_rate': poll_success_rate,
            'total_bytes': self.total_bytes,
            'topic_metrics': topic_summary
        }

        base_summary.update(consumer_metrics)
        return base_summary


class ConsumerMetricsBounds:
    """Performance bounds for consumer metrics validation"""

    def __init__(self,
                 min_consumption_rate: float = 1.0,
                 max_avg_latency_ms: float = 5000.0,
                 max_p95_latency_ms: float = 10000.0,
                 min_success_rate: float = 0.95,
                 max_error_rate: float = 0.05):
        self.min_consumption_rate = min_consumption_rate
        self.max_avg_latency_ms = max_avg_latency_ms
        self.max_p95_latency_ms = max_p95_latency_ms
        self.min_success_rate = min_success_rate
        self.max_error_rate = max_error_rate


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

    is_valid = len(violations) == 0
    return is_valid, violations


def print_consumer_metrics_report(metrics: Dict[str, Any], is_valid: bool, violations: List[str]):
    """Print simplified consumer metrics report (similar to producer format)"""
    print("\n=== Consumer Performance Report ===")

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
    print(f"  Success rate: {metrics.get('success_rate', 0):.4f}")
    print(f"  Error rate: {metrics.get('error_rate', 0):.4f}")

    # Validation
    print("\nValidation:")
    if is_valid:
        print("  All performance bounds satisfied")
    else:
        print("  Performance bounds violations:")
        for violation in violations:
            print(f"    - {violation}")

    print("=" * 50)
