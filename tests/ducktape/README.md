# Ducktape Producer Tests

Ducktape-based producer tests for the Confluent Kafka Python client with comprehensive performance metrics.

## Prerequisites

- `pip install ducktape confluent-kafka psutil`
- Kafka running on `localhost:9092`

## Running Tests

```bash
# Run all tests with integrated performance metrics
./tests/ducktape/run_ducktape_test.py

# Run specific test with metrics
./tests/ducktape/run_ducktape_test.py SimpleProducerTest.test_basic_produce
```

## Test Cases

- **test_basic_produce**: Basic message production with integrated metrics tracking
- **test_produce_multiple_batches**: Parameterized tests (2s, 5s, 10s durations) with metrics
- **test_produce_with_compression**: Matrix tests (none, gzip, snappy) with compression-aware metrics

## Integrated Performance Metrics Features

Every test automatically includes:

- **Latency Tracking**: P50, P95, P99 percentiles with real-time calculation
- **Per-Topic/Partition Metrics**: Detailed breakdown by topic and partition
- **Memory Monitoring**: Peak memory usage and growth tracking with psutil
- **Batch Efficiency**: Messages per poll and buffer utilization analysis
- **Throughput Validation**: Messages/sec and MB/sec with configurable bounds checking
- **Comprehensive Reporting**: Detailed performance reports with pass/fail validation
- **Automatic Bounds Validation**: Performance assertions against configurable thresholds

## Configuration

Performance bounds are loaded from a JSON config file. By default, it loads `benchmark_bounds.json`, but you can override this with the `BENCHMARK_BOUNDS_CONFIG` environment variable:

```json
{
  "min_throughput_msg_per_sec": 1500.0,
  "max_p95_latency_ms": 1500.0,
  "max_error_rate": 0.01,
  "min_success_rate": 0.99,
  "max_p99_latency_ms": 2500.0,
  "max_memory_growth_mb": 600.0,
  "max_buffer_full_rate": 0.03,
  "min_messages_per_poll": 15.0
}
```

Usage:
```bash
# Use default config file
./run_ducktape_test.py

# Use different configs for different environments
BENCHMARK_BOUNDS_CONFIG=ci_bounds.json ./run_ducktape_test.py
BENCHMARK_BOUNDS_CONFIG=production_bounds.json ./run_ducktape_test.py
```

```python
from benchmark_metrics import MetricsBounds

# Loads from BENCHMARK_BOUNDS_CONFIG env var, or benchmark_bounds.json if not set
bounds = MetricsBounds()

# Or load from a specific config file
bounds = MetricsBounds.from_config_file("my_bounds.json")
```