# Ducktape Tests

Ducktape-based tests for the Confluent Kafka Python client with comprehensive performance metrics.

## Prerequisites

- `pip install ducktape confluent-kafka psutil`
- Kafka running on `localhost:9092` (PLAINTEXT listener - ducktape tests use the simple port)
- Schema Registry running on `localhost:8081` (uses `host.docker.internal:29092` for Kafka connection)

## Running Tests

```bash
# Run all tests with integrated performance metrics
./tests/ducktape/run_ducktape_test.py

# Run all tests in a file
./tests/ducktape/run_ducktape_test.py producer

# Run a specific test with metrics
./tests/ducktape/run_ducktape_test.py producer SimpleProducerTest.test_basic_produce
```

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

Performance bounds are loaded from an environment-based JSON config file. By default, it loads `producer_benchmark_bounds.json`, but you can override this with the `BENCHMARK_BOUNDS_CONFIG` environment variable.

### Environment-Based Configuration

The bounds configuration supports different environments with different performance thresholds:

```json
{
  "_comment": "Performance bounds for benchmark tests by environment",
  "local": {
    "_comment": "Default bounds for local development - more relaxed thresholds",
    "min_throughput_msg_per_sec": 1000.0,
    "max_p95_latency_ms": 2000.0,
    "max_error_rate": 0.02,
    "min_success_rate": 0.98,
    "max_p99_latency_ms": 3000.0,
    "max_memory_growth_mb": 1000.0,
    "max_buffer_full_rate": 0.05,
    "min_messages_per_poll": 10.0
  },
  "ci": {
    "_comment": "Stricter bounds for CI environment - production-like requirements",
    "min_throughput_msg_per_sec": 1500.0,
    "max_p95_latency_ms": 1500.0,
    "max_error_rate": 0.01,
    "min_success_rate": 0.99,
    "max_p99_latency_ms": 2500.0,
    "max_memory_growth_mb": 700.0,
    "max_buffer_full_rate": 0.03,
    "min_messages_per_poll": 15.0
  },
  "_default_environment": "local"
}
```

### Environment Selection

- **BENCHMARK_ENVIRONMENT**: Selects which environment bounds to use (`local`, `ci`, etc.)
- **Default**: Uses "local" environment if not specified
- **CI**: Automatically uses "ci" environment in CI pipelines

Usage:
```bash
# Use default environment (local)
./run_ducktape_test.py

# Explicitly use local environment
BENCHMARK_ENVIRONMENT=local ./run_ducktape_test.py

# Use CI environment with stricter bounds
BENCHMARK_ENVIRONMENT=ci ./run_ducktape_test.py

# Use different config file entirely
BENCHMARK_BOUNDS_CONFIG=custom_bounds.json ./run_ducktape_test.py
```

```python
from producer_benchmark_metrics import MetricsBounds

# Loads from BENCHMARK_BOUNDS_CONFIG env var, or producer_benchmark_bounds.json if not set
bounds = MetricsBounds()

# Or load from a specific config file
bounds = MetricsBounds.from_config_file("my_bounds.json")
```
