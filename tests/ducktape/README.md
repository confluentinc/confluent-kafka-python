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
- **Automatic Bounds Validation**: Performance assertions against realistic thresholds