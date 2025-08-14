# Ducktape Producer Tests

Ducktape-based producer tests for the Confluent Kafka Python client.

## Prerequisites

- `pip install ducktape confluent-kafka`
- Kafka running on `localhost:9092`

## Running Tests

```bash
# Run all tests
./tests/ducktape/run_ducktape_test.py

# Run specific test
./tests/ducktape/run_ducktape_test.py SimpleProducerTest.test_basic_produce
```

## Test Cases

- **test_basic_produce**: Basic message production with callbacks
- **test_produce_multiple_batches**: Parameterized tests (5, 10, 50 messages)  
- **test_produce_with_compression**: Matrix tests (none, gzip, snappy)