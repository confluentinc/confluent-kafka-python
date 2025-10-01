# Developer Notes

This document provides information useful to developers working on confluent-kafka-python.

## Development Environment Setup

### Prerequisites

- Python 3.7 or higher
- Git
- librdkafka (for Kafka functionality)

### Quick start (editable install)

<!-- markdownlint-disable MD029 -->

1. **Fork and Clone**

   ```bash
   git clone https://github.com/your-username/confluent-kafka-python.git
   cd confluent-kafka-python
   ```

2. **Create Virtual Environment**
   ```bash
   python3 -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

**Note**: On Windows the variables for Visual Studio are named INCLUDE and LIB

3. **Install librdkafka** (if not already installed)

See the main README.md for platform-specific installation instructions.

If librdkafka is installed in a non-standard location provide the include and library directories with:

```bash
C_INCLUDE_PATH=/path/to/include LIBRARY_PATH=/path/to/lib python -m build
```

4. **Install confluent-kafka-python (editable) with dev/test/docs extras**

   ```bash
   pip3 install -e .[dev,tests,docs]
   ```

   Alternatively you can build the bundle independently with:

   ```bash
   python3 -m build
   ```

5. **Verify Setup**

   ```bash
   python3 -c "import confluent_kafka; print('Setup successful!')"
   ```

<!-- markdownlint-enable MD029 -->

## Project layout

- `src/confluent_kafka/` — core sync client APIs
- `src/confluent_kafka/aio/` — AsyncIO Producer/Consumer (first-class asyncio, not generated)
- `src/confluent_kafka/schema_registry/` — Schema Registry clients and serdes
- `tests/` — unit and integration tests (including async producer tests)
- `examples/` — runnable samples (includes asyncio example)
- `tools/unasync.py` — SR-only sync code generation from async sources

## Generate Documentation

Install docs dependencies:

```bash
pip3 install .[docs]
```

Build HTML docs:

```bash
make docs
```

Documentation will be generated in `docs/_build/`.

or:

```bash
python3 setup.py build_sphinx
```

Documentation will be generated in  `build/sphinx/html`.

## Unasync — maintaining sync versions of async code (Schema Registry only)

```bash
python3 tools/unasync.py

# Run the script with the --check flag to ensure the sync code is up to date
python3 tools/unasync.py --check
```

If you make any changes to the async code (in `src/confluent_kafka/schema_registry/_async` and `tests/integration/schema_registry/_async`), you **must** run this script to generate the sync counterparts (in `src/confluent_kafka/schema_registry/_sync` and `tests/integration/schema_registry/_sync`). Otherwise, this script will be run in CI with the `--check` flag and fail the build.

Note: The AsyncIO Producer/Consumer under `src/confluent_kafka/aio/` are first-class asyncio implementations and are not generated using `unasync`.

## AsyncIO Producer development (AIOProducer)

Source:

- `src/confluent_kafka/aio/producer/_AIOProducer.py` (public async API)
- Internal modules in `src/confluent_kafka/aio/producer/` and helpers in `src/confluent_kafka/aio/_common.py`

For a complete usage example, see [`examples/asyncio_example.py`](examples/asyncio_example.py).

**Architecture:** See the [AIOProducer Architecture Overview](aio_producer_simple_diagram.md) for component design and data flow details.

Design guidelines:

- Offload blocking librdkafka calls using `_common.async_call` with a `ThreadPoolExecutor`.
- Wrap common callbacks (`error_cb`, `throttle_cb`, `stats_cb`, `oauth_cb`, `logger`) onto the event loop using `_common.wrap_common_callbacks`.
- Batched async produce flushes on batch size or timeout; per-message headers are not supported in batched async produce.
- Ensure `await producer.flush()` and `await producer.close()` are called in shutdown paths to stop background tasks.

Performance considerations:

- AsyncIO producer adds up to 50% overhead compared to fire-and-forget sync produce
  in highest possible throughput cases. Normal application operations see no significant overhead.
- Batched async produce is more efficient than individual awaited produce calls.
  This is caused by thread contention while librdkafka queue locking mechanisms
  are in play.
- For maximum throughput without event loop integration, use the synchronous Producer.
- The official implementation outperforms custom AsyncIO wrappers due to optimized thread pool management.
- AsyncIO Schema Registry client maintains 100% interface parity with sync client - no unexpected gotchas or limitations.

Event loop safety:

- Do not block the event loop. Any new blocking operations must be routed using `_common.async_call`.
- When exposing callback entry points, use `asyncio.run_coroutine_threadsafe` to re-enter the loop if invoked from non-loop threads.

## Running tests

Unit tests:

```bash
pytest -q
```

Run async producer tests only:

```bash
pytest -q tests/test_AIOProducer.py
pytest -q -k AIOProducer
```

Integration tests (may require local/CI Kafka cluster; see tests/README.md):

```bash
pytest -q tests/integration
```

## Tests

See [tests/README.md](tests/README.md) for instructions on how to run tests.

## Linting & formatting (suggested)

- Python: `black .` and `flake8` (or `ruff`) per project configuration
- Markdown: `markdownlint '**/*.md'`

## Documentation build

See “Generate Documentation” above; ensure examples and code blocks compile where applicable.

## Contributing

- Use topic branches (e.g., `feature/asyncio-improvements`).
- Keep edits focused; include tests where possible.
- Follow existing code style and add type hints to public APIs where feasible.

## Troubleshooting

- Build errors related to librdkafka: ensure headers and libraries are discoverable; see “Install librdkafka” above for `C_INCLUDE_PATH` and `LIBRARY_PATH`.
- Async tests hanging: check event loop usage and that `await producer.close()` is called to stop background tasks.
