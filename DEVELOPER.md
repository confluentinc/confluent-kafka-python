# Developer Notes

This document provides information useful to developers working on confluent-kafka-python.

## Development Environment Setup

### Prerequisites

- Python 3.7 or higher
- Git
- librdkafka (for Kafka functionality)

### Setup Steps

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
See the main README.md for platform-specific installation instructions

If librdkafka is installed in a non-standard location provide the include and library directories with:

```bash
C_INCLUDE_PATH=/path/to/include LIBRARY_PATH=/path/to/lib python -m build
```

4. **Install Dependencies**
   ```bash
   pip3 install -e .[dev,tests,docs]
   ```

   This will also build the wheel be default. Alternatively you can build the bundle independently with:

   ```bash
   python3 -m build
   ```

5. **Verify Setup**
   ```bash
   python3 -c "import confluent_kafka; print('Setup successful!')"
   ```

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

## Unasync -- maintaining sync versions of async code

```bash
python3 tools/unasync.py

# Run the script with the --check flag to ensure the sync code is up to date
python3 tools/unasync.py --check
```

If you make any changes to the async code (in `src/confluent_kafka/schema_registry/_async` and `tests/integration/schema_registry/_async`), you **must** run this script to generate the sync counter parts (in `src/confluent_kafka/schema_registry/_sync` and `tests/integration/schema_registry/_sync`). Otherwise, this script will be run in CI with the --check flag and fail the build.


## Tests


See [tests/README.md](tests/README.md) for instructions on how to run tests.
