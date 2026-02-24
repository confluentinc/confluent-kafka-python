#!/bin/bash
#
#
# Source Package Verification
#
set -e

uv pip install -r requirements/requirements-tests-install.txt
uv pip install -U build

# Cache trivup Apache Kafka versions

BASE=$PWD
for version in 3.9.0 4.0.0; do
    artifact pull project kafka_2.13-$version.tgz || true
    if [[ ! -f  ./kafka_2.13-$version.tgz ]]; then
        wget -O ./kafka_2.13-$version.tgz "https://archive.apache.org/dist/kafka/$version/kafka_2.13-$version.tgz"
        artifact push project ./kafka_2.13-$version.tgz || true
    fi
    mkdir -p tmp-KafkaCluster/KafkaCluster/KafkaBrokerApp/kafka/$version
    (cd tmp-KafkaCluster/KafkaCluster/KafkaBrokerApp/kafka/$version && \
     tar -xvf $BASE/kafka_2.13-$version.tgz --strip-components=1)
done

lib_dir=dest/runtimes/$OS_NAME-$ARCH/native
tools/wheels/install-librdkafka.sh "${LIBRDKAFKA_VERSION#v}" dest
export CFLAGS="$CFLAGS -I${PWD}/dest/build/native/include"
export LDFLAGS="$LDFLAGS -L${PWD}/${lib_dir}"
export LD_LIBRARY_PATH="$LD_LIBRARY_PATH:$PWD/$lib_dir"
export DYLD_LIBRARY_PATH="$DYLD_LIBRARY_PATH:$PWD/$lib_dir"

if [[ $RUN_COVERAGE == true ]]; then
  echo "Running tests with coverage"
  # Install source with editable flag (-e) so that SonarQube can parse the coverage report.
      # Otherwise, the report shows source files located in site-packages, which SonarQube cannot find.
      # Example: ".tox/cover/lib/python3.11/site-packages/confluent_kafka/__init__.py"
      #   instead of "src/confluent_kafka/__init__.py"
  uv pip install -e .
  python -m pytest --cov confluent_kafka --cov-report term --cov-report html --cov-report xml \
      --cov-branch --junitxml=test-report.xml tests/ --timeout 1200 --ignore=dest
  exit 0
fi

echo "Checking for uncommitted changes in generated _sync directories"
python3 tools/unasync.py --check

uv pip install .

echo "Checking if KafkaError stub has up to date error codes"
python3 tools/generate_kafka_error_stub_codes.py --check

if [[ $OS_NAME == linux && $ARCH == x64 ]]; then
    if [[ -z $TEST_CONSUMER_GROUP_PROTOCOL ]]; then
        # Run these actions and tests only in this case
        echo "Checking code formatting ..."
        # Check all tracked files (Python and C)
        # Commenting out clang-format until consistent versions across platforms are available for dev and CI testing
        all_files=$(git ls-tree -r --name-only HEAD | egrep '\.(py)$')  # egrep '\.(py|c|h)$')
        # clang-format --version
        tools/style-format.sh $all_files || exit 1
        echo "Building documentation ..."
        flake8 --exclude ./_venv,*_pb2.py,./build

        echo "Running mypy type checking ..."
        python3.11 -m mypy src/confluent_kafka

        uv pip install -r requirements/requirements-docs.txt
        make docs

        echo "Testing extra dependencies ..."
        uv pip install ".[schema-registry,avro,json,protobuf]"
        python3 -c "
import importlib.metadata, sys
extras = set(importlib.metadata.metadata('confluent-kafka').get_all('Provides-Extra') or [])
required = {'schema-registry', 'schemaregistry', 'avro', 'json', 'protobuf', 'rules'}
missing = required - extras
if missing:
    print(f'Failing: package does not provide extras: {missing}', file=sys.stderr)
    sys.exit(1)
print(f'All required extras present: {required}')
"
    fi
    python -m pytest --timeout 1200 --ignore=dest
else
    python -m pytest --timeout 1200 --ignore=dest --ignore=tests/integration
fi
