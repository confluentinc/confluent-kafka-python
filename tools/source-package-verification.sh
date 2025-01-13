#!/bin/bash
#
#
# Source Package Verification
#
set -e

pip install -r requirements/requirements-tests-install.txt
pip install -U build

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
  python3 -m pip install -e .
  python -m pytest --cov confluent_kafka --cov-report term --cov-report html --cov-report xml \
      --cov-branch --junitxml=test-report.xml tests/ --timeout 1200 --ignore=dest
  exit 0
fi

python3 -m pip install .

if [[ $OS_NAME == linux && $ARCH == x64 ]]; then
    if [[ -z $TEST_CONSUMER_GROUP_PROTOCOL ]]; then
        flake8 --exclude ./_venv,*_pb2.py
        pip install -r requirements/requirements-docs.txt
        make docs
    fi
    python -m pytest --timeout 1200 --ignore=dest
else
    python -m pytest --timeout 1200 --ignore=dest --ignore=tests/integration
fi
