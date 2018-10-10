#!/bin/bash -eu
#
#
# Tests wheels work on osx
#
# $tools/test-osx.sh [ wheelhouse ]

if [[ ! -f tools/$(basename $0) ]]; then
    echo "Must be called from confluent-kafka-python root directory"
    exit 1
fi

# clean up pytest_cache if it exists 
if [[ -f .pytest_cache ]]; then
	rm .pytest_cache
fi 

WHEELHOUSE=${1-wheelhouse} 

if [[ ! -d $WHEELHOUSE ]]; then
    echo "$WHEELHOUSE does not exist"
    exit 1
fi

# Make sure pip itself is up to date
pip install -U pip
hash -r # let go of previous 'pip'

# Install modules
# TODO: revisit to avoid hardcoding dependencies
pip install "futures;python_version=='2.7'" "enum34;python_version=='2.7'"

pip install confluent_kafka --no-cache-dir --no-index -f $WHEELHOUSE

# Pytest relies on a new version of six; later versions of pip fail to remove older versions gracefully
# https://github.com/pypa/pip/issues/5247
pip install pytest pytest-timeout --ignore-installed six

# Verify that OpenSSL and zlib are properly linked
python -c '
import confluent_kafka

p = confluent_kafka.Producer({"ssl.cipher.suites":"DEFAULT",
                             "compression.codec":"gzip"})
'

# Bump out of the source root and execute tests
(cd .. && pytest -v --timeout 20 --ignore=tmp-build --import-mode append --ignore=avro confluent-kafka-python/tests)
