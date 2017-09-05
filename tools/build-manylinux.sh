#!/bin/bash
#
#
# Builds autonomous Python packages including all dependencies
# using the excellent manylinux docker images and the equally awesome
# auditwheel tool.
#
# This script should be run in a docker image where the confluent-kafka-python
# directory is mapped as /io .
#
# Usage on host:
#  tools/build-manylinux.sh <librdkafka_tag>
#
# Usage in container:
#  docker run -t -v $(pwd):/io quay.io/pypa/manylinux1_x86_64:latest  /io/tools/build-manylinux.sh <librdkafka_tag>

LIBRDKAFKA_VERSION=$1

if [[ -z "$LIBRDKAFKA_VERSION" ]]; then
    echo "Usage: $0 <librdkafka_tag>"
    exit 1
fi

set -ex

if [[ ! -f /.dockerenv ]]; then
    #
    # Running on host, fire up a docker container a run it.
    #

    if [[ ! -f tools/$(basename $0) ]]; then
        echo "Must be called from confluent-kafka-python root directory"
        exit 1
    fi

    docker run -t -v $(pwd):/io quay.io/pypa/manylinux1_x86_64:latest  /io/tools/build-manylinux.sh "$LIBRDKAFKA_VERSION"

    exit $?
fi


#
# Running in container
#

echo "# Installing basic system dependencies"
yum install -y zlib-devel gcc-c++

# Build OpenSSL
$(dirname $0)/build-openssl.sh /usr

echo "# Building librdkafka ${LIBRDKAFKA_VERSION}"
$(dirname $0)/bootstrap-librdkafka.sh --require-ssl ${LIBRDKAFKA_VERSION} /usr

# Compile wheels
echo "# Compile"
for PYBIN in /opt/python/*/bin; do
    echo "## Compiling $PYBIN"
    CFLAGS="-Werror -Wno-strict-aliasing -Wno-parentheses" \
          "${PYBIN}/pip" wheel /io/ -w unrepaired-wheelhouse/
done

# Bundle external shared libraries into the wheels
echo "# auditwheel repair"
mkdir -p /io/wheelhouse
for whl in unrepaired-wheelhouse/*.whl; do
    echo "## Repairing $whl"
    auditwheel repair "$whl" -w /io/wheelhouse
done

echo "# Repaired wheels"
for whl in /io/wheelhouse/*.whl; do
    echo "## Repaired wheel $whl"
    auditwheel show "$whl"
done

# Install packages and test
echo "# Installing wheels"
for PYBIN in /opt/python/*/bin/; do
    echo "## Installing $PYBIN"
    "${PYBIN}/pip" install confluent_kafka --no-index -f /io/wheelhouse
    "${PYBIN}/python" -c 'import confluent_kafka; print(confluent_kafka.libversion())'
    echo "## Uninstalling $PYBIN"
    "${PYBIN}/pip" uninstall -y confluent_kafka
done



