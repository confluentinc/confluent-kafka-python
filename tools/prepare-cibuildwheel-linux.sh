#!/bin/bash
#

# cibuildwheel builder for Linux

LIBRDKAFKA_VERSION=$1

CIBW_MANYLINUX_X86_64_IMAGE="manylinux1"
CIBW_MANYLINUX_I686_IMAGE="manylinux1"

if [[ -z $LIBRDKAFKA_VERSION ]]; then
    echo "Usage: $0 <librdkafka-version/tag/gitref>"
    exit 1
fi

set -ex

echo "# Installing basic system dependencies"
yum install -y zlib-devel gcc-c++

echo "# Building librdkafka ${LIBRDKAFKA_VERSION}"
$(dirname $0)/bootstrap-librdkafka.sh --require-ssl ${LIBRDKAFKA_VERSION} /usr

