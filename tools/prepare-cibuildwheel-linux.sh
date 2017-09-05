#!/bin/bash
#

# cibuildwheel builder for Linux

echo "# Installing basic system dependencies"
yum install -y zlib-devel gcc-c++

# Build OpenSSL
$(dirname $0)/build-openssl.sh /usr

echo "# Building librdkafka ${LIBRDKAFKA_VERSION}"
$(dirname $0)/bootstrap-librdkafka.sh ${LIBRDKAFKA_VERSION} /usr

