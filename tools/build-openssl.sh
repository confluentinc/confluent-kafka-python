#!/bin/bash
#

# Builds and installs OpenSSL for later use by build-manylinux.sh,
# bootstrap-librdkafka.sh, etc.

# NOTE: Keep this updated to make sure we always build the latest
#       version of OpenSSL in the 1.0 release train.
OPENSSL_VERSION=1.0.2l

PREFIX=$1
if [[ -z $PREFIX ]]; then
    echo "Usage: $0 <installation-prefix>"
fi

set -ex

echo "# Building OpenSSL ${OPENSSL_VERSION}"

rm -rf build-openssl
mkdir -p build-openssl
pushd build-openssl
curl -s -l https://www.openssl.org/source/openssl-${OPENSSL_VERSION}.tar.gz | \
    tar -xz --strip-components=1 -f -
./config --prefix=${PREFIX} zlib no-krb5 zlib shared
echo "## building openssl"
make 2>&1 | tail -50
echo "## testing openssl"
make test 2>&1 | tail -50
echo "## installing openssl"
make install 2>&1 | tail -50
popd



