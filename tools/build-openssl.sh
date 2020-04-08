#!/bin/bash
#

# Builds and installs OpenSSL for later use by build-manylinux.sh,
# bootstrap-librdkafka.sh, etc.

# NOTE: Keep this updated to make sure we always build the latest
#       version of OpenSSL in the 1.0 release train.
OPENSSL_VERSION_SUFFIX=u

PREFIX=$1
if [[ -z $PREFIX ]]; then
    echo "Usage: $0 <installation-prefix>"
fi

set -ex
set -o pipefail

echo "# Building OpenSSL ${OPENSSL_VERSION_SUFFIX}"

if ! grep -q "^VERSION=${OPENSSL_VERSION_SUFFIX}$" build-openssl/Makefile ; then
    echo "No usable build-openssl directory: downloading ${OPENSSL_VERSION_SUFFIX}"
    rm -rf build-openssl
    mkdir -p build-openssl
    pushd build-openssl
    curl -s -l https://www.openssl.org/source/old/1.0.2/openssl-1.0.2${OPENSSL_VERSION_SUFFIX}.tar.gz | \
        tar -xz --strip-components=1 -f -
else
    echo "Reusing existing build-openssl directory"
    pushd build-openssl
fi

./config --prefix=${PREFIX} zlib no-krb5 zlib shared
echo "## building openssl"
if ! make -j 2>&1 | tail -20 ; then
    echo "## Make failed, cleaning up and retrying"
    time make clean 2>&1 | tail -20
    rm -f test/PASSED
    echo "## building openssl (retry)"
    time make -j 2>&1 | tail -20
fi

if [[ ! -f test/PASSED ]]; then
    echo "## testing openssl"
    time make test 2>&1 | tail -20
    touch test/PASSED
fi
echo "## installing openssl"
time make install 2>&1 | tail -20
popd



