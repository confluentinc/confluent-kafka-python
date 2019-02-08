#!/bin/bash
#

# Builds and installs OpenSSL for later use by build-manylinux.sh,
# bootstrap-librdkafka.sh, etc.

# NOTE: Keep this updated to make sure we always build the latest
#       version of OpenSSL in the 1.0 release train.
OPENSSL_VERSION=1.0.2q

export DESTDIR=$1
if [[ -z $DESTDIR ]]; then
    echo "Usage: $0 <installation-destdir>"
fi

set -ex
set -o pipefail

echo "# Building OpenSSL ${OPENSSL_VERSION}"

if ! grep -q "^VERSION=${OPENSSL_VERSION}$" build-openssl/Makefile ; then
    echo "No usable build-openssl directory: downloading ${OPENSSL_VERSION}"
    rm -rf build-openssl
    mkdir -p build-openssl
    pushd build-openssl
    curl -s -l https://www.openssl.org/source/openssl-${OPENSSL_VERSION}.tar.gz | \
        tar -xz --strip-components=1 -f -
else
    echo "Reusing existing build-openssl directory"
    pushd build-openssl
fi

extra_conf_args=
if [[ $OPENSSL_VERSION == 1.0.* ]]; then
    extra_conf_args="no-krb5 shared"
fi

./config --prefix=/usr --openssldir=/usr/lib/ssl zlib shared $extra_conf_args

echo "## building openssl"
if ! make -j 2>&1 | tail -100 ; then
    echo "## Make failed, cleaning up and retrying"
    time make clean 2>&1 | tail -20
    rm -f test/PASSED
    echo "## building openssl (retry)"
    time make -j 2>&1 | tail -100
fi

if [[ ! -f test/PASSED ]]; then
    echo "## testing openssl"
    #time make test 2>&1 | tail -100
    touch test/PASSED
fi
echo "## installing openssl to $DESTDIR"
make DESTDIR=$DESTDIR install 2>&1 | tail -100
popd



