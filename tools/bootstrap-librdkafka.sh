#!/bin/bash
#
#
# Downloads, builds and installs librdkafka into <install-dir>
#

if [[ $1 == "--require-ssl" ]]; then
    REQUIRE_SSL=1
    shift
fi

VERSION=$1
INSTALLDIR=$2
BUILDDIR=$PWD/tmp-build

set -ex
set -o pipefail

if [[ -z "$VERSION" ]]; then
    echo "Usage: $0 --require-ssl <librdkafka-version> [<install-dir>]" 1>&2
    exit 1
fi

if [[ $INSTALLDIR != /* ]]; then
    INSTALLDIR="$PWD/$INSTALLDIR"
fi

mkdir -p "$BUILDDIR/librdkafka"
pushd "$BUILDDIR/librdkafka"

test -f configure ||
curl -q -L "https://github.com/confluentinc/librdkafka/archive/refs/tags/${VERSION}.tar.gz" | \
    tar -xz --strip-components=1 -f -

./configure --clean
make clean

if [[ $OSTYPE == "linux"* ]]; then
  EXTRA_OPTS="--disable-gssapi"
fi

./configure --enable-static --install-deps --source-deps-only $EXTRA_OPTS --prefix="$INSTALLDIR"

if [[ $REQUIRE_SSL == 1 ]]; then
    grep '^#define WITH_SSL 1$' config.h || \
        (echo "ERROR: OpenSSL support required" ; cat config.log config.h ; exit 1)
fi

make -j
examples/rdkafka_example -X builtin.features

if [[ $INSTALLDIR == /usr && $(whoami) != root ]]; then
    sudo make install
else
    make install
fi
popd

