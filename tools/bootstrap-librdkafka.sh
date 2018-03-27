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
PREFIXDIR=$2

set -ex
set -o pipefail

if [[ -z "$VERSION" ]]; then
    echo "Usage: $0 --require-ssl <librdkafka-version> [<install-dir>]" 1>&2
    exit 1
fi

if [[ -z "$PREFIXDIR" ]]; then
    PREFIXDIR=tmp-build
fi

if [[ $PREFIXDIR != /* ]]; then
    PREFIXDIR="$PWD/$PREFIXDIR"
fi

mkdir -p "$PREFIXDIR/librdkafka"
pushd "$PREFIXDIR/librdkafka"

test -f configure ||
curl -q -L "https://github.com/edenhill/librdkafka/archive/${VERSION}.tar.gz" | \
    tar -xz --strip-components=1 -f -

./configure --clean
make clean
./configure --prefix="$PREFIXDIR"

if [[ $REQUIRE_SSL == 1 ]]; then
    grep '^#define WITH_SSL 1$' config.h || \
        (echo "ERROR: OpenSSL support required" ; cat config.log config.h ; exit 1)
fi

make -j
make install
popd

