#!/bin/bash
#
#
# Downloads, builds and installs librdkafka into <install-dir>
#
# An existing build-dir with the same version can be reused.

if [[ $1 == "--require-ssl" ]]; then
    REQUIRE_SSL=1
    shift
fi

VERSION=$1
INSTALLDIR=$2
BUILDDIR="$3"

if [ -z $BUILDDIR ]; then
    BUILDDIR=$PWD/tmp-build
fi

if [[ -z "$VERSION" ]]; then
    echo "Usage: $0 [--require-ssl] <librdkafka-version> [<install-dir> [<build-dir>]]" 1>&2
    exit 1
fi

set -ex
set -o pipefail

if [[ $INSTALLDIR != /* ]]; then
    INSTALLDIR="$PWD/$INSTALLDIR"
fi

if [[ -f $BUILDDIR/.stamp ]]; then
    prevver=$(cat $BUILDDIR/.stamp)
    if [[ $prevver != $VERSION ]]; then
        echo "$0: ERROR: Existing build directory $BUILDDIR contains $prevver, not $VERSION"
        exit 1
    fi
    echo "$0: Reusing build directory $BUILDDIR for version $VERSION"
    pushd "$BUILDDIR/librdkafka"

    if [[ $REQUIRE_SSL == 1 ]]; then
        grep '^#define WITH_SSL 1$' config.h || \
            (echo "ERROR: OpenSSL support required" ; cat config.log config.h ; exit 1)
    fi

else

    mkdir -p "$BUILDDIR/librdkafka"
    pushd "$BUILDDIR/librdkafka"

    test -f configure ||
        curl -q -L "https://github.com/edenhill/librdkafka/archive/${VERSION}.tar.gz" | \
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
fi


examples/rdkafka_example -X builtin.features

if [[ $INSTALLDIR == /usr && $(whoami) != root ]]; then
    sudo make install
else
    make install
fi

popd
echo "$VERSION" > $BUILDDIR/.stamp

