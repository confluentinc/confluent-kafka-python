#!/bin/bash
#
# TODO KIP-932: This script is temporary until share consumer support
# lands in a released librdkafka version.
#
# Build librdkafka from a git branch and install it into a NuGet-compatible
# directory layout that matches what install-librdkafka.sh produces.
#
# Usage: build-librdkafka-branch.sh <branch> <destdir>
#
#   branch   - git branch name, e.g. dev_kip-932_queues-for-kafka
#   destdir  - destination directory, e.g. dest
#
# Resulting layout (mirrors NuGet redist package):
#   <destdir>/build/native/include/librdkafka/rdkafka.h
#   <destdir>/runtimes/<os>-<arch>/native/librdkafka.{so.1,dylib}

set -ex

BRANCH="$1"
DEST="$2"

if [[ -z $BRANCH || -z $DEST ]]; then
    echo "Usage: $0 <branch> <destdir>"
    exit 1
fi

if [[ -f $DEST/build/native/include/librdkafka/rdkafka.h ]]; then
    echo "$0: librdkafka already built in $DEST"
    exit 0
fi

echo "$0: Building librdkafka branch '$BRANCH' into '$DEST'"

ARCH=${ARCH:-x64}
SRC=/tmp/librdkafka-${BRANCH}
INSTALL=$SRC/install

[[ -d "$DEST" ]] || mkdir -p "$DEST"
rm -rf "$SRC"

git clone --depth 1 --branch "$BRANCH" \
    https://github.com/confluentinc/librdkafka.git "$SRC"

if [[ $OSTYPE == linux* ]]; then
    sudo apt-get update -qq && sudo apt-get install -y -qq libssl-dev libsasl2-dev liblz4-dev libzstd-dev
elif [[ $OSTYPE == darwin* ]]; then
    # openssl@3 is keg-only in Homebrew (the system ships LibreSSL/Apple
    # crypto with no -lcrypto headers), so configure's compile-probe for
    # libcrypto silently fails unless we add brew's path. Same for zstd /
    # lz4 / pkg-config on a fresh runner.
    brew install pkg-config openssl@3 zstd lz4
    OPENSSL_PREFIX="$(brew --prefix openssl@3)"
    export PKG_CONFIG_PATH="$OPENSSL_PREFIX/lib/pkgconfig${PKG_CONFIG_PATH:+:$PKG_CONFIG_PATH}"
    export CPPFLAGS="-I$OPENSSL_PREFIX/include${CPPFLAGS:+ $CPPFLAGS}"
    export LDFLAGS="-L$OPENSSL_PREFIX/lib${LDFLAGS:+ $LDFLAGS}"
fi

pushd "$SRC"

# --enable-ssl/-lz4/-zstd convert mklove's silent "failed (disable)" into a
# loud configure error if a future regression breaks dep installation —
# otherwise we ship a wheel where sasl.oauthbearer.config etc. fail at
# runtime with _INVALID_ARG -186.
CONFIGURE_OPTS="--prefix=$INSTALL --enable-ssl --enable-lz4-ext --enable-zstd"
if [[ $OSTYPE == linux* ]]; then
    CONFIGURE_OPTS="$CONFIGURE_OPTS --disable-gssapi"
fi

# LIBRDKAFKA_SANITIZE=address builds an instrumented lib for the share-consumer
# ASAN pipeline. Keep the debug symbols mklove would otherwise strip so leak
# reports land on real source lines; normal wheel builds still strip them.
if [[ -n $LIBRDKAFKA_SANITIZE ]]; then
    export CFLAGS="-fsanitize=${LIBRDKAFKA_SANITIZE} -g -fno-omit-frame-pointer ${CFLAGS}"
    export LDFLAGS="-fsanitize=${LIBRDKAFKA_SANITIZE} ${LDFLAGS}"
else
    CONFIGURE_OPTS="$CONFIGURE_OPTS --disable-debug-symbols"
fi

./configure $CONFIGURE_OPTS
make -j"$(nproc 2>/dev/null || sysctl -n hw.ncpu)"
make install
popd

# --- Mirror NuGet layout ---

INC_DST="$DEST/build/native/include"
mkdir -p "$INC_DST"
cp -r "$INSTALL/include/librdkafka" "$INC_DST/"

if [[ $OSTYPE == linux* ]]; then
    OS_NAME=linux
    LIB_DST="$DEST/runtimes/$OS_NAME-$ARCH/native"
    mkdir -p "$LIB_DST"
    cp -v "$INSTALL"/lib/librdkafka.so* "$LIB_DST/" 2>/dev/null || true
    # Ensure librdkafka.so.1 exists (needed by the loader)
    if [[ ! -f "$LIB_DST/librdkafka.so.1" ]]; then
        cp -v "$LIB_DST/librdkafka.so" "$LIB_DST/librdkafka.so.1"
    fi
    ldd "$LIB_DST/librdkafka.so.1"

elif [[ $OSTYPE == darwin* ]]; then
    OS_NAME=osx
    LIB_DST="$DEST/runtimes/$OS_NAME-$ARCH/native"
    mkdir -p "$LIB_DST"
    cp -v "$INSTALL"/lib/librdkafka*.dylib "$LIB_DST/"
    # Fix the dylib self-referencing name to its installed path
    install_name_tool -id "$LIB_DST/librdkafka.dylib" "$LIB_DST/librdkafka.dylib"
    otool -L "$LIB_DST/librdkafka.dylib"
fi

rm -rf "$SRC"
echo "$0: Done. Headers at $INC_DST, library at $LIB_DST"
