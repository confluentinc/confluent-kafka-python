#!/bin/bash
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
SRC=/tmp/librdkafka-branch-src
INSTALL=$SRC/install

[[ -d "$DEST" ]] || mkdir -p "$DEST"
rm -rf "$SRC"

git clone --depth 1 --branch "$BRANCH" \
    https://github.com/confluentinc/librdkafka.git "$SRC"

pushd "$SRC"
./configure --prefix="$INSTALL" --disable-debug-symbols
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
