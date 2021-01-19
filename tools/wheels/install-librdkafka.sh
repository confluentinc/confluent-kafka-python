#!/bin/bash

set -ex

VER="$1"
DEST="$2"

if [[ -z $DEST ]]; then
    echo "Usage: $0 <librdkafka-redist-version> <destdir>"
    exit 1
fi

if [[ -f $DEST/build/native/include/librdkafka/rdkafka.h ]]; then
    echo "$0: librdkafka already installed in $DEST"
    exit 0
fi

echo "$0: Installing lbirdkafka $VER to $DEST"
[[ -d "$DEST" ]] || mkdir -p "$DEST"
pushd "$DEST"

curl -L -o lrk$VER.zip https://www.nuget.org/api/v2/package/librdkafka.redist/$VER

unzip lrk$VER.zip


if [[ $OSTYPE == linux* ]]; then
    # Linux

    # Copy the librdkafka build with least dependencies to librdkafka.so.1
    cp -v runtimes/linux-x64/native/{centos6-librdkafka.so,librdkafka.so.1}
    ldd runtimes/linux-x64/native/librdkafka.so.1

elif [[ $OSTYPE == darwin* ]]; then
    # MacOS X

    # Change the library's self-referencing name from
    # /Users/travis/.....somelocation/librdkafka.1.dylib to its local path.
    install_name_tool -id $PWD/runtimes/osx-x64/native/librdkafka.dylib runtimes/osx-x64/native/librdkafka.dylib

    otool -L runtimes/osx-x64/native/librdkafka.dylib
fi

popd
