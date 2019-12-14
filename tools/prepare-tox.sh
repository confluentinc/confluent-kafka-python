#!/usr/bin/env bash

set -eu

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
BUILD_DIR=tmp-build
LIBRDKAFKA_VERSION="v$(python setup.py --version)"

INSTALL_DIR="$BUILD_DIR/$LIBRDKAFKA_VERSION"


function install_librdkafka () {
        rm -if "$BUILD_DIR/lib" "$BUILD_DIR/include"
        tools/bootstrap-librdkafka.sh --require-ssl "$LIBRDKAFKA_VERSION" "$INSTALL_DIR"

        echo "dest" $(realpath "$INSTALL_DIR/lib")
        echo "source" $(realpath "$BUILD_DIR/lib")
}

function set_librdkafka () {
        local version=$1

        pushd ${BUILD_DIR}
                rm -if "lib" "include"
                ln -s $(realpath "$version/lib") $(realpath "lib")
                ln -s $(realpath "$version/include") $(realpath "include")
        popd
}


pushd "$DIR/.."
        [[ ! -d "$BUILD_DIR/$LIBRDKAFKA_VERSION" ]] && install_librdkafka
        set_librdkafka ${LIBRDKAFKA_VERSION}
popd
