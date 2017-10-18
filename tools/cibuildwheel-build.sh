#!/bin/bash
#
#
# Build Python packages using cibuildwheel (and docker)
#
# Usage:
#   tools/cibuildwheel-build.sh <output_dir> [<librdkafka-tag(def:master)>]


if [[ ! -f tools/$(basename $0) ]]; then
    echo "$0: must be executed from the confluent-kafka-python root directory"
    exit 1
fi

OUT_DIR=$1

if [[ -z $OUT_DIR ]]; then
    echo "Usage: $0 <out_dir> [<librdkafka-tag>]"
    exit 1
fi

LIBRDKAFKA_VERSION=$2

if [[ -z $LIBRDKAFKA_VERSION ]]; then
    LIBRDKAFKA_VERSION=master
fi

set -e


_CIBW_ARGS=
case "$(uname -s)" in
    Linux*)
        export CIBW_BEFORE_BUILD="tools/prepare-cibuildwheel-linux.sh $LIBRDKAFKA_VERSION"
        if [[ -z $TRAVIS_OS_NAME ]]; then
            _CIBW_ARGS="--platform linux"
        fi
        ;;
    Darwin*)
        export CIBW_BEFORE_BUILD="tools/bootstrap-librdkafka.sh --require-ssl $LIBRDKAFKA_VERSION librdkafka-tmp"
        export CFLAGS="-Ilibrdkafka-tmp/include"
        export LDFLAGS="-Llibrdkafka-tmp/lib"
        if [[ -z $TRAVIS_OS_NAME ]]; then
            _CIBW_ARGS="--platform macos"
        fi

        ;;
    *)
        echo "$0: Unsupported platform: $(uname -s)"
        exit 1
        ;;
esac

if ! which cibuildwheel 2>/dev/null ; then
    pip install cibuildwheel==0.4.1
fi

cibuildwheel $_CIBW_ARGS --output-dir "$OUT_DIR"

echo "Packages in $OUT_DIR:"
(cd $OUT_DIR ; ls -la)

