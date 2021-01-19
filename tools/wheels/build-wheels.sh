#!/bin/bash
#
#
# Build wheels (on Linux or OSX) using cibuildwheel.
#

this_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"


# Skip PyPy, old Python3 versions, and x86 builds.
export CIBW_SKIP="pp* cp35-* *i686"
export CIBW_TEST_COMMAND="python {project}/test.py"


librdkafka_version=$1
wheeldir=$2

if [[ -z $wheeldir ]]; then
    echo "Usage: $0 <librdkafka-nuget-version> <wheeldir>"
    exit 1
fi

set -ex

[[ -d $wheeldir ]] || mkdir -p "$wheeldir"


case $OSTYPE in
    linux*)
        os=linux
        # Need to set up env vars (in docker) so that setup.py
        # finds librdkafka.
        lib_dir=dest/runtimes/linux-x64/native
        export CIBW_ENVIRONMENT="INCLUDE_DIRS=dest/build/native/include LIB_DIRS=$lib_dir LD_LIBRARY_PATH=\$LD_LIBRARY_PATH:\$PWD/$lib_dir"
        ;;
    darwin*)
        os=macos
        # Need to set up env vars so that setup.py finds librdkafka.
        lib_dir=dest/runtimes/osx-x64/native
        export INCLUDE_DIRS="${PWD}/dest/build/native/include"
        export LIB_DIRS="${PWD}/$lib_dir"
        ;;
    *)
        echo "$0: Unsupported OSTYPE $OSTYPE"
        exit 1
        ;;
esac


$this_dir/install-librdkafka.sh $librdkafka_version dest

python3 -m pip install cibuildwheel==1.7.4

if [[ -z $TRAVIS ]]; then
    cibw_args="--platform $os"
fi

LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$PWD/$lib_dir python3 -m cibuildwheel --output-dir $wheeldir $cibw_args

ls $wheeldir

for f in $wheeldir/*whl ; do
    echo $f
    unzip -l $f
done

