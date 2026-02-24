#!/bin/bash
#
#
# Build wheels (on Linux or OSX) using cibuildwheel.
#
this_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"


# Skip PyPy, Python2, old Python3 versions, musl, and x86 builds.
export CIBW_SKIP="pp* cp27-* cp35-* cp36-* *i686 *musllinux* $CIBW_SKIP"
# Run a simple test suite
export CIBW_TEST_REQUIRES="pytest"
export CIBW_TEST_COMMAND="pytest {project}/tests/test_error.py"
export CIBW_MANYLINUX_X86_64_IMAGE="manylinux_2_28"
export CIBW_MANYLINUX_AARCH64_IMAGE="manylinux_2_28"

librdkafka_version=$1
wheeldir=$2
cibuildwheel_version=${3:-"3.2.1"}

if [[ -z $wheeldir ]]; then
    echo "Usage: $0 <librdkafka-nuget-version> <wheeldir>"
    exit 1
fi

set -ex

[[ -d $wheeldir ]] || mkdir -p "$wheeldir"

ARCH=${ARCH:-x64}

case $OSTYPE in
    linux*)
        os=linux
        # Need to set up env vars (in docker) so that cibuildwheel finds librdkafka.
        lib_dir=dest/runtimes/linux-$ARCH/native
        export CIBW_ENVIRONMENT="CFLAGS=-I\$PWD/dest/build/native/include LDFLAGS=-L\$PWD/$lib_dir LD_LIBRARY_PATH=\$LD_LIBRARY_PATH:\$PWD/$lib_dir"
        ;;
    darwin*)
        os=macos
        # Need to set up env vars so that cibuildwheel finds librdkafka.
        lib_dir=dest/runtimes/osx-$ARCH/native
        export CFLAGS="-I${PWD}/dest/build/native/include"
        export LDFLAGS="-L${PWD}/$lib_dir"
        ;;
    *)
        echo "$0: Unsupported OSTYPE $OSTYPE"
        exit 1
        ;;
esac

$this_dir/install-librdkafka.sh $librdkafka_version dest

install_pkgs=cibuildwheel==$cibuildwheel_version

uv pip install $install_pkgs

if [[ -z $TRAVIS ]]; then
    cibw_args="--platform $os"
fi

if [[ $os == "macos" ]]; then
    python3 $this_dir/install-macos-python-required-by-cibuildwheel.py $cibuildwheel_version
fi

LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$PWD/$lib_dir python3 -m cibuildwheel --output-dir $wheeldir $cibw_args

ls $wheeldir

for f in $wheeldir/*whl ; do
    echo $f
    unzip -l $f
done

