#!/bin/bash
#
#

# The binary wheels contain librdkafka, but has its library name
# mangled to librdkafka-<random>.so.1 to avoid collission with
# system installed libraries.
# This causes problems for plugin libraries since they will not be able
# to load symbols from the same library as the client runs, which either
# causes a link failure (if librdkafka is not installed) or
# linking to another librdkafka library (if librdkafka is installed) which
# most likely is not internally binary compatible with the wheel librdkafka.
#
# The solution is to unpack each wheel and rename the mangled librdkafka
# back to librdkafka.so.1 and change the reference in cimpl.so.
#

set -e

wheelhouse=$1
fixed_wheelhouse=$2
platform=$(uname -s)

if [[ ! -d $wheelhouse || -z $fixed_wheelhouse ]]; then
    echo "Usage: $0 <wheelhouse-directory> <fixed-wheelhouse-directory>"
    exit 1
fi

if [[ $fixed_wheelhouse != "/*" ]]; then
    fixed_wheelhouse=$PWD/$fixed_wheelhouse
fi

mkdir -p $fixed_wheelhouse

fixup_wheel_linux () {
    local whl="$1"
    local fixed_whl="$fixed_wheelhouse/$(basename $whl)"
    local tmpdir=$(mktemp -d)

    echo "Patching $whl"
    unzip $whl -d $tmpdir
    pushd $tmpdir
    pushd confluent_kafka

    # Find mangled librdkafka name
    pushd .libs
    local mangled=$(echo librdkafka-*.so.1)
    if [[ ! -f $mangled ]]; then
        echo "Failed to find mangled librdkafka:"
        ls -la
        exit 1
    fi
    echo "Patching $PWD/$mangled with current soname: "
    patchelf --print-soname $mangled
    patchelf --set-soname librdkafka.so.1 $mangled
    mv $mangled librdkafka.so.1
    popd # .libs

    patchelf --replace-needed $mangled librdkafka.so.1 cimpl*.so
    echo "Needed libraries now:"
    patchelf --print-needed cimpl*.so

    popd # confluent_kafka

    zip -r $fixed_whl .

    popd # tmpdir
    rm -rf $tmpdir

    echo "Fixed $fixed_whl"
}


if [[ $platform == "Linux" ]]; then
    if ! which patchelf >/dev/null 2>&1; then
        echo "Need to install patchelf to continue"
        sudo apt-get install -y patchelf
    fi

    for wheel in $wheelhouse/*linux*.whl ; do
        fixup_wheel_linux "$wheel"
    done

elif [[ $platform == "Darwin" ]]; then
    exit 0 # No action needed
    if ! which install_name_tool >/dev/null 2>&1; then
        echo "Requires install_name_tool, but not found"
        exit 1
    fi



else
    echo "Unsupported platform: $platform"
    exit 1
fi





