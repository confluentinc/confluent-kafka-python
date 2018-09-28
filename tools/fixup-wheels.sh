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

stagedir=$PWD/staging

if [[ ! -d $wheelhouse || -z $fixed_wheelhouse ]]; then
    echo "Usage: $0 <wheelhouse-directory> <fixed-wheelhouse-directory>"
    exit 1
fi

if [[ $fixed_wheelhouse != "/*" ]]; then
    fixed_wheelhouse=$PWD/$fixed_wheelhouse
fi

mkdir -p $fixed_wheelhouse

fixup_wheel_linux () {

    pushd confluent_kafka

    pushd .libs

    echo "Copying additional libs and plugins"
    cp -v $stagedir/libs/* .

    # Find mangled librdkafka name
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
}


fixup_wheel_macosx () {

    pushd confluent_kafka/.dylibs

    echo "Copying additional libs and plugins from $stagedir/libs/"
    for extlib in $stagedir/libs/*.dylib ; do
        echo "Fixing $extlib"

        # Copy file to wheel's libdir
        lib=$(basename $extlib)
        cp -v "$extlib" "$lib"

        # Change the name to be local
        install_name_tool -id "$lib" $lib
        if otool -L $lib | grep -q /usr/local/lib/librdkafka.1.dylib ; then
            # Change the librdkafka reference to load from the same
            # directory as the plugin
            install_name_tool -change /usr/local/lib/librdkafka.1.dylib '@loader_path/librdkafka.1.dylib' $lib
            otool -L $lib
        else
            echo "WARNING: couldn't find librdkafka reference in $lib"
            otool -L $lib
        fi

    done

    popd # confluent_kafka
}



fixup_wheel () {
    local whl="$1"
    local fixed_whl="$fixed_wheelhouse/$(basename $whl)"
    local tmpdir=$(mktemp -d)

    echo "Patching $whl"
    unzip $whl -d $tmpdir

    pushd $tmpdir
    if [[ $platform == "Linux" ]]; then
        fixup_wheel_linux
    elif [[ $platform == "Darwin" ]]; then
        fixup_wheel_macosx
    fi

    zip -r $fixed_whl .

    popd # tmpdir
    rm -rf $tmpdir

    echo "Fixed $fixed_whl"
}


build_patchelf () {
    # Download, build and install patchelf from source.

    curl -l https://nixos.org/releases/patchelf/patchelf-0.9/patchelf-0.9.tar.gz | tar xzf -
    pushd patchelf-0.9
    ./configure
    make -j
    sudo make install
    popd # patchelf-..
}

if [[ $platform == "Linux" ]]; then
    if ! which patchelf >/dev/null 2>&1; then
        echo "Need to install patchelf to continue"
        sudo apt-get install -y patchelf || build_patchelf
    fi

    patchelf --version

    wheelmatch=linux

elif [[ $platform == "Darwin" ]]; then
    if ! which install_name_tool >/dev/null 2>&1; then
        echo "Requires install_name_tool, but not found"
        exit 1
    fi

    wheelmatch=macosx

else
    echo "Unsupported platform: $platform"
    exit 1
fi



for wheel in $wheelhouse/*${wheelmatch}*.whl ; do
    fixup_wheel "$wheel"
done



