#!/bin/bash
#
#

#
# Modify libraries included in the wheel to find the wheel-included
# librdkafka, including plugins such as the monitoring-interceptor.


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

    # The .libs directory seems to differ between wheel tooling versions.
    pushd confluent_kafka/.libs || pushd confluent_kafka.libs

    # Find mangled librdkafka name
    local mangled=$(echo librdkafka-*.so.1)
    if [[ ! -f $mangled ]]; then
        echo "Failed to find mangled librdkafka:"
        ls -la
        exit 1
    fi

    echo "Copying additional libs and plugins"
    for extlib in $stagedir/libs/*.so ; do
        echo "Fixing $extlib"

        # Copy file to wheel's libdir
        local lib=$(basename $extlib)
        cp -v "$extlib" "$lib"

        # Change the name to be local
        patchelf --print-soname $lib
        patchelf --set-soname $lib $lib

        local curr_lrk=$(ldd $lib | grep librdkafka.so | awk '{print $1}')
        if [[ -n $curr_lrk ]]; then
            # Change the librdkafka reference to load from the same
            # directory as the plugin
            patchelf --replace-needed "$curr_lrk" "$mangled" $lib
        elif [[ $lib == monitoring-interceptor* ]]; then
            # Some versions of the monitoring interceptor is not
            # properly linked to librdkafka (CP 5.0.1), fix it.
            patchelf --add-needed "$mangled" $lib
        fi
        echo "$lib dependencies:"
        ldd $lib
    done

    popd # confluent_kafka/.libs or confluent_kafka.libs
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
        # Extract existing(old) reference
        old=$(otool -L $lib | grep -o '.*librdkafka.1.dylib' | xargs)
        if [[ ! -z "$old" ]]; then
            # Change the librdkafka reference to load from the same
            # directory as the plugin
            install_name_tool -change "$old" '@loader_path/librdkafka.1.dylib' $lib
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

    popd # tmpdir

    wheel pack $tmpdir -d $fixed_wheelhouse

    rm -rf $tmpdir

    echo "Fixed $fixed_whl"
}


build_patchelf () {
    # Download, build and install patchelf from source.

    curl -l https://releases.nixos.org/patchelf/patchelf-0.9/patchelf-0.9.tar.gz | tar xzf -
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
