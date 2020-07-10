#!/bin/bash
#

#
# Run smoke tests to verify a vanilla installation of confluent-kafka.
#
# If a wheeldir is supplied confluent-kafka is first installed.
#
# Must be run from the top-level project directory.
#

if [[ ! -f tools/$(basename $0) ]]; then
    echo "$0: Must be run from the top-level project directory"
    exit 1
fi

set -e

wheeldir=$1
testdir=


if [[ -n $wheeldir ]]; then
    if [[ ! -d $wheeldir ]]; then
        echo "$0: wheeldir $wheeldir does not exist"
        exit 1
    fi
    python -m pip install virtualenv
fi


# Run tests with both python2 and python3 (whatever versions the OS provides)
for py in 2.7 3.8 3.7 3.6 3.5 ; do
    echo "# Smoketest with Python$py"

    if ! python$py -V ; then
        echo "$0: python$py not available: skipping"
        continue
    fi

    if [[ -n $wheeldir ]]; then
        venvdir=$(mktemp -d /tmp/_venvXXXXXX)

        function cleanup () {
            set +e
            deactivate
            echo $venvdir
            rm -rf "$venvdir"
            [[ -d $testdir ]] && rm -rf "$testdir"
            set -e
        }

        trap cleanup EXIT

        virtualenv -p python$py $venvdir
        source $venvdir/bin/activate
        hash -r

        pip install -U pip pkginfo
        pip install -r tests/requirements.txt

        # Get the packages version so we can pin the install
        # command to this version (which hopefully loads it from the wheeldir
        # rather than PyPi) while still allowing dependencies to be installed
        # from PyPi.
        # Assuming that the wheeldirectory only contains wheels for a single
        # version we can pick any wheel file.
        version=$(pkginfo -f version $(ls $wheeldir/confluent_kafka*.whl | head -1) | sed -e 's/^version: //')
        if [[ -z $version ]]; then
            echo "Unable to parse version from wheel files in $wheeldir"
            exit 1
        fi

        pip install --find-links "$wheeldir" confluent-kafka==$version
        pip install --find-links "$wheeldir" confluent-kafka[avro]==$version
        pip install --find-links "$wheeldir" confluent-kafka[protobuf]==$version
        pip install --find-links "$wheeldir" confluent-kafka[json]==$version
    fi


    # Copy unit tests to temporary directory to avoid any conflicting __pycache__
    # directories from the source tree.
    testdir=$(mktemp -d /tmp/_testdirXXXXXX)
    cp tests/*.py $testdir/

    # Change to a neutral path where there is no confluent_kafka sub-directory
    # that might interfere with module load.
    pushd $testdir
    echo "Running unit tests"
    pytest

    fails=""

    echo "Verifying OpenSSL"
    python -c "
import confluent_kafka
confluent_kafka.Producer({'ssl.cipher.suites':'DEFAULT'})
" || fails="$fails OpenSSL"

    for compr in gzip lz4 snappy zstd; do
        echo "Verifying $compr"
        python -c "
import confluent_kafka
confluent_kafka.Producer({'compression.codec':'$compr'})
" || fails="$fails $compr"
    done

    echo "Verifying Interceptor installation"
    echo "Note: Requires protobuf-c to be installed on the system."
    python -c '
from confluent_kafka import Consumer

c = Consumer({"group.id": "test-linux", "plugin.library.paths": "monitoring-interceptor"})
' || echo "Warning: interceptor test failed, which we ignore"

    popd # $testdir
done

if [[ -z $fails ]]; then
    echo "ALL SMOKE TESTS PASSED"
    exit 0
fi

echo "SMOKE TEST FAILURES: $fails"
exit 1


