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
all_fails=

if [[ -n $wheeldir ]]; then
    if [[ ! -d $wheeldir ]]; then
        echo "$0: wheeldir $wheeldir does not exist"
        exit 1
    fi
fi

pyvers_tested=

# Run tests with python3
for py in 3.8 ; do
    echo "$0: # Smoketest with Python$py"

    if ! python$py -V ; then
        echo "$0: python$py not available: skipping"
        continue
    fi

    pyvers_tested="$pyvers_tested $py"

    if [[ -n $wheeldir ]]; then
        venvdir=$(mktemp -d /tmp/_venvXXXXXX)

        function cleanup () {
            set +e
            deactivate
            rm -rf "$venvdir"
            [[ -d $testdir ]] && rm -rf "$testdir"
            set -e
        }

        trap cleanup EXIT

        python$py -m pip install virtualenv

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
            echo "$0: Unable to parse version from wheel files in $wheeldir"
            exit 1
        fi

        pip install --find-links "$wheeldir" confluent-kafka==$version
        # Install a prebuilt version of fastavro that doesn't require a gcc toolchain
        pip install --find-links "$wheeldir" --only-binary :fastavro: confluent-kafka[avro]==$version
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
    echo "$0: Running unit tests"
    pytest

    fails=""

    echo "$0: Verifying OpenSSL"
    python -c "
import confluent_kafka
confluent_kafka.Producer({'ssl.cipher.suites':'DEFAULT'})
" || fails="$fails OpenSSL"

    for compr in gzip lz4 snappy zstd; do
        echo "$0: Verifying $compr"
        python -c "
import confluent_kafka
confluent_kafka.Producer({'compression.codec':'$compr'})
" || fails="$fails $compr"
    done

    echo "$0: Verifying Interceptor installation"
    echo "$0: Note: Requires protobuf-c to be installed on the system."
    python -c '
from confluent_kafka import Consumer

c = Consumer({"group.id": "test-linux", "plugin.library.paths": "monitoring-interceptor"})
' || echo "$0: Warning: interceptor test failed, which we ignore"


    if [[ -n $fails ]]; then
        echo "$0: SMOKE TEST FAILS FOR python$py: $fails"
        all_fails="$all_fails \[py$py: $fails\]"
    fi

    popd # $testdir
done

if [[ -z $pyvers_tested ]]; then
    echo "$0: NO PYTHON VERSIONS TESTED"
    exit 1
fi

if [[ -z $all_fails ]]; then
    echo "$0: ALL SMOKE TESTS PASSED (python versions:$pyvers_tested)"
    exit 0
fi

echo "$0: SMOKE TEST FAILURES: $all_fails"
exit 1


