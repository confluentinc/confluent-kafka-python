#!/bin/bash
#
#
# Tests the manylinux wheels on a plethora of bare-bone Linux docker images.
# To override docker images to test, set env DOCKER_IMAGES.
#
# Usage outside of docker:
#  $ tools/test-manylinux.sh

set -ex

echo "$0 running from $(pwd)"

if [[ ! -z $1 ]]; then
    WHEELHOUSE=$1
else
    WHEELHOUSE="wheelhouse"
fi



function setup_centos {
    # CentOS container setup
    yum install -q -y python epel-release
    yum install -q -y python-pip
}

function setup_ubuntu {
    # Ubuntu container setup
    apt-get update
    apt-get install -y python python-pip
}


function run_single_in_docker {
    # Run single test inside docker container
    local wheelhouse=/io/$1

    if [[ ! -d $wheelhouse ]]; then
        echo "On docker instance: wheelhouse $wheelhouse does not exist"
        exit 1
    fi

    # Detect OS
    if grep -qi centos /etc/system-release /etc/redhat-release ; then
        setup_centos
    elif grep -qiE 'ubuntu|debian' /etc/os-release ; then
        setup_ubuntu
    else
        echo "WARNING: Don't know what platform I'm on: $(uname -a)"
    fi

    # Make sure pip itself is up to date
    pip install -U pip
    hash -r # let go of previous 'pip'

    # Install modules

    # TODO: revisit to avoid hardcoding dependencies
    pip install "futures;python_version=='2.7'"
    pip install "enum34;python_version=='2.7'"

    pip install confluent_kafka --no-index -f $wheelhouse

    # Pytest relies on a new version of six; later versions of pip fail to remove older versions gracefully
    # https://github.com/pypa/pip/issues/5247
    pip install pytest --ignore-installed six

    # Verify that OpenSSL and zlib are properly linked
    python -c '
import confluent_kafka

p = confluent_kafka.Producer({"ssl.cipher.suites":"DEFAULT",
                              "compression.codec":"gzip"})
'

    pushd /io/tests
    # Remove cached files from previous runs
    rm -rf __pycache__ *.pyc
    # Test
    pytest --import-mode=append --ignore=avro
    popd

}

function run_all_with_docker {
    # Run tests in all listed docker containers.
    # This is executed on the host.
    local wheelhouse=$1

    [[ ! -z $DOCKER_IMAGES ]] || \
        # LTS and stable release of popular Linux distros.
        # We require >= Python 2.7 to be avaialble (which rules out Centos 6.6)
        DOCKER_IMAGES="ubuntu:14.04 ubuntu:16.04 ubuntu:17.10 debian:stable centos:7"


    _wheels="$wheelhouse/*manylinux*.whl"
    if [[ -z $_wheels ]]; then
        echo "No wheels in $wheelhouse, must run build-manylinux.sh first"
        exit 1
    else
        echo "Wheels:"
        ls $wheelhouse/*.whl
    fi

    for DOCKER_IMAGE in $DOCKER_IMAGES; do
        echo "# Testing on $DOCKER_IMAGE"
        docker run -v $(pwd):/io $DOCKER_IMAGE /io/tools/test-manylinux.sh "$wheelhouse" || \
            (echo "Failed on $DOCKER_IMAGE" ; false)

    done
}



if [[ -f /.dockerenv && -d /io ]]; then
    # Called from within a docker container
    run_single_in_docker $WHEELHOUSE

else
    # Run from host, trigger runs for all docker images.

    run_all_with_docker $WHEELHOUSE
fi


