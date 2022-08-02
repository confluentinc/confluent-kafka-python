#!/bin/bash
#
#
# Tests the manylinux wheels on a plethora of bare-bone Linux docker images.
# To override docker images to test, set env DOCKER_IMAGES.
#
# Usage outside of docker:
#  $ tools/test-manylinux.sh

set -e

if [[ $1 == "--in-docker" ]]; then
    IN_DOCKER=1
    shift
else
    IN_DOCKER=0
fi

if [[ -z $1 ]]; then
    echo "Usage: $0 <wheel-directory>"
    exit 1
fi

WHEELHOUSE="$1"

if [[ ! -d $WHEELHOUSE ]]; then
    echo "Wheelhouse directory $WHEELHOUSE does not exist"
    exit 1
fi

echo "$0 running from $(pwd)"

function setup_ubuntu {
    # Ubuntu container setup
    apt-get update
    apt-get install -y python3.8 curl
    # python3-distutils is required on Ubuntu 18.04 and later but does
    # not exist on 14.04.
    apt-get install -y python3.8-distutils || true
}


function run_single_in_docker {
    # Run single test inside docker container
    local wheelhouse=$1
    local testscript=$2

    if [[ ! -d $wheelhouse ]]; then
        echo "On docker instance: wheelhouse $wheelhouse does not exist"
        exit 1
    fi

    # Detect OS
    if grep -qiE 'ubuntu|debian' /etc/os-release 2>/dev/null ; then
        setup_ubuntu
    else
        echo "WARNING: Don't know what platform I'm on: $(uname -a)"
    fi

    # Don't install pip from distro packaging since it pulls
    # in a plethora of possibly outdated Python requirements that
    # might interfere with the newer packages from PyPi, such as six.
    # Instead install it directly from PyPa.
    curl https://bootstrap.pypa.io/get-pip.py | python3.8

    /io/tools/smoketest.sh "$wheelhouse"
}

function run_all_with_docker {
    # Run tests in all listed docker containers.
    # This is executed on the host.
    local wheelhouse=$1

    if [[ ! -d ./$wheelhouse ]]; then
        echo "$wheelhouse must be a relative subdirectory of $(pwd)"
        exit 1
    fi

    [[ ! -z $DOCKER_IMAGES ]] || \
        # LTS and stable release of popular Linux distros.
        DOCKER_IMAGES="ubuntu:18.04 ubuntu:20.04"


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
        docker run -v $(pwd):/io $DOCKER_IMAGE /io/tools/test-manylinux.sh --in-docker "/io/$wheelhouse" || \
            (echo "Failed on $DOCKER_IMAGE" ; false)

    done
}



if [[ $IN_DOCKER == 1 ]]; then
    # Called from within a docker container
    cd /io  # Enter the confluent-kafka-python top level directory
    run_single_in_docker $WHEELHOUSE

else
    # Run from host, trigger runs for all docker images.

    run_all_with_docker $WHEELHOUSE
fi


