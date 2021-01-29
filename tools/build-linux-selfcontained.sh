#!/bin/bash
#
#
# Builds autonomous Python packages including all available dependencies
# using docker.
#
# This is a tiny linux alternative to cibuildwheel on linux that does
# not rely on Docker volumes to work (such as for docker-in-docker).
#
# The in-docker portion of this script is based on cibuildwheel's counterpart.
#

if [[ $# -ne 2 ]]; then
    echo "Usage: $0 <librdkafka_tag> <out-dir>"
    exit 1
fi

set -ex

if [[ $1 != "--in-docker" ]]; then
    # Outside our docker
    LIBRDKAFKA_VERSION=$1
    outdir=$2
    [[ -d $outdir ]] || mkdir -p $outdir

    docker_image=quay.io/pypa/manylinux2010_x86_64

    script_in_docker=/tmp/$(basename $0)

    # Create container
    container=$(basename $(mktemp -u pywhlXXXXXX))
    docker create -i --name $container $docker_image $script_in_docker --in-docker $LIBRDKAFKA_VERSION

    # Create archive
    git archive -o src.tar.gz HEAD

    # Copy this script to container
    docker cp $0 $container:$script_in_docker

    # Copy archive to container
    docker cp src.tar.gz $container:/tmp/

    # Run this script in docker
    docker start -i $container

    # Copy artifacts from container
    rm -rf $outdir/output
    docker cp $container:/output $outdir/
    mv $outdir/output/* $outdir/
    rm -rf $outdir/output

    # Remove container
    docker rm $container

    echo "Artifacts now available in $outdir:"
    ls -la $outdir/

    exit 0
fi


#
# Inside our docker
#

if [[ $# -ne 2 ]]; then
    echo "Inner usage: $0 --in-docker <librdkafka_tag>"
    exit 1
fi

echo "$0: $HOSTNAME: Run"

shift # remove --in-docker

LIBRDKAFKA_VERSION=$1


function install_deps {
    echo "# Installing basic system dependencies"
    if which apt-get >/dev/null 2>&1; then
        sudo apt-get -y install gcc g++ zlib1g-dev
    else
        yum install -y zlib-devel gcc gcc-c++ libstdc++-devel
    fi
}

function build_librdkafka {
    local dest=$1
    echo "# Building librdkafka ${LIBRDKAFKA_VERSION}"
    tools/bootstrap-librdkafka.sh --require-ssl ${LIBRDKAFKA_VERSION} $dest

}


function build {
    local workdir=$1
    local outdir=/output

    mkdir -p $workdir
    pushd $workdir

    tar xvzf /tmp/src.tar.gz

    install_deps

    build_librdkafka /usr

    mkdir -p $outdir

    for PYBIN in /opt/python/cp27-*/bin; do
        # Setup
        rm -rf /tmp/built_wheel
        rm -rf /tmp/delocated_wheel
        mkdir /tmp/built_wheel
        mkdir /tmp/delocated_wheel

        # Build that wheel
        PATH="$PYBIN:$PATH" "$PYBIN/pip" wheel . -w /tmp/built_wheel --no-deps
        built_wheel=(/tmp/built_wheel/*.whl)

        # Delocate the wheel
        # NOTE: 'built_wheel' here is a bash array of glob matches; "$built_wheel" returns
        # the first element
        if [[ "$built_wheel" == *none-any.whl ]]; then
            # pure python wheel - just copy
            mv "$built_wheel" /tmp/delocated_wheel
        else
            auditwheel repair "$built_wheel" -w /tmp/delocated_wheel
        fi
        delocated_wheel=(/tmp/delocated_wheel/*.whl)

        # Install the wheel we just built
        "$PYBIN/pip" install "$delocated_wheel"

        # we're all done here; move it to output
        mv "$delocated_wheel" $outdir/
        ls -la $outdir/
    done

    popd # workdir
}

echo "$0: $HOSTNAME: Building in docker"
build /build
echo "$0: $HOSTNAME: Done"

