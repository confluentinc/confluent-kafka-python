#!/bin/bash
#
#
# Builds autonomous Python packages including all available dependencies
# using docker.
#
# This is a tiny linux alternative to cibuildwheel as a workaround
# to provide --volumes-from=.. on docker-in-docker builds.
#

if [[ $# -lt 3 ]]; then
    echo "Usage: $0 <librdkafka_tag> <repo-dir-full-path> <relative-out-dir>"
    exit 1
fi

set -ex

if [[ $1 != "--in-docker" ]]; then
    # Outside our docker
    workdir=$2
    if [[ -f /.dockerenv ]]; then
        echo "$0: $HOSTNAME: Currently running from inside docker: exposing host mounts"
        docker run -i --volumes-from=$HOSTNAME quay.io/pypa/manylinux1_x86_64 $workdir/tools/build-linux-selfcontained.sh --in-docker $1 $2 $3
    else
        echo "$0: $HOSTNAME: Not currently running from inside docker: exposing $2 as volume"
        docker run -i -v $workdir:$workdir quay.io/pypa/manylinux1_x86_64 $workdir/tools/build-linux-selfcontained.sh --in-docker $1 $2 $3
    fi
    exit 0
fi

#
# Inside our docker
#

echo "$0: $HOSTNAME: Run"

shift # remove --in-docker

LIBRDKAFKA_VERSION=$1
WORKDIR=$2
OUTDIR=$3


function install_deps {
    echo "# Installing basic system dependencies"
    if which apt-get >/dev/null 2>&1; then
        sudo apt-get -y install gcc g++ zlib1g-dev libssl-dev
    else
        yum install -y zlib-devel gcc gcc-c++ libstdc++-devel

        # Build OpenSSL
        $(dirname $0)/build-openssl.sh /usr

    fi
}

function build_librdkafka {
    local dest=$1
    echo "# Building librdkafka ${LIBRDKAFKA_VERSION}"
    $(dirname $0)/bootstrap-librdkafka.sh --require-ssl ${LIBRDKAFKA_VERSION} $dest

}


function build {
    local workdir=$1
    local outdir=$2

    pushd $workdir

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
    done

    popd # workdir
}

build $WORKDIR $OUTDIR

