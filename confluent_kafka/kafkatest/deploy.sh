#!/bin/bash
#
#
# Deploys confluent-kafka-python (with dependencies) on kafkatest VM instance.
#

set -ex

# Relative directory where we put our stuff.
#  $KAFKA_DIR/$REL_DIR on host, and /vagrant/$REL_DIR on worker
REL_DIR=tests/confluent-kafka-python

if [[ $1 == "--prepare" ]]; then
    #
    # On host: prepare kafka directory with artifacts needed on worker instances
    #
    shift

    if [[ $# -ne 2 ]]; then
        echo "Usage: $0 --prepare <kafka-directory> <wheel-directory>"
        exit 1
    fi

    KAFKA_DIR=$1
    WHEEL_DIR=$2

    if [[ -z $(ls $WHEEL_DIR/*.whl || true) ]]; then
        echo "$0: No wheels found in $WHEEL_DIR"
        exit 1
    fi

    DIR="$KAFKA_DIR/$REL_DIR"
    mkdir -p $DIR

    # Copy this script
    cp -v $0 $DIR/

    # Copy wheels
    cp -v $WHEEL_DIR/*.whl $DIR/

    # Copy kafkatest's globals.json
    cp -v $(dirname $0)/globals.json $DIR/

    echo ""
    echo "$DIR prepared successfully:"
    ls -la $DIR/
    exit 0
fi


#
# On worker instance
#

if [[ $1 == "--update" ]]; then
    FORCE_UPDATE=1
    shift
fi

DIR=$1
if [[ -z $DIR ]]; then
    DIR=/tmp
fi

[[ -d $DIR ]] || mkdir -p $DIR
pushd $DIR

mkdir -p $DIR/dist

function setup_virtualenv {
    if [[ ! -f $DIR/venv/bin/activate ]]; then
        echo "Installing and creating virtualenv"
        which virtualenv || sudo apt-get install -y python-virtualenv
        virtualenv $DIR/venv
        source $DIR/venv/bin/activate
        # Upgrade pip
        pip install -U pip
    else
        echo "Reusing existing virtualenv"
        source $DIR/venv/bin/activate
    fi

}


function install_librdkafka {
    [[ $FORCE_UPDATE == 1 ]] && rm -rf librdkafka
    mkdir -p librdkafka
    [[ -f librdkafka/configure ]] || curl -Lq https://github.com/edenhill/librdkafka/archive/master.tar.gz | \
	    tar -xvf - --strip=1
    pushd librdkafka
    ./configure --prefix=$DIR/dist
    make
    make install
    popd
}

function install_client {
    pip uninstall -y confluent_kafka || true
    pip install -U --only-binary confluent_kafka -f /vagrant/$REL_DIR confluent_kafka
}

function verify_client {
    python -m confluent_kafka.kafkatest.verifiable_consumer --help
}


if [[ $FORCE_UPDATE != 1 ]]; then
    verify_client && exit 0
fi

setup_virtualenv

# librdkafka is bundled with the wheel, if not, install it here:
#install_librdkafka

if ! verify_client ; then
    echo "Client not installed, installing..."
    install_client
    verify_client
else
    echo "Client already installed"
fi



