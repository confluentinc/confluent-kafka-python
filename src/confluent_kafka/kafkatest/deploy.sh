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
    echo "Jing Liu force update"
    shift
fi

DIR=$1
if [[ -z $DIR ]]; then
    DIR=/tmp
fi

echo "Jing test 1"
[[ -d $DIR ]] || mkdir -p $DIR
echo "Jing test 2"
pushd $DIR
echo "Jing test 3"

mkdir -p $DIR/dist
echo "Jing test 4"

function setup_virtualenv {
    if [[ ! -f $DIR/venv/bin/activate ]]; then
        echo "Installing and creating virtualenv"
        which virtualenv || sudo apt-get install -y python-virtualenv
        virtualenv $DIR/venv
        source $DIR/venv/bin/activate
        # Upgrade pip
        #pip install -U pip
        #python -m pip install --upgrade pip
        #python3 -m pip install --upgrade pip
    else
        echo "Reusing existing virtualenv"
        source $DIR/venv/bin/activate
    fi

}


function install_librdkafka {
    echo "Jing test 5"
    [[ $FORCE_UPDATE == 1 ]] && rm -rf librdkafka
    echo "Jing test 6"
    mkdir -p librdkafka
    echo "Jing test 7"
    [[ -f librdkafka/configure ]] || curl -Lq https://github.com/edenhill/librdkafka/archive/master.tar.gz | \
	    tar -xvf - --strip=1
    echo "Jing test 8"
    pushd librdkafka
    echo "Jing test 9"
    ./configure --prefix=$DIR/dist
    echo "Jing test 10"
    make
    echo "Jing test 11"
    make install
    echo "Jing test 12"
    popd
    echo "Jing test 13"
}

function install_client {
    sudo apt update && sudo apt install -y sudo python3-pip libpq-dev python3-dev libffi-dev libssl-dev && apt-get -y clean
    echo "pip upgrade"
    python3 -m pip install --upgrade pip
    echo "Jing test 14"
    pip uninstall -y confluent_kafka || true
    echo "Jing test 15"
    pip install -U --only-binary confluent_kafka -f /vagrant/$REL_DIR confluent_kafka
    echo "Jing test 16"
}

function verify_client {
    install_client
    echo "Jing list all python version"
    ls -ls /usr/bin/python*
    python -m confluent_kafka.kafkatest.verifiable_consumer --help
    python3 -m confluent_kafka.kafkatest.verifiable_consumer --help
    echo "Jing test 18"
}


if [[ $FORCE_UPDATE != 1 ]]; then
    echo "Jing test 19"
    verify_client && exit 0
fi

setup_virtualenv

# librdkafka is bundled with the wheel, if not, install it here:
#install_librdkafka

if ! verify_client ; then
    echo "Client not installed, installing..."
    install_client
    echo "Jing test 20"
    verify_client
    echo "Jing test 21"
else
    echo "Client already installed"
fi



