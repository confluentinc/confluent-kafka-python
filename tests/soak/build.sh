#!/bin/bash
#

librdkafka_version=$1
cflpy_version=$2

if [[ -z $cflpy_version ]]; then
    echo "Usage: $0 <librdkafka_version|tag|branch> <cfl-kafka-python-version|tag|branch>"
    exit 1
fi

set -eu

testdir=$PWD
mkdir -p $testdir/librdkafka-installation

if [[ ! -d confluent-kafka-python ]]; then
    git clone https://github.com/confluentinc/confluent-kafka-python
fi

venv=$PWD/venv
if [[ ! -d $venv ]]; then
    echo "Setting up virtualenv in $venv"
    python3 -m venv $venv
    source $venv/bin/activate
    pip install -U pip
    pip install -r $testdir/../../requirements/requirements-soaktest.txt
    deactivate
fi

echo "Building and installing librdkafka $librdkafka_version"
if [[ ! -d librdkafka ]]; then
    git clone https://github.com/confluentinc/librdkafka.git
fi
pushd librdkafka
git fetch --tags
git checkout $librdkafka_version
echo "Configuring librdkafka $librdkafka_version with prefix $testdir/librdkafka-installation"
./configure --prefix=$testdir/librdkafka-installation
sudo make uninstall
make clean
make -j
make install
popd

export LIBRARY_PATH=$testdir/librdkafka-installation/lib
export LD_LIBRARY_PATH=$testdir/librdkafka-installation/lib
export CPLUS_INCLUDE_PATH=$testdir/librdkafka-installation/include
export C_INCLUDE_PATH=$testdir/librdkafka-installation/include

echo "Building confluent-kafka-python $cflpy_version"
set +u
source venv/bin/activate
python3 -m pip uninstall -y confluent-kafka
set -u
pushd confluent-kafka-python
rm -rf ./build
git fetch --tags
git checkout $cflpy_version
python3 -m pip install .
popd

echo ""
echo "=============================================================================="
(cd / ; python3 -c 'import confluent_kafka as c; print("python", c.version(), "librdkafka", c.libversion())')
