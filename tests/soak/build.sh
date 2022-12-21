#!/bin/bash
#

librdkafka_version=$1
cflpy_version=$2

if [[ -z $cflpy_version ]]; then
    echo "Usage: $0 <librdkafka_version|tag|branch> <cfl-kafka-python-version|tag|branch>"
    exit 1
fi

set -eu



echo "Building and installing librdkafka $librdkafka_version"
pushd librdkafka
sudo make uninstall
git fetch --tags
git checkout $librdkafka_version
./configure --reconfigure
make clean
make -j
sudo make install
popd


echo "Building confluent-kafka-python $cflpy_version"
set +u
source venv/bin/activate
set -u
pushd confluent-kafka-python
git fetch --tags
git checkout $cflpy_version
python3 setup.py clean -a
python3 setup.py build
python3 -m pip install .
popd

echo ""
echo "=============================================================================="
(cd / ; python3 -c 'import confluent_kafka as c; print("python", c.version(), "librdkafka", c.libversion())')

