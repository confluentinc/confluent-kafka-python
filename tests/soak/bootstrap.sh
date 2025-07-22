#!/bin/bash
#
#

# Bootstrap EC2 instance (Ubuntu) for soak client use
#
# Usage:
#  $0 <python-branch/tag> <librdkafka-branch/tag>

set -e

if [[ $# != 2 ]]; then
    echo "Usage: $0 <librdkafka-branch/tag> <python-client-branch/tag>"
    exit 1
fi

librdkafka_branch=$1
python_branch=$2
otel_collector_version=0.130.0
otel_collector_package_url="https://github.com/open-telemetry/"\
"opentelemetry-collector-releases/releases/download/"\
"v${otel_collector_version}/otelcol-contrib_${otel_collector_version}_linux_amd64.deb"
venv=$PWD/venv

sudo apt update
sudo apt install -y git curl wget make gcc g++ zlib1g-dev libssl-dev \
    libzstd-dev python3-dev python3-pip python3-venv
wget -O otel_collector_package.deb $otel_collector_package_url
sudo dpkg -i otel_collector_package.deb
rm otel_collector_package.deb
sudo cp otel-config.yaml /etc/otelcol-contrib/config.yaml
sudo systemctl restart otelcol-contrib

testdir=$PWD
export LIBRARY_PATH=$testdir/librdkafka-installation/lib
export LD_LIBRARY_PATH=$testdir/librdkafka-installation/lib
export CPLUS_INCLUDE_PATH=$testdir/librdkafka-installation/include
export C_INCLUDE_PATH=$testdir/librdkafka-installation/include
mkdir -p $testdir/librdkafka-installation

if [[ ! -d confluent-kafka-python ]]; then
    git clone https://github.com/confluentinc/confluent-kafka-python
fi

echo "Setting up virtualenv in $venv"
if [[ ! -d $venv ]]; then
    python3 -m venv $venv
fi
source $venv/bin/activate
pip install -U pip
pip install -r $testdir/../../requirements/requirements-soaktest.txt
deactivate

./build.sh $librdkafka_branch $python_branch

source $venv/bin/activate
echo "Verifying python client installation"
python -c "import confluent_kafka; print(confluent_kafka.version(), confluent_kafka.libversion())"
deactivate

echo "All done, activate the virtualenv in $venv before running the client:"
echo "source $venv/bin/activate"

