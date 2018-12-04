#!/bin/bash
#
#

# Bootstrap EC2 instance (Ubuntu 18.04) for soak client use
#
# Usage:
#  $0 <python-branch/tag> <librdkafka-branch/tag>

set -e

if [[ $# != 2 ]]; then
    echo "Usage: $0 <python-client-branch/tag> <librdkafka-branch/tag>"
    exit 1
fi

python_branch=$1
librdkafka_branch=$2

sudo apt update
sudo apt install -y make gcc g++ zlib1g-dev libssl-dev libzstd-dev screen \
     python3.6-dev python3-pip python3-virtualenv

pushd $HOME

if [[ ! -d confluent-kafka-python ]]; then
    git clone https://github.com/confluentinc/confluent-kafka-python
fi

pushd confluent-kafka-python

git checkout $python_branch

echo "Installing librdkafka $librdkafka_branch"
tools/bootstrap-librdkafka.sh --require-ssl $librdkafka_branch /usr

echo "Installing interceptors"
tools/install-interceptors.sh

venv=$HOME/venv
echo "Setting up virtualenv in $venv"
if [[ ! -d $venv ]]; then
    virtualenv -p python3.6 $venv
fi
source $venv/bin/activate

pip install -U pip

pip install -v .

pip install -r tests/soak/requirements.txt

popd # ..python

echo "Verifying python client installation"
python -c "import confluent_kafka; print(confluent_kafka.version(), confluent_kafka.libversion())"

deactivate

popd # $HOME

echo "All done, activate the virtualenv in $venv before running the client:"
echo "source $venv/bin/activate"

