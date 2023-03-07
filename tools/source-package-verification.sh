#!/bin/bash
#
#
# Source Package Verification
#

pip install -r docs/requirements.txt
pip install -U protobuf
pip install -r tests/requirements.txt

lib_dir=dest/runtimes/$OS_NAME-$ARCH/native
tools/wheels/install-librdkafka.sh "${LIBRDKAFKA_VERSION#v}" dest
export CFLAGS="$CFLAGS -I${PWD}/dest/build/native/include"
export LDFLAGS="$LDFLAGS -L${PWD}/${lib_dir}"
export LD_LIBRARY_PATH="$LD_LIBRARY_PATH:$PWD/$lib_dir"
export DYLD_LIBRARY_PATH="$DYLD_LIBRARY_PATH:$PWD/$lib_dir"

python setup.py build && python setup.py install
if [[ $OS_NAME == linux && $ARCH == x64 ]]; then
    flake8 --exclude ./_venv
    make docs
    python -m pytest --timeout 600 --ignore=dest
    echo 'Running Integration Tests for SSL Verification'
    rm -rf tests/docker/conf/tls
    cd tests/docker
    source .env.sh
    cd bin
    certify.sh
    cd ../
    docker-compose up -d
    sleep 50
    cd ../integration
    python3 integration_test.py --avro-https testconf.json
else
    python -m pytest --timeout 600 --ignore=dest --ignore=tests/integration
fi
