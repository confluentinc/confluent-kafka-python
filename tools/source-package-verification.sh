#!/bin/bash
#
#
# Source Package Verification
#
set -e

pip install poetry
poetry install --all-extras 
poetry build

lib_dir=dest/runtimes/$OS_NAME-$ARCH/native
tools/wheels/install-librdkafka.sh "${LIBRDKAFKA_VERSION#v}" dest
export CFLAGS="$CFLAGS -I${PWD}/dest/build/native/include"
export LDFLAGS="$LDFLAGS -L${PWD}/${lib_dir}"
export LD_LIBRARY_PATH="$LD_LIBRARY_PATH:$PWD/$lib_dir"
export DYLD_LIBRARY_PATH="$DYLD_LIBRARY_PATH:$PWD/$lib_dir"

python3 -m pip install .

if [[ $OS_NAME == linux && $ARCH == x64 ]]; then
    if [[ -z $TEST_CONSUMER_GROUP_PROTOCOL ]]; then
        flake8 --exclude ./_venv,*_pb2.py
        pip install -r requirements/requirements-docs.txt
        make docs
    fi
    python -m pytest --timeout 1200 --ignore=dest
else
    python -m pytest --timeout 1200 --ignore=dest --ignore=tests/integration
fi
