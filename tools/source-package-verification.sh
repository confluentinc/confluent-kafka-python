#!/bin/bash
#
#
# Source Package Verification
#
set -e

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
    flake8 --exclude ./_venv,*_pb2.py
    make docs
    python -m pytest --timeout 1200 --ignore=dest
else
    python -m pytest --timeout 1200 --ignore=dest --ignore=tests/integration
fi
