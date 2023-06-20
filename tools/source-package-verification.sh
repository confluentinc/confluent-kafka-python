#!/bin/bash
#
#
# Source Package Verification
#

pip install -r docs/requirements.txt
pip install -U protobuf
pip install -r tests/requirements.txt
pip install -U build

lib_dir=dest/runtimes/$OS_NAME-$ARCH/native
tools/wheels/install-librdkafka.sh "${LIBRDKAFKA_VERSION#v}" dest
export CFLAGS="$CFLAGS -I${PWD}/dest/build/native/include"
export LDFLAGS="$LDFLAGS -L${PWD}/${lib_dir}"
export LD_LIBRARY_PATH="$LD_LIBRARY_PATH:$PWD/$lib_dir"
export DYLD_LIBRARY_PATH="$DYLD_LIBRARY_PATH:$PWD/$lib_dir"

python3 -m build
if [[ $OS_NAME == linux && $ARCH == x64 ]]; then
    flake8 --exclude ./_venv
    make docs
    python -m pytest --timeout 600 --ignore=dest
else
    python -m pytest --timeout 600 --ignore=dest --ignore=tests/integration
fi
