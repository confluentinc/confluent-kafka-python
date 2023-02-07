#!/bin/bash
#
#
# Source Package Verification
#

pip install -r docs/requirements.txt
pip install -U protobuf
pip install -r tests/requirements.txt

tools/wheels/install-librdkafka.sh "${LIBRDKAFKA_VERSION#v}" dest
if [[ $OS_NAME == windows]]; then
    export include="$include -I${PWD}\dest\build\native\include"
    export lib="$LDFLAGS -L${PWD}\dest\build\native\lib\win\%ARCH%\win-${ARCH}-Release\v142"
    export DLL_DIR="$DLL_DIR -L${PWD}\dest\runtimes\win-${ARCH}\native"
else
    lib_dir=dest/runtimes/$OS_NAME-$ARCH/native
    export CFLAGS="$CFLAGS -I${PWD}/dest/build/native/include"
    export LDFLAGS="$LDFLAGS -L${PWD}/${lib_dir}"
    export LD_LIBRARY_PATH="$LD_LIBRARY_PATH:$PWD/$lib_dir"
    export DYLD_LIBRARY_PATH="$DYLD_LIBRARY_PATH:$PWD/$lib_dir"
fi

python setup.py build && python setup.py install
if [[ $OS_NAME == linux && $ARCH == x64 ]]; then
    flake8
    make docs
    python -m pytest --timeout 600 --ignore=dest
else
    python -m pytest --timeout 600 --ignore=dest --ignore=tests/integration
fi
