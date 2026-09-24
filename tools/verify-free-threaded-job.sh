#!/bin/bash
#
# Body of the "Verify free threaded" job in the Linux and macOS Wheel
# Verification blocks of .semaphore/semaphore.yml: install the cp314t wheel,
# run the free-threaded wheel checks (tools/verify-free-threaded-wheel.sh) and then the unit suite.
#
# Run from the repo root with uv on PATH. Expects OS_NAME (linux|osx) and ARCH
# (x64|arm64, or s390x on linux) as the Wheel blocks set them, and
# LIBRDKAFKA_VERSION for the wheel checks.

set -eu

: "${OS_NAME:?set OS_NAME to linux or osx}"
: "${ARCH:?set ARCH to x64, arm64 or s390x}"

# cibuildwheel tags Linux wheels manylinux_*_{x86_64,aarch64,s390x} and macOS
# wheels macosx_*_{x86_64,arm64}.
case "$OS_NAME-$ARCH" in
    linux-x64)   wheel_glob='*-cp314t-manylinux*x86_64.whl' ;;
    linux-arm64) wheel_glob='*-cp314t-manylinux*aarch64.whl' ;;
    linux-s390x) wheel_glob='*-cp314t-manylinux*s390x.whl' ;;
    osx-x64)     wheel_glob='*-cp314t-macosx*x86_64.whl' ;;
    osx-arm64)   wheel_glob='*-cp314t-macosx*arm64.whl' ;;
    *) echo "$0: unsupported OS_NAME-ARCH '$OS_NAME-$ARCH'" >&2; exit 1 ;;
esac
echo "Verifying the free-threaded wheel matching $wheel_glob for $OS_NAME-$ARCH"

# s390x: the schema-registry stack's deps (cryptography, via authlib and trivup's
# jwcrypto) publish no s390x wheels, so install the reduced set (see
# requirements/requirements-tests-install-nogil-s390x.txt). tests/conftest.py
# skips collecting tests/schema_registry on s390x to match.
tests_install_reqs=requirements/requirements-tests-install-nogil.txt
if [[ $ARCH == s390x ]]; then
    tests_install_reqs=requirements/requirements-tests-install-nogil-s390x.txt
fi

uv venv _venv314t --python 3.14t
source _venv314t/bin/activate
uv pip install -r "$tests_install_reqs"

artifact pull workflow artifacts
# Fail fast, with a clear message, if the pull brought no wheel tarballs at all.
ls artifacts/*.tgz
(cd artifacts && ls *.tgz | xargs -n1 tar -xvf)
# Unquoted on purpose: the glob must expand, and ls fails if nothing matches.
ls artifacts/wheelhouse/$wheel_glob
uv pip install --no-index --find-links artifacts/wheelhouse confluent-kafka

tools/verify-free-threaded-wheel.sh
python -m pytest tests/ --ignore=tests/integration --ignore=tests/test_unasync.py --timeout 1200
