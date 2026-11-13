#!/bin/bash

# Verify repaired wheels in clean Alpine runtimes without build-time libraries or compilers.
set -eu

: "${LIBRDKAFKA_VERSION:?set LIBRDKAFKA_VERSION to the version bundled in the wheels}"
wheeldir=$(cd "${1:?Usage: tools/test-musllinux.sh <wheel-directory>}" && pwd)
project_dir=$(cd "$(dirname "$0")/.." && pwd)

case "${ARCH:-$(uname -m)}" in
    x64|x86_64) wheel_arch=x86_64; docker_platform=linux/amd64 ;;
    arm64|aarch64) wheel_arch=aarch64; docker_platform=linux/arm64 ;;
    *) echo "Musllinux verification supports only x64 and arm64" >&2; exit 1 ;;
esac

# Free-threaded 3.14 is checked by cibuildwheel; Docker Hub has no python:3.14t-alpine image.
for version in 3.8 3.9 3.10 3.11 3.12 3.13 3.14 3.14t; do
    abi="cp${version//./}"
    python_tag="${abi%t}"
    wheels=("$wheeldir"/*-"$python_tag"-"$abi"-musllinux_1_2_"$wheel_arch".whl)
    if [[ ${#wheels[@]} != 1 || ! -f ${wheels[0]} ]]; then
        echo "Expected exactly one $abi musllinux_1_2_$wheel_arch wheel in $wheeldir" >&2
        exit 1
    fi
    if [[ $version == *t ]]; then
        # Require the artifact even though its runtime checks run inside cibuildwheel.
        continue
    fi

    docker run --rm --platform "$docker_platform" \
        -v "$project_dir:/io:ro" -v "$wheeldir:/wheels:ro" \
        -e LIBRDKAFKA_VERSION -e "WHEEL=/wheels/$(basename "${wheels[0]}")" \
        -w /tmp "python:$version-alpine" sh -ec '
            python -m pip install --only-binary=:all: "$WHEEL" pytest
            python /io/tools/wheels/verify-wheel.py "$LIBRDKAFKA_VERSION"
            python -m pytest /io/tests/test_error.py
        '
done
