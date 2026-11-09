#!/bin/sh

# Run inside each Linux cibuildwheel container before building any Python ABI.
set -eu

if ! ldd --version 2>&1 | grep -q musl; then
    exit 0
fi

case "$(uname -m)" in
    x86_64) arch=x64 ;;
    aarch64) arch=arm64 ;;
    *) echo "No Alpine librdkafka redistributable for $(uname -m)" >&2; exit 1 ;;
esac

native_dir="dest/runtimes/linux-$arch/native"
# Both the link-time name and runtime SONAME must resolve to the musl build.
cp "$native_dir/alpine-librdkafka.so" "$native_dir/librdkafka.so"
cp "$native_dir/alpine-librdkafka.so" "$native_dir/librdkafka.so.1"
