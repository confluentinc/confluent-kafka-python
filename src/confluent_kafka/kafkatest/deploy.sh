#!/bin/bash
#
# Deploy the confluent-kafka-python verifiable clients on a Ducktape worker.
#
# This branch requires a newer librdkafka than any distro package provides
# (KIP-932 share-consumer APIs, IncrementalAlterConfigs, the modern admin
# types, etc.). So we do NOT use the apt librdkafka-dev package. Instead we
# build the librdkafka that's mounted into the container and link the C
# extension against it.
#
# Both the confluent-kafka-python repo and the librdkafka repo must be mounted
# read-write into the container (see README for the ducker-ak -v patch):
#   ${REPO_DIR}        confluent-kafka-python   (default /confluent-kafka-python)
#   ${LIBRDKAFKA_DIR}  librdkafka               (default /librdkafka)
#
# The verifiable clients run from a virtualenv at ${VENV_DIR}, which
# globals.json points exec_cmd at.
#
# Idempotency / concurrency:
#   1. A per-node sentinel (${NODE_SENTINEL}, in /tmp) makes deploy.sh a no-op
#      on a node that's already been deployed (Ducktape runs it before the
#      first exec on each node).
#   2. A flock on the librdkafka mount serializes the librdkafka build across
#      nodes sharing the mount, and a shared sentinel lets later nodes skip
#      the rebuild.
#
# Honors:
#   REPO_DIR        (default /confluent-kafka-python) repo mount point
#   LIBRDKAFKA_DIR  (default /librdkafka) librdkafka source mount point
#   VENV_DIR        (default /opt/cfk-python/venv) virtualenv location
#   DEPLOY_SKIP_APT=1  skip apt (for pre-provisioned images)

set -euo pipefail

REPO_DIR="${REPO_DIR:-/confluent-kafka-python}"
LIBRDKAFKA_DIR="${LIBRDKAFKA_DIR:-/librdkafka}"
VENV_DIR="${VENV_DIR:-/opt/cfk-python/venv}"
NODE_SENTINEL="/tmp/cfk-python-deploy.done"
LRK_LOCK="${LIBRDKAFKA_DIR}/.cfk-build-lock"
LRK_SENTINEL="${LIBRDKAFKA_DIR}/.cfk-built"
PYTHON="${PYTHON:-python3}"

if [[ -f "${NODE_SENTINEL}" ]] && [[ -x "${VENV_DIR}/bin/python" ]]; then
    echo "deploy.sh: node already deployed (${NODE_SENTINEL}); skipping"
    exit 0
fi

if [[ ! -d "${LIBRDKAFKA_DIR}" ]]; then
    echo "deploy.sh: librdkafka not mounted at ${LIBRDKAFKA_DIR}." >&2
    echo "deploy.sh: mount it with ducker-ak (-v) or set LIBRDKAFKA_DIR." >&2
    exit 1
fi

# Build/runtime deps. No librdkafka-dev: we build librdkafka ourselves.
if [[ "${DEPLOY_SKIP_APT:-0}" != "1" ]]; then
    sudo apt-get update
    sudo apt-get install -y --no-install-recommends \
        build-essential \
        pkg-config \
        python3 \
        python3-dev \
        python3-venv \
        python3-pip \
        libssl-dev \
        libsasl2-dev \
        zlib1g-dev \
        libzstd-dev
fi

# Build and install the mounted librdkafka into /usr/local, serialized across
# nodes that share the mount. configure/make are run under flock; the shared
# sentinel lets later nodes skip straight to `make install` (cheap, per-node,
# since /usr/local is container-local).
exec 9>"${LRK_LOCK}"
echo "deploy.sh: acquiring librdkafka build lock..."
flock 9

if [[ ! -f "${LRK_SENTINEL}" ]] || [[ ! -f "${LIBRDKAFKA_DIR}/src/librdkafka.a" ]]; then
    pushd "${LIBRDKAFKA_DIR}" >/dev/null
    # Clean first: a bind-mounted tree may carry arch-mismatched objects from
    # a developer's host build (e.g. macOS), which break incremental make.
    make clean 2>/dev/null || true
    ./configure
    make -j"$(nproc)" libs
    popd >/dev/null
    touch "${LRK_SENTINEL}"
fi

# Install into this container's /usr/local (per-container, so every node does
# it) and refresh the linker cache so the runtime finds librdkafka.so.
sudo make -C "${LIBRDKAFKA_DIR}" install
sudo ldconfig

# Lock released when fd 9 closes at exit.

# Create the virtualenv (container-local, outside the shared mounts).
sudo mkdir -p "$(dirname "${VENV_DIR}")"
sudo chown "$(id -u):$(id -g)" "$(dirname "${VENV_DIR}")"
if [[ ! -x "${VENV_DIR}/bin/python" ]]; then
    "${PYTHON}" -m venv "${VENV_DIR}"
fi
# shellcheck disable=SC1091
source "${VENV_DIR}/bin/activate"
pip install -U pip wheel

# Build the C extension against the just-installed librdkafka. /usr/local is
# on the default search path, but pass the flags explicitly so the build
# doesn't accidentally pick up an older system copy.
C_INCLUDE_PATH="/usr/local/include${C_INCLUDE_PATH:+:$C_INCLUDE_PATH}" \
LIBRARY_PATH="/usr/local/lib${LIBRARY_PATH:+:$LIBRARY_PATH}" \
LD_LIBRARY_PATH="/usr/local/lib${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}" \
    pip install -e "${REPO_DIR}"

# Sanity-check the clients respond to --help before declaring success.
python -m confluent_kafka.kafkatest.verifiable_producer --help >/dev/null
python -m confluent_kafka.kafkatest.verifiable_share_consumer --help >/dev/null

touch "${NODE_SENTINEL}"
echo "deploy.sh: deploy complete (venv at ${VENV_DIR})"
