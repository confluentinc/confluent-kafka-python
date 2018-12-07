#!/usr/bin/env bash -eu

DOCKER_BIN="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"
export PASS="abcdefgh"

source ${DOCKER_BIN}/../.env

mkdir -p ${TLS}

if [[ -f ${TLS}/ca-cert ]]; then
    echo "${TLS}/ca-cert found; skipping certificate generation.."
    exit 0
fi

HOST=$(hostname -f)

echo "Creating ca-cert..."
${DOCKER_BIN}/gen-ssl-certs.sh ca ${TLS}/ca-cert ${HOST}
echo "Creating server cert..."
${DOCKER_BIN}/gen-ssl-certs.sh -k server ${TLS}/ca-cert  ${TLS}/ ${HOST} ${HOST}
echo "Creating client cert..."
${DOCKER_BIN}/gen-ssl-certs.sh client ${TLS}/ca-cert ${TLS}/ ${HOST} ${HOST}

echo "Creating key ..."
openssl rsa -in ${TLS}/client.key -out ${TLS}/client.key  -passin pass:${PASS}

