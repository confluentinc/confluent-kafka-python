#!/usr/bin/env bash

DOCKER_BIN="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"
export PASS="abcdefgh"
source ${DOCKER_BIN}/../.env

if [ -f ${TLS}/ca-cert ]; then
    echo "${TLS}/ca-cert found; skipping certificate generation.."
    exit 0
fi

# Clean up old certs
#for file in $(ls ${TLS});do
#        rm ${TLS}/${file}
#done

echo "Creating ca-cert..."
${DOCKER_BIN}/gen-ssl-certs.sh ca ${TLS}/ca-cert $(hostname -f)
echo "Creating server cert..."
${DOCKER_BIN}/gen-ssl-certs.sh -k server ${TLS}/ca-cert  ${TLS}/ $(hostname -f) $(hostname -f)
echo "Creating client cert..."
${DOCKER_BIN}/gen-ssl-certs.sh client ${TLS}/ca-cert ${TLS}/ $(hostname -f) $(hostname -f)

echo "Creating key ..."
openssl rsa -in ${TLS}/client.key -out ${TLS}/client.key  -passin pass:${PASS}

