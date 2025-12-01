#!/usr/bin/env bash

export PY_DOCKER_SOURCE="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"
export PY_DOCKER_COMPOSE_FILE=$PY_DOCKER_SOURCE/docker-compose.yaml
export PY_DOCKER_CONTEXT="python-test-$(uuidgen)"
export PY_DOCKER_BIN=$PY_DOCKER_SOURCE/bin
export PY_DOCKER_CONF=$PY_DOCKER_SOURCE/conf
export TLS=$PY_DOCKER_CONF/tls

export MY_BOOTSTRAP_SERVER_ENV=localhost:29092
export MY_SCHEMA_REGISTRY_URL_ENV=http://$(hostname):8081
export MY_SCHEMA_REGISTRY_SSL_URL_ENV=https://$(hostname -f):8082
export MY_SCHEMA_REGISTRY_SSL_CA_LOCATION_ENV=$TLS/ca-cert
export MY_SCHEMA_REGISTRY_SSL_CERTIFICATE_LOCATION_ENV=$TLS/client.pem
export MY_SCHEMA_REGISTRY_SSL_KEY_LOCATION_ENV=$TLS/client.key
export MY_SCHEMA_REGISTRY_SSL_KEY_WITH_PASSWORD_LOCATION_ENV=$TLS/client_with_password.key
export MY_SCHEMA_REGISTRY_SSL_KEY_PASSWORD="abcdefgh"