#!/bin/bash

set -eu

PY_DOCKER_BIN="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"
source ${PY_DOCKER_BIN}/../.env.sh

echo "Destroying cluster.."
docker-compose -f $PY_DOCKER_COMPOSE_FILE down -v --remove-orphans
