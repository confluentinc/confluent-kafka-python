#!/bin/bash

set -eu

DOCKER_BIN="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"
source ${DOCKER_BIN}/../.env

echo "Destroying cluster.."
docker-compose -f ${DOCKER_FILE} down -v --remove-orphans
