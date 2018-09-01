#!/usr/bin/env bash

TEST_SOURCE="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"
source ${TEST_SOURCE}/../docker/.env

echo "Destroying cluster.."
docker-compose -f ${DOCKER_CONTEXT} down -v --remove-orphans
