#!/usr/bin/env bash

TEST_SOURCE="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"
source ${TEST_SOURCE}/../docker/.env

echo "Configure Environment..."
source ${DOCKER_SOURCE}/.env

echo "Generate SSL certs..."
${DOCKER_BIN}/certify.sh

echo "Deploying cluster..."
docker-compose -f ${DOCKER_CONTEXT} up -d

echo "Setting throttle for throttle test..."
docker-compose -f ${DOCKER_CONTEXT} exec kafka sh -c "
        /usr/bin/kafka-configs  --zookeeper zookeeper:2181 \
                --alter --add-config 'producer_byte_rate=1,consumer_byte_rate=1,request_percentage=001' \
                --entity-name throttled_client --entity-type clients"

