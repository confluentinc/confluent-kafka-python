#!/bin/bash
set -e

DOCKER_REPOSITORY_DEFAULT=${DOCKER_REPOSITORY:-docker.io/library/njc-py-soak-tests}
NAMESPACE=njc-soak-tests

for var in LK_VERSION CKPY_VERSION CC_BOOSTRAP_SERVERS CC_USERNAME CC_PASSWORD \
DOCKER_REPOSITORY_DEFAULT; do
    VAR_VALUE=$(eval echo \$$var)
    if [ -z "$VAR_VALUE" ]; then
        echo "env variable $var is required"
        exit 1
    fi
done

TAG=${LK_VERSION}-${CKPY_VERSION}

docker build . --build-arg LK_VERSION=${LK_VERSION} \
--build-arg CKPY_VERSION=${CKPY_VERSION} \
-t ${DOCKER_REPOSITORY_DEFAULT}:${TAG}

if [ ! -z "$DOCKER_REPOSITORY" ]; then
    docker push ${DOCKER_REPOSITORY}:${TAG}
fi

echo helm upgrade --install njc-py-soak-tests njc-py-soak-tests \
--set "cluster.bootstrapServers=${CC_BOOSTRAP_SERVERS}" \
--set "cluster.username=${CC_USERNAME}" \
--set "cluster.password=${CC_PASSWORD}" \
--set "image.repository=${DOCKER_REPOSITORY_DEFAULT}" \
--set "image.tag=${TAG}" \
--namespace "${NAMESPACE}" --create-namespace
