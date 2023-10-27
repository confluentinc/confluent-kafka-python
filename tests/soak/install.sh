#!/bin/bash
set -e

DOCKER_REPOSITORY_DEFAULT=${DOCKER_REPOSITORY:-docker.io/library/njc-py-soak-tests}
NAMESPACE=njc-soak-tests
NOCACHE=${NOCACHE:---no-cache}

for var in LK_VERSION CKPY_VERSION CC_BOOSTRAP_SERVERS CC_USERNAME CC_PASSWORD \
DOCKER_REPOSITORY_DEFAULT; do
    VAR_VALUE=$(eval echo \$$var)
    if [ -z "$VAR_VALUE" ]; then
        echo "env variable $var is required"
        exit 1
    fi
done

TAG=${LK_VERSION}-${CKPY_VERSION}

COMMAND="docker build . $NOCACHE --build-arg LK_VERSION=${LK_VERSION} \
--build-arg CKPY_VERSION=${CKPY_VERSION} \
-t ${DOCKER_REPOSITORY_DEFAULT}:${TAG}"
echo $COMMAND
$COMMAND

if [ ! -z "$DOCKER_REPOSITORY" ]; then
    COMMAND="docker push ${DOCKER_REPOSITORY}:${TAG}"
    echo $COMMAND
    $COMMAND
fi

if [ "$(uname -p)" = "x86_64" ]; then
    NODE_ARCH="amd64"
else
    NODE_ARCH="arm64"
fi

COMMAND="helm upgrade --install njc-py-soak-tests njc-py-soak-tests \
--set "cluster.bootstrapServers=${CC_BOOSTRAP_SERVERS}" \
--set "cluster.username=${CC_USERNAME}" \
--set "cluster.password=${CC_PASSWORD}" \
--set "image.repository=${DOCKER_REPOSITORY_DEFAULT}" \
--set "testid=${TESTID}" \
--set "fullnameOverride=njc-py-soak-tests-${TESTID}" \
--set "image.tag=${TAG}" \
--set "nodeSelector.kubernetes\\.io/arch=${NODE_ARCH}" \
--namespace "${NAMESPACE}" --create-namespace"
echo $COMMAND
$COMMAND
