#!/usr/bin/env bash

TEST_SOURCE="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"
source ${TEST_SOURCE}/../docker/.env

#start cluster
${TEST_SOURCE}/cluster_up.sh

for mode in "$@"
do
    modes="${modes} --${mode}"
done

curl http://localhost:8081/subjects

echo "Executing test suite..."
python ${TEST_SOURCE}/../examples/integration_test.py ${modes} ${DOCKER_CONF}/testconf.json

#teardown cluster
${TEST_SOURCE}/cluster_down.sh
