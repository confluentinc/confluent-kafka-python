#!/usr/bin/env bash

TEST_SOURCE="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"
source ${TEST_SOURCE}/../docker/.env

#start cluster
${TEST_SOURCE}/cluster_up.sh

echo "Executing test suite..."
${TEST_SOURCE}/run_test.sh

#teardown cluster
${TEST_SOURCE}/cluster_down.sh
