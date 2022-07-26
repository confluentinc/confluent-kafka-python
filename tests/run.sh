#!/bin/bash

set -eu

TEST_SOURCE="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"
DOCKER_BIN=${TEST_SOURCE}/docker/bin

cleanup() {
        ${DOCKER_BIN}/cluster_down.sh
}

trap cleanup 0 2 3 6 15

source ${DOCKER_BIN}/../.env.sh

if [[ ${1:-} == "help" ]]; then
    python ${TEST_SOURCE}/integration/integration_test.py --help
    exit 0
fi

start_cluster() {
    ${DOCKER_BIN}/cluster_up.sh
}

run_tox() {
    start_cluster
    echo "Executing tox $@"
    cd ${TEST_SOURCE}
    tox -r "$@"
}

run_native() {
    pip install -v .[avro]
    start_cluster

    for mode in "$@"; do
        modes="${modes:-} --${mode}"
    done

    echo "Executing test modes $@"
    python ${TEST_SOURCE}/integration/integration_test.py ${modes:-} ${TEST_SOURCE}/integration/testconf.json
}

run_unit() {
    py.test -v --timeout 20 --ignore=tmp-build
}

case ${1:-} in
    "unit")
        run_unit
        ;;
    "tox")
        shift
        run_tox $@
        ;;
    "all")
        shift
        run_unit $@
        run_native $@
        ;;
    "native")
        run_native $@
        ;;
    *)
        echo $"Usage: $0 {unit|tox|native|all}"
        exit 1
        ;;
esac
