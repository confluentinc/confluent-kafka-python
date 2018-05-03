#!/usr/bin/env bash

# Used by Jenkins in post step to cleanup after ./run-tests.sh

if [[ -z $WORKSPACE ]]; then
  echo "This should be run by Jenkins only"
  exit 1
fi

set -e

. jenkins-common/resources/scripts/extract-iam-credential.sh

set -x

KAFKA_DIR=$WORKSPACE/kafka

if [ -d $KAFKA_DIR ]; then
  cd $KAFKA_DIR
  vagrant destroy -f
fi
