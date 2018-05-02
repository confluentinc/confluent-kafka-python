#!/usr/bin/env bash

# Used by Jenkins in post step to upload results from ./run-tests.sh

if [[ $# -lt 4 ]]
then
   echo "usage: upload-system-test-results <kafka|muckrake> branch repo test_path [ducktape_args]"
   exit -1
fi

if [[ -z $WORKSPACE ]]; then
  echo "This should be run by Jenkins only"
  exit 1
fi

set -e

. jenkins-common/resources/scripts/extract-iam-credential.sh

set -x

PROJECT=$1
BRANCH=$2
REPO=$3
TEST_PATH=$4
DUCKTAPE_ARGS=${DUCKTAPE_ARGS:-$5}  # Existing caller might be setting it via env...
DIRECTORY=$WORKSPACE/results
BUILD_URL=$JOB_URL

subdir=$(ls -tr $DIRECTORY | tail -1)

bucket=$(python $WORKSPACE/jenkins-common/scripts/system-tests/test-result-storage/get_upload_location.py \
            --project $PROJECT --branch $BRANCH)
bash -x $WORKSPACE/jenkins-common/scripts/system-tests/test-result-storage/s3-upload.sh \
    --bucket $bucket --directory $DIRECTORY
echo Uploaded to S3 bucket: $bucket
