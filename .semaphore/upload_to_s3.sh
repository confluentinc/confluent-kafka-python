#!/bin/bash
if [ -z "$1" -o -z "$2" ]; then
    echo "usage: $0 <src> <dst>"
    exit 1
fi

FROM=$1
TO=$2

. mk-include/bin/vault-setup
. vault-sem-get-secret aws_credentials
aws sts get-caller-identity
aws s3 cp --recursive "$FROM" "s3://librdkafka-ci-packages/$TO"