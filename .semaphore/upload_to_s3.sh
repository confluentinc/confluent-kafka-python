#!/bin/bash
DEFAULT_AWS_ROLE_ARN=arn:aws:iam::368821881613:role/ConfluentDevRole
if [ -z "$1" -o -z "$2" ]; then
    echo "usage: $0 <src> <dst>"
    exit 1
fi
if [ -z "$AWS_ROLE_ARN" ]; then
    export AWS_ROLE_ARN=$DEFAULT_AWS_ROLE_ARN
fi

FROM=$1
TO=$2

make install-vault
. mk-include/bin/vault-setup
. vault-sem-get-secret aws_credentials
#aws sts assume-role --role-arn "$AWS_ROLE_ARN" --role-session-name "AWSCLI-Session"
aws sts get-caller-identity
echo aws s3 ls s3://librdkafka-ci-packages
aws s3 cp --recursive "$FROM" "s3://librdkafka-ci-packages/confluent-kafka-python/$TO"