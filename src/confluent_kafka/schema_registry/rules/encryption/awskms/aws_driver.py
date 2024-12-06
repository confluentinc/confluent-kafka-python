# Copyright 2024 Confluent Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import boto3
import tink

from tink import KmsClient
from tink.integration.awskms import new_client

from confluent_kafka.schema_registry.rules.encryption.kms_driver_registry import \
    KmsDriver, register_kms_driver

_PREFIX = "aws-kms://"
_ACCESS_KEY_ID = "access.key.id"
_SECRET_ACCESS_KEY = "secret.access.key"


class AwsKmsDriver(KmsDriver):
    def __init__(self):
        pass

    def get_key_url_prefix(self) -> str:
        return _PREFIX

    def new_kms_client(self, conf: dict, key_url: str) -> KmsClient:
        uri_prefix = _PREFIX
        if key_url is not None:
            uri_prefix = key_url
        key = conf.get(_ACCESS_KEY_ID)
        secret = conf.get(_SECRET_ACCESS_KEY)

        key_arn = _key_uri_to_key_arn(uri_prefix)
        region = _get_region_from_key_arn(key_arn)
        if key is None or secret is None:
            client = boto3.client('kms', region_name=region)
        else:
            client = boto3.client(
                'kms',
                region_name=region,
                aws_access_key_id=key,
                aws_secret_access_key=secret
            )
        return new_client(boto3_client=client, key_uri=uri_prefix)

    @classmethod
    def register(cls):
        register_kms_driver(AwsKmsDriver())


def _key_uri_to_key_arn(key_uri: str) -> str:
    if not key_uri.startswith(_PREFIX):
        raise tink.TinkError('invalid key URI')
    return key_uri[len(_PREFIX):]


def _get_region_from_key_arn(key_arn: str) -> str:
    # An AWS key ARN is of the form
    # arn:aws:kms:us-west-2:111122223333:key/1234abcd-12ab-34cd-56ef-1234567890ab.
    key_arn_parts = key_arn.split(':')
    if len(key_arn_parts) < 6:
        raise tink.TinkError('invalid key id')
    return key_arn_parts[3]
