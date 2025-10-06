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
import os

from tink import KmsClient

from confluent_kafka.schema_registry.rules.encryption.hcvault.hcvault_client import \
    HcVaultKmsClient
from confluent_kafka.schema_registry.rules.encryption.kms_driver_registry import \
    KmsDriver, register_kms_driver

_PREFIX = "hcvault://"
_TOKEN_ID = "token.id"
_NAMESPACE = "namespace"
_APPROLE_ROLE_ID = "approle.role.id"
_APPROLE_SECRET_ID = "approle.secret.id"


class HcVaultKmsDriver(KmsDriver):
    def __init__(self):
        pass

    def get_key_url_prefix(self) -> str:
        return _PREFIX

    def new_kms_client(self, conf: dict, key_url: str) -> KmsClient:
        uri_prefix = _PREFIX
        if key_url is not None:
            uri_prefix = key_url
        token = conf.get(_TOKEN_ID)
        if token is None:
            token = os.getenv("VAULT_TOKEN")
        namespace = conf.get(_NAMESPACE)
        if namespace is None:
            namespace = os.getenv("VAULT_NAMESPACE")
        role_id = conf.get(_APPROLE_ROLE_ID)
        if role_id is None:
            role_id = os.getenv("VAULT_APPROLE_ROLE_ID")
        secret_id = conf.get(_APPROLE_SECRET_ID)
        if secret_id is None:
            secret_id = os.getenv("VAULT_APPROLE_SECRET_ID")
        return HcVaultKmsClient(uri_prefix, token, namespace, role_id, secret_id)

    @classmethod
    def register(cls):
        register_kms_driver(HcVaultKmsDriver())
