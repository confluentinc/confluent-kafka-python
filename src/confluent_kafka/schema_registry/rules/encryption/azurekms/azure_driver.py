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
from typing import Any, Dict, NamedTuple, Optional
from urllib.parse import urlparse

from azure.core.credentials import TokenCredential
from azure.identity import ClientSecretCredential, DefaultAzureCredential
from azure.keyvault.keys import KeyClient
from tink import KmsClient, TinkError

from confluent_kafka.schema_registry.rules.encryption.azurekms.azure_client import AzureKmsClient
from confluent_kafka.schema_registry.rules.encryption.kms_driver_registry import KmsDriver, register_kms_driver

_PREFIX = "azure-kms://"
_TENANT_ID = 'tenant.id'
_CLIENT_ID = 'client.id'
_CLIENT_SECRET = 'client.secret'

# Enables making a DEK's encrypted key material self-describing with respect to which exact Azure
# Key Vault key version wrapped it (see AzureKmsAead), matching the same self-description property
# AWS KMS and GCP KMS ciphertext already provide natively. Set as a kek kms_props entry.
ENCRYPT_AZURE_KEY_VERSION_SAVE = "encrypt.azure.key.version.save"


class _KeyVaultId(NamedTuple):
    vault_url: str
    name: str
    version: Optional[str]


def _parse(kms_key_id: str) -> _KeyVaultId:
    parsed = urlparse(kms_key_id)
    if not parsed.scheme or not parsed.netloc:
        raise TinkError(f"invalid Azure Key Vault key id: {kms_key_id}")
    segments = [s for s in parsed.path.split('/') if s]
    if len(segments) < 2 or len(segments) > 3 or segments[0] != 'keys':
        raise TinkError(f"invalid Azure Key Vault key id: {kms_key_id}")
    vault_url = f"{parsed.scheme}://{parsed.netloc}"
    version = segments[2] if len(segments) == 3 else None
    return _KeyVaultId(vault_url, segments[1], version)


def is_versionless(kms_key_id: str) -> bool:
    """Returns true if kms_key_id has no explicit version segment.

    Used to warn when ENCRYPT_AZURE_KEY_VERSION_SAVE is not enabled for a versionless key, without
    performing any actual resolution (no KeyClient call).
    """
    return _parse(kms_key_id).version is None


def with_version(kms_key_id: str, version: str) -> str:
    """Combines kms_key_id (versionless or versioned; only the vault and key name are used) with
    an explicit version, returning the full versioned key identifier.

    Used to reconstruct a target for a version extracted from an already-wrapped DEK, which may
    differ from whatever get_versioned_key_id currently resolves to (e.g. after a rotation).
    """
    parsed = _parse(kms_key_id)
    return f"{parsed.vault_url}/keys/{parsed.name}/{version}"


def get_versioned_key_id(conf: Dict[str, Any], kms_key_id: str) -> str:
    """Resolves a possibly-versionless Azure Key Vault key identifier (e.g.
    "https://vault.vault.azure.net/keys/name") into the concrete, currently-enabled version (e.g.
    "https://vault.vault.azure.net/keys/name/<version>"). If kms_key_id already includes a version
    segment, it is returned unchanged and no call is made.

    This exists because, unlike AWS KMS and GCP KMS, Azure Key Vault's wrap/unwrap operations
    address an explicit key version and do not embed that version in the returned ciphertext, so a
    caller that only ever uses a versionless reference has no way to know which version encrypted a
    given DEK once the key has been rotated.
    """
    parsed = _parse(kms_key_id)
    if parsed.version is not None:
        # Already versioned; respect the explicitly pinned config as-is.
        return kms_key_id
    client = KeyClient(vault_url=parsed.vault_url, credential=_get_credentials(conf))
    try:
        key = client.get_key(parsed.name)
    except Exception as e:
        raise TinkError(
            f"Failed to resolve Azure Key Vault key id for key name '{parsed.name}' in vault "
            f"{parsed.vault_url}"
        ) from e
    if key is None or key.id is None:
        raise TinkError(
            f"Failed to resolve Azure Key Vault key id for key name '{parsed.name}' in vault "
            f"{parsed.vault_url}"
        )
    resolved_id = key.id
    if _parse(resolved_id).version is None:
        raise TinkError(f"resolved Azure Key Vault key id is missing a version segment: {resolved_id}")
    return resolved_id


def _get_credentials(conf: Dict[str, Any]) -> TokenCredential:
    tenant_id = conf.get(_TENANT_ID)
    client_id = conf.get(_CLIENT_ID)
    client_secret = conf.get(_CLIENT_SECRET)
    if tenant_id is not None and client_id is not None and client_secret is not None:
        return ClientSecretCredential(tenant_id, client_id, client_secret)
    return DefaultAzureCredential()


class AzureKmsDriver(KmsDriver):
    def __init__(self) -> None:
        pass

    def get_key_url_prefix(self) -> str:
        return _PREFIX

    def new_kms_client(self, conf: Dict[str, Any], key_url: Optional[str]) -> KmsClient:
        uri_prefix = _PREFIX
        if key_url is not None:
            uri_prefix = key_url
        return AzureKmsClient(uri_prefix, _get_credentials(conf), conf)

    @classmethod
    def register(cls) -> None:
        register_kms_driver(AzureKmsDriver())
