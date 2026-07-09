# Copyright 2024 Confluent Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""A client for Azure Key Vault KMS."""

import logging
from typing import Any, Callable, Dict, Optional, Tuple

import tink
from azure.core.credentials import TokenCredential
from azure.keyvault.keys.crypto import CryptographyClient, EncryptionAlgorithm
from tink import aead

from confluent_kafka.schema_registry.rules.encryption.azurekms import azure_driver
from confluent_kafka.schema_registry.rules.encryption.azurekms.azure_aead import AzureKmsAead

AZURE_KEYURI_PREFIX = 'azure-kms://'

log = logging.getLogger(__name__)


class AzureKmsClient(tink.KmsClient):
    """Basic Azure client for AEAD."""

    def __init__(
        self, key_uri: str, credentials: TokenCredential, conf: Optional[Dict[str, Any]] = None
    ) -> None:
        """Creates a new AzureKmsClient that is bound to the key specified in 'key_uri'.

        Uses the specified credentials when communicating with the KMS.

        Args:
          key_uri: The URI of the key the client should be bound to.
          credentials: The token credentials.
          conf: The rule config, merged with the kek's kms_props. Consulted by get_aead to resolve
            ENCRYPT_AZURE_KEY_VERSION_SAVE before building a CryptographyClient.

        Raises:
          TinkError: If the key uri is not valid.
        """

        if key_uri.startswith(AZURE_KEYURI_PREFIX):
            self._key_uri = key_uri
        else:
            raise tink.TinkError('Invalid key_uri.')

        self._credentials = credentials
        self._conf = conf if conf is not None else {}

    def does_support(self, key_uri: str) -> bool:
        """Returns true iff this client supports KMS key specified in 'key_uri'.

        Args:
          key_uri: URI of the key to be checked.

        Returns:
          A boolean value which is true if the key is supported and false otherwise.
        """
        if not self._key_uri:
            return key_uri.startswith(AZURE_KEYURI_PREFIX)
        return key_uri == self._key_uri

    def get_aead(self, key_uri: str) -> aead.Aead:
        """Returns an Aead-primitive backed by KMS key specified by 'key_uri'.

        Args:
          key_uri: URI of the key which should be used.

        Returns:
          An Aead object.
        """
        if self._key_uri and self._key_uri != key_uri:
            raise tink.TinkError('This client is bound to %s and cannot use key %s' % (self._key_uri, key_uri))
        if not key_uri.startswith(AZURE_KEYURI_PREFIX):
            raise tink.TinkError('Invalid key_uri.')
        key_id = key_uri[len(AZURE_KEYURI_PREFIX):]

        save_version = str(self._conf.get(azure_driver.ENCRYPT_AZURE_KEY_VERSION_SAVE, False)).lower() == 'true'
        if not save_version:
            try:
                if azure_driver.is_versionless(key_id):
                    log.warning(
                        "Azure Key Vault key '%s' is versionless and %s is not enabled; DEKs "
                        "wrapped with it may become undecryptable after the key is rotated.",
                        key_id, azure_driver.ENCRYPT_AZURE_KEY_VERSION_SAVE,
                    )
            except tink.TinkError:
                pass  # Malformed key id; surfaced properly when it is actually used below.

        # Built from the raw (possibly versionless) key_id, exactly as before this feature
        # existed: used directly whenever save_version is off, and as decrypt()'s fallback for
        # legacy ciphertext with no embedded version. Cheap to build eagerly: the constructor does
        # not itself make a network call (the Azure SDK resolves lazily on the first actual
        # encrypt/decrypt/wrap_key call).
        default_client = CryptographyClient(key_id, self._credentials)

        # Always built, regardless of the current toggle value: a DEK wrapped while the toggle was
        # on may still need to be decrypted after it has been turned back off, so decrypt() must
        # always be able to resolve whatever version is embedded in an already-prefixed ciphertext.
        def client_factory(version: str) -> CryptographyClient:
            versioned_key_uri = azure_driver.with_version(key_id, version)
            return CryptographyClient(versioned_key_uri, self._credentials)

        encrypt_target: Optional[Callable[[], Tuple[CryptographyClient, str]]]
        if save_version:
            # Deferred until encrypt() actually runs (not built here), so that constructing this
            # Aead for a decrypt-only call site never triggers a wasted version-resolution round
            # trip: get_aead() is called for both encrypt and decrypt, and decrypt's own
            # resolution (if needed at all) comes from whatever version is embedded in the
            # ciphertext, not from re-resolving "current" here.
            def encrypt_target() -> Tuple[CryptographyClient, str]:
                resolved_key_uri = azure_driver.get_versioned_key_id(self._conf, key_id)
                version = resolved_key_uri.rsplit('/', 1)[-1]
                # Reuses client_factory rather than building its own CryptographyClient, so there
                # is only one place that knows how to turn a version into a client.
                return client_factory(version), version
        else:
            encrypt_target = None

        return AzureKmsAead(default_client, EncryptionAlgorithm.rsa_oaep_256, encrypt_target, client_factory)
