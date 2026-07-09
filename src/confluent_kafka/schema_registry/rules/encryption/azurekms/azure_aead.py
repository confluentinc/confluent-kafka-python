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

import string
from typing import Callable, Optional, Tuple

import tink
from azure.keyvault.keys.crypto import CryptographyClient, EncryptionAlgorithm
from tink import aead

_PREFIX = b"azure:v1:"
_VERSION_LENGTH = 32
_HEADER_LENGTH = len(_PREFIX) + _VERSION_LENGTH + 1  # +1 for ':'


def _is_valid_version(value: Optional[str]) -> bool:
    """Returns true if value is exactly _VERSION_LENGTH hex characters, the only shape that can be
    embedded in (and later parsed back out of) the fixed-width azure:v1: prefix. Used to validate
    both a freshly resolved version (in encrypt) and one extracted from ciphertext (in decrypt),
    since encrypted key material is unauthenticated at this layer and could be corrupted or
    tampered with.
    """
    return value is not None and len(value) == _VERSION_LENGTH and all(c in string.hexdigits for c in value)


def _extract_version(ciphertext: bytes) -> Optional[str]:
    """Returns the embedded version if ciphertext carries the azure:v1: prefix (see class
    docstring), or None if it does not (e.g. a legacy DEK wrapped before
    ENCRYPT_AZURE_KEY_VERSION_SAVE was enabled on its KEK, or the toggle is not set). Returning
    None rather than raising is deliberate: the toggle can be flipped on/off over a KEK's lifetime,
    and old, un-prefixed ciphertext must remain decryptable.
    """
    if (
        len(ciphertext) < _HEADER_LENGTH
        or not ciphertext.startswith(_PREFIX)
        or ciphertext[_HEADER_LENGTH - 1] != ord(':')
    ):
        return None
    return ciphertext[len(_PREFIX) : len(_PREFIX) + _VERSION_LENGTH].decode('ascii', errors='replace')


class AzureKmsAead(aead.Aead):
    """Implements the Aead interface for Azure KMS.

    Unlike AWS KMS and GCP KMS, Azure Key Vault addresses wrap/unwrap by an explicit key version
    and does not embed that version in the ciphertext it returns. When 'encrypt_target' is set (see
    azure_driver.ENCRYPT_AZURE_KEY_VERSION_SAVE), 'encrypt' makes its output self-describing by
    prepending the exact version that produced it: azure:v1:<32-character key version>:<raw
    ciphertext bytes>.

    'decrypt' always checks for this prefix regardless of the current 'encrypt_target', since a DEK
    wrapped while the toggle was on must remain decryptable even after it is turned back off.
    """

    def __init__(
        self,
        default_client: CryptographyClient,
        algorithm: EncryptionAlgorithm,
        encrypt_target: Optional[Callable[[], Tuple[CryptographyClient, str]]] = None,
        client_factory: Optional[Callable[[str], CryptographyClient]] = None,
    ) -> None:
        """
        Args:
          default_client: used when encrypting with encrypt_target unset, and when decrypting
            ciphertext with no embedded version prefix.
          algorithm: algorithm.
          encrypt_target: if set, called lazily (not until encrypt is actually invoked) to
            determine the client and version to prefix new output with.
          client_factory: builds a CryptographyClient for an arbitrary version, used by decrypt to
            target whichever version is embedded in already-wrapped ciphertext, which may differ
            from whatever encrypt_target currently resolves to. Consulted by decrypt regardless of
            whether encrypt_target is set, so may be None only if encrypt_target is also None and
            no already-prefixed ciphertext will ever be presented to this Aead.
        """
        if not default_client:
            raise tink.TinkError('client cannot be null.')
        self._default_client = default_client
        self._algorithm = algorithm
        self._encrypt_target = encrypt_target
        self._client_factory = client_factory

    def encrypt(self, plaintext: bytes, associated_data: bytes) -> bytes:
        if self._encrypt_target is None:
            try:
                return self._default_client.encrypt(self._algorithm, plaintext).ciphertext
            except ValueError as e:
                raise tink.TinkError(e)

        try:
            client, version = self._encrypt_target()
        except Exception as e:
            raise tink.TinkError(f'failed to resolve kms client for encryption: {e}') from e

        if not _is_valid_version(version):
            # Mirrors decrypt()'s own validation: a DEK this method wraps must always be one this
            # same class can later unwrap.
            raise tink.TinkError(
                f"kms key version '{version}' must be a {_VERSION_LENGTH}-character hex string; "
                "cannot be embedded in a fixed-width azure:v1: prefix"
            )
        try:
            ciphertext = client.encrypt(self._algorithm, plaintext).ciphertext
        except ValueError as e:
            raise tink.TinkError(e)
        return _PREFIX + version.encode('ascii') + b':' + ciphertext

    def decrypt(self, ciphertext: bytes, associated_data: bytes) -> bytes:
        client = self._default_client
        wrapped = ciphertext
        version = _extract_version(ciphertext)
        if version is not None:
            if not _is_valid_version(version):
                # Encrypted key material is unauthenticated at this layer, so a corrupted or
                # tampered value could otherwise smuggle arbitrary characters (e.g. '/') into the
                # key identifier URL built from it below.
                raise tink.TinkError(f"ciphertext carries an invalid azure:v1: key version: '{version}'")
            if self._client_factory is None:
                raise tink.TinkError(
                    'ciphertext carries a kms key version prefix, but this Aead has no client ' 'factory to resolve it'
                )
            try:
                client = self._client_factory(version)
            except Exception as e:
                raise tink.TinkError(f"failed to resolve kms client for embedded key version '{version}': {e}") from e
            wrapped = ciphertext[_HEADER_LENGTH:]
        try:
            return client.decrypt(self._algorithm, wrapped).plaintext
        except ValueError as e:
            raise tink.TinkError(e)
