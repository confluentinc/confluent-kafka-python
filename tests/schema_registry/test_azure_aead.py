#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
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
#

from unittest import mock

import pytest
from azure.keyvault.keys.crypto import EncryptionAlgorithm
from tink import TinkError

from confluent_kafka.schema_registry.rules.encryption.azurekms.azure_aead import AzureKmsAead

VERSION_A = "a" * 32
VERSION_B = "b" * 32


def _fake_client(ciphertext=b"wrapped", plaintext=b"plain"):
    client = mock.Mock()
    client.encrypt.return_value = mock.Mock(ciphertext=ciphertext)
    client.decrypt.return_value = mock.Mock(plaintext=plaintext)
    return client


def test_encrypt_without_target_returns_raw_ciphertext():
    client = _fake_client(ciphertext=b"raw-ciphertext")
    aead = AzureKmsAead(client, EncryptionAlgorithm.rsa_oaep_256)

    result = aead.encrypt(b"plaintext", b"")

    assert result == b"raw-ciphertext"


def test_encrypt_with_target_prefixes_without_double_encoding():
    default_client = _fake_client()
    encrypt_client = _fake_client(ciphertext=b"wrapped-bytes")
    aead = AzureKmsAead(
        default_client,
        EncryptionAlgorithm.rsa_oaep_256,
        encrypt_target=lambda: (encrypt_client, VERSION_A),
    )

    result = aead.encrypt(b"plaintext", b"")

    assert result == b"azure:v1:" + VERSION_A.encode("ascii") + b":wrapped-bytes"


@pytest.mark.parametrize(
    "bad_version",
    ["not-32-chars", "g" * 32, None],
    ids=["wrong-length", "non-hex", "none"],
)
def test_encrypt_throws_for_invalid_version(bad_version):
    aead = AzureKmsAead(
        _fake_client(),
        EncryptionAlgorithm.rsa_oaep_256,
        encrypt_target=lambda: (_fake_client(), bad_version),
    )

    with pytest.raises(TinkError):
        aead.encrypt(b"plaintext", b"")


def test_decrypt_uses_embedded_version_via_client_factory():
    default_client = _fake_client(plaintext=b"wrong-client")
    versioned_client = _fake_client(plaintext=b"correct-plaintext")
    client_factory = mock.Mock(return_value=versioned_client)
    aead = AzureKmsAead(default_client, EncryptionAlgorithm.rsa_oaep_256, client_factory=client_factory)
    ciphertext = b"azure:v1:" + VERSION_A.encode("ascii") + b":wrapped-bytes"

    result = aead.decrypt(ciphertext, b"")

    assert result == b"correct-plaintext"
    client_factory.assert_called_once_with(VERSION_A)
    versioned_client.decrypt.assert_called_once_with(EncryptionAlgorithm.rsa_oaep_256, b"wrapped-bytes")


def test_decrypt_falls_back_to_default_client_for_legacy_ciphertext():
    default_client = _fake_client(plaintext=b"legacy-plaintext")
    client_factory = mock.Mock()
    aead = AzureKmsAead(default_client, EncryptionAlgorithm.rsa_oaep_256, client_factory=client_factory)

    result = aead.decrypt(b"legacy-unprefixed-ciphertext", b"")

    assert result == b"legacy-plaintext"
    client_factory.assert_not_called()


def test_decrypt_remains_possible_after_toggle_turned_off():
    # A DEK wrapped while ENCRYPT_AZURE_KEY_VERSION_SAVE was on must stay decryptable even once
    # the toggle is turned back off: decrypt() must consult client_factory regardless of whether
    # encrypt_target (the toggle-gated half) is set.
    versioned_client = _fake_client(plaintext=b"still-decryptable")
    client_factory = mock.Mock(return_value=versioned_client)
    aead = AzureKmsAead(
        _fake_client(), EncryptionAlgorithm.rsa_oaep_256, encrypt_target=None, client_factory=client_factory
    )
    ciphertext = b"azure:v1:" + VERSION_B.encode("ascii") + b":wrapped-bytes"

    result = aead.decrypt(ciphertext, b"")

    assert result == b"still-decryptable"


def test_decrypt_throws_for_prefixed_ciphertext_with_no_client_factory():
    aead = AzureKmsAead(_fake_client(), EncryptionAlgorithm.rsa_oaep_256)
    ciphertext = b"azure:v1:" + VERSION_A.encode("ascii") + b":wrapped-bytes"

    with pytest.raises(TinkError):
        aead.decrypt(ciphertext, b"")


def test_decrypt_throws_for_non_hex_embedded_version():
    non_hex_version = ("g" * 32).encode("ascii")
    aead = AzureKmsAead(_fake_client(), EncryptionAlgorithm.rsa_oaep_256, client_factory=mock.Mock())
    ciphertext = b"azure:v1:" + non_hex_version + b":wrapped-bytes"

    with pytest.raises(TinkError):
        aead.decrypt(ciphertext, b"")


def test_decrypt_wraps_unexpected_exception_from_client_factory():
    client_factory = mock.Mock(side_effect=RuntimeError("boom"))
    aead = AzureKmsAead(_fake_client(), EncryptionAlgorithm.rsa_oaep_256, client_factory=client_factory)
    ciphertext = b"azure:v1:" + VERSION_A.encode("ascii") + b":wrapped-bytes"

    with pytest.raises(TinkError):
        aead.decrypt(ciphertext, b"")
