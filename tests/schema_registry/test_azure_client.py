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

import logging
from unittest import mock

import pytest

from confluent_kafka.schema_registry.rules.encryption.azurekms import azure_client
from confluent_kafka.schema_registry.rules.encryption.azurekms.azure_driver import (
    ENCRYPT_AZURE_KEY_VERSION_SAVE,
)

KEY_URI = "azure-kms://https://yokota1.vault.azure.net/keys/key1"
VERSION_A = "a" * 32


@pytest.fixture(autouse=True)
def _fake_cryptography_client(monkeypatch):
    """Replace CryptographyClient with a recorder so get_aead's wiring can be exercised without
    making any Azure network calls. Clients are cached by key_id so repeated calls for the same
    key_id (e.g. client_factory(version) called more than once in a test) yield the same mock."""
    clients_by_key_id = {}

    def fake_client(key_id, credentials):
        if key_id not in clients_by_key_id:
            client = mock.Mock()
            client.key_id = key_id
            clients_by_key_id[key_id] = client
        return clients_by_key_id[key_id]

    monkeypatch.setattr(azure_client, "CryptographyClient", fake_client)


def test_versionless_without_toggle_logs_warning(caplog):
    client = azure_client.AzureKmsClient(KEY_URI, mock.Mock(), conf={})

    with caplog.at_level(logging.WARNING, logger=azure_client.__name__):
        client.get_aead(KEY_URI)

    assert any("versionless" in record.message for record in caplog.records)


def test_no_warning_when_toggle_enabled(caplog):
    client = azure_client.AzureKmsClient(
        KEY_URI, mock.Mock(), conf={ENCRYPT_AZURE_KEY_VERSION_SAVE: "true"}
    )

    with caplog.at_level(logging.WARNING, logger=azure_client.__name__):
        client.get_aead(KEY_URI)

    assert not any("versionless" in record.message for record in caplog.records)


@pytest.mark.parametrize("truthy_value", [True, "true", "True"])
def test_toggle_accepts_bool_and_string_values(truthy_value, caplog):
    # Values coming from kek kms_props are always strings, but a caller could also pass a real
    # bool; both must enable the feature (and neither should raise).
    client = azure_client.AzureKmsClient(
        KEY_URI, mock.Mock(), conf={ENCRYPT_AZURE_KEY_VERSION_SAVE: truthy_value}
    )

    with caplog.at_level(logging.WARNING, logger=azure_client.__name__):
        aead = client.get_aead(KEY_URI)

    assert not any("versionless" in record.message for record in caplog.records)
    assert aead._encrypt_target is not None


def test_get_aead_always_builds_client_factory_regardless_of_toggle():
    # Regression guard: a DEK wrapped while the toggle was on must remain decryptable even after
    # the toggle is turned back off, so decrypt() must always be able to resolve an embedded
    # version via client_factory.
    client = azure_client.AzureKmsClient(
        KEY_URI, mock.Mock(), conf={ENCRYPT_AZURE_KEY_VERSION_SAVE: "false"}
    )

    aead = client.get_aead(KEY_URI)
    ciphertext = b"azure:v1:" + VERSION_A.encode("ascii") + b":wrapped-bytes"

    assert aead._encrypt_target is None
    assert aead._client_factory is not None

    result = aead.decrypt(ciphertext, b"")

    # The fake CryptographyClient built by client_factory(VERSION_A) is what actually served the
    # decrypt call, not the default (versionless) client -- proving client_factory was consulted.
    versioned_client = aead._client_factory(VERSION_A)
    assert versioned_client.key_id.endswith("/" + VERSION_A)
    assert result == versioned_client.decrypt.return_value.plaintext
