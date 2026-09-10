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
from tink import TinkError

from confluent_kafka.schema_registry.rules.encryption.azurekms import azure_driver

VERSION_A = "a" * 32
VERSIONLESS_KEY_ID = "https://yokota1.vault.azure.net/keys/key1"
VERSIONED_KEY_ID = VERSIONLESS_KEY_ID + "/" + VERSION_A


def test_is_versionless_true_for_versionless_id():
    assert azure_driver.is_versionless(VERSIONLESS_KEY_ID) is True


def test_is_versionless_false_for_versioned_id():
    assert azure_driver.is_versionless(VERSIONED_KEY_ID) is False


def test_is_versionless_raises_for_malformed_id():
    with pytest.raises(TinkError):
        azure_driver.is_versionless("https://yokota1.vault.azure.net/notkeys/key1")


def test_with_version_combines_versionless_id_with_explicit_version():
    assert azure_driver.with_version(VERSIONLESS_KEY_ID, VERSION_A) == VERSIONED_KEY_ID


def test_with_version_ignores_existing_version():
    # Only the vault and key name are used; any existing version segment is discarded in favor
    # of the explicit version argument.
    other_version = "b" * 32
    result = azure_driver.with_version(VERSIONED_KEY_ID, other_version)
    assert result == VERSIONLESS_KEY_ID + "/" + other_version


@mock.patch.object(azure_driver, "KeyClient")
def test_get_versioned_key_id_returns_unchanged_when_already_versioned(mock_key_client_cls):
    result = azure_driver.get_versioned_key_id({}, VERSIONED_KEY_ID)

    assert result == VERSIONED_KEY_ID
    mock_key_client_cls.assert_not_called()


@mock.patch.object(azure_driver, "KeyClient")
def test_get_versioned_key_id_resolves_versionless_id(mock_key_client_cls):
    mock_client = mock_key_client_cls.return_value
    mock_client.get_key.return_value = mock.Mock(id=VERSIONED_KEY_ID)

    result = azure_driver.get_versioned_key_id({}, VERSIONLESS_KEY_ID)

    assert result == VERSIONED_KEY_ID
    mock_client.get_key.assert_called_once_with("key1")


def test_get_versioned_key_id_throws_for_malformed_key_id():
    with pytest.raises(TinkError):
        azure_driver.get_versioned_key_id({}, "https://yokota1.vault.azure.net/notkeys/key1")


def test_get_versioned_key_id_throws_for_invalid_uri():
    with pytest.raises(TinkError):
        azure_driver.get_versioned_key_id({}, "::not a uri::")


@mock.patch.object(azure_driver, "KeyClient")
def test_get_versioned_key_id_wraps_exception_from_resolver(mock_key_client_cls):
    mock_client = mock_key_client_cls.return_value
    resolver_failure = RuntimeError("simulated HttpResponseError")
    mock_client.get_key.side_effect = resolver_failure

    with pytest.raises(TinkError) as exc_info:
        azure_driver.get_versioned_key_id({}, VERSIONLESS_KEY_ID)

    assert exc_info.value.__cause__ is resolver_failure


@mock.patch.object(azure_driver, "KeyClient")
def test_get_versioned_key_id_throws_when_resolver_returns_none(mock_key_client_cls):
    mock_client = mock_key_client_cls.return_value
    mock_client.get_key.return_value = None

    with pytest.raises(TinkError):
        azure_driver.get_versioned_key_id({}, VERSIONLESS_KEY_ID)


@mock.patch.object(azure_driver, "KeyClient")
def test_get_versioned_key_id_throws_when_resolved_key_id_is_none(mock_key_client_cls):
    mock_client = mock_key_client_cls.return_value
    mock_client.get_key.return_value = mock.Mock(id=None)

    with pytest.raises(TinkError):
        azure_driver.get_versioned_key_id({}, VERSIONLESS_KEY_ID)


@mock.patch.object(azure_driver, "KeyClient")
def test_get_versioned_key_id_throws_when_resolved_key_id_is_versionless(mock_key_client_cls):
    mock_client = mock_key_client_cls.return_value
    # Resolver misconfiguration: returns the same versionless id it was asked to resolve.
    mock_client.get_key.return_value = mock.Mock(id=VERSIONLESS_KEY_ID)

    with pytest.raises(TinkError):
        azure_driver.get_versioned_key_id({}, VERSIONLESS_KEY_ID)
