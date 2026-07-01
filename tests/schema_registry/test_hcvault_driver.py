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

from confluent_kafka.schema_registry.rules.encryption.hcvault import hcvault_client
from confluent_kafka.schema_registry.rules.encryption.hcvault.hcvault_driver import HcVaultKmsDriver


@pytest.fixture(autouse=True)
def _clear_vault_env(monkeypatch):
    """Ensure Vault TLS env vars don't leak in from the host environment."""
    for var in ("VAULT_CACERT", "VAULT_CLIENT_CERT", "VAULT_CLIENT_KEY"):
        monkeypatch.delenv(var, raising=False)


def test_get_verify_defaults_to_true():
    # Verification is enabled by default when nothing is configured.
    assert HcVaultKmsDriver._get_verify({}) is True


def test_get_verify_from_conf():
    assert HcVaultKmsDriver._get_verify({"ssl.ca.location": "/path/ca.pem"}) == "/path/ca.pem"


def test_get_verify_from_env(monkeypatch):
    monkeypatch.setenv("VAULT_CACERT", "/env/ca.pem")
    assert HcVaultKmsDriver._get_verify({}) == "/env/ca.pem"


def test_get_verify_conf_takes_precedence_over_env(monkeypatch):
    monkeypatch.setenv("VAULT_CACERT", "/env/ca.pem")
    assert HcVaultKmsDriver._get_verify({"ssl.ca.location": "/conf/ca.pem"}) == "/conf/ca.pem"


def test_get_verify_empty_conf_defaults_to_true():
    # An empty CA path must NOT disable verification: requests/hvac treat a falsy
    # `verify` as CERT_NONE, so an empty string would silently turn TLS
    # verification off. It must fall back to the secure default instead.
    assert HcVaultKmsDriver._get_verify({"ssl.ca.location": ""}) is True


def test_get_verify_empty_env_defaults_to_true(monkeypatch):
    monkeypatch.setenv("VAULT_CACERT", "")
    assert HcVaultKmsDriver._get_verify({}) is True


def test_get_verify_empty_conf_falls_back_to_env(monkeypatch):
    # An empty conf value is treated as unset, so a real env var still applies.
    monkeypatch.setenv("VAULT_CACERT", "/env/ca.pem")
    assert HcVaultKmsDriver._get_verify({"ssl.ca.location": ""}) == "/env/ca.pem"


def test_get_cert_none_when_unset():
    assert HcVaultKmsDriver._get_cert({}) is None


def test_get_cert_returns_tuple_for_cert_and_key():
    conf = {"ssl.certificate.location": "client.pem", "ssl.key.location": "client.key"}
    assert HcVaultKmsDriver._get_cert(conf) == ("client.pem", "client.key")


def test_get_cert_returns_path_for_cert_only():
    # A single PEM bundling cert and key is valid for requests/hvac.
    assert HcVaultKmsDriver._get_cert({"ssl.certificate.location": "bundle.pem"}) == "bundle.pem"


def test_get_cert_from_env(monkeypatch):
    monkeypatch.setenv("VAULT_CLIENT_CERT", "env.pem")
    monkeypatch.setenv("VAULT_CLIENT_KEY", "env.key")
    assert HcVaultKmsDriver._get_cert({}) == ("env.pem", "env.key")


def test_get_cert_conf_takes_precedence_over_env(monkeypatch):
    monkeypatch.setenv("VAULT_CLIENT_CERT", "env.pem")
    monkeypatch.setenv("VAULT_CLIENT_KEY", "env.key")
    conf = {"ssl.certificate.location": "conf.pem", "ssl.key.location": "conf.key"}
    assert HcVaultKmsDriver._get_cert(conf) == ("conf.pem", "conf.key")


def test_get_cert_key_without_cert_raises():
    with pytest.raises(ValueError, match="ssl.certificate.location required"):
        HcVaultKmsDriver._get_cert({"ssl.key.location": "client.key"})


def test_get_cert_key_from_env_without_cert_raises(monkeypatch):
    monkeypatch.setenv("VAULT_CLIENT_KEY", "env.key")
    with pytest.raises(ValueError, match="ssl.certificate.location required"):
        HcVaultKmsDriver._get_cert({})


@pytest.fixture
def captured_hvac_kwargs(monkeypatch):
    """Replace hvac.Client with a recorder so we can assert what the driver
    actually passes through, without making any network calls."""
    captured = {}

    def fake_client(**kwargs):
        captured.update(kwargs)
        # A MagicMock tolerates any later use (e.g. an AppRole login call).
        return mock.MagicMock()

    monkeypatch.setattr(hcvault_client.hvac, "Client", fake_client)
    return captured


def test_new_kms_client_defaults_to_verify_true(captured_hvac_kwargs):
    # End-to-end guard: a default-configured driver must build an hvac.Client
    # with TLS verification on and no client cert. Covers the wiring
    # new_kms_client -> HcVaultKmsClient -> hvac.Client, not just the helper.
    HcVaultKmsDriver().new_kms_client({}, "hcvault://localhost:8200/transit/keys/key1")

    assert captured_hvac_kwargs["verify"] is True
    assert captured_hvac_kwargs["cert"] is None


def test_new_kms_client_forwards_configured_ca_and_cert(captured_hvac_kwargs):
    # Guards against dropped or mis-ordered arguments in the constructor call:
    # a configured CA bundle and client cert/key must reach hvac.Client intact.
    conf = {
        "ssl.ca.location": "/path/ca.pem",
        "ssl.certificate.location": "client.pem",
        "ssl.key.location": "client.key",
    }
    HcVaultKmsDriver().new_kms_client(conf, "hcvault://localhost:8200/transit/keys/key1")

    assert captured_hvac_kwargs["verify"] == "/path/ca.pem"
    assert captured_hvac_kwargs["cert"] == ("client.pem", "client.key")


def test_new_kms_client_empty_ca_env_still_verifies(monkeypatch, captured_hvac_kwargs):
    # End-to-end regression guard for the empty-string footgun: an empty
    # VAULT_CACERT must still build a client with verification enabled, not
    # silently fall back to verify="" (which requests treats as no verification).
    monkeypatch.setenv("VAULT_CACERT", "")
    HcVaultKmsDriver().new_kms_client({}, "hcvault://localhost:8200/transit/keys/key1")

    assert captured_hvac_kwargs["verify"] is True
