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

import pytest

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
