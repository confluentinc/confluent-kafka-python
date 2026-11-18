#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright 2026 Confluent Inc.
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

from confluent_kafka.schema_registry.rules.encryption.encrypt_executor import FieldEncryptionExecutor
from confluent_kafka.schema_registry.serde import RuleError

"""
Tests for config handling that depend on the rules extra (tink/celpy) and
are excluded from collection on free-threaded builds -- see
tests/schema_registry/conftest.py.
"""


def test_config_encrypt_executor():
    executor = FieldEncryptionExecutor()
    client_conf = {'url': 'mock://'}
    rule_conf = {'key': 'value'}
    executor.configure(client_conf, rule_conf)
    # configure with same args is fine
    executor.configure(client_conf, rule_conf)
    rule_conf2 = {'key2': 'value2'}
    # configure with additional rule_conf keys is fine
    executor.configure(client_conf, rule_conf2)

    client_conf2 = {
        'url': 'mock://',
        'ssl.key.location': '/ssl/keys/client',
        'ssl.certificate.location': '/ssl/certs/client',
    }
    with pytest.raises(RuleError, match="executor already configured"):
        executor.configure(client_conf2, rule_conf)

    rule_conf3 = {'key': 'value3'}
    with pytest.raises(RuleError, match="rule config key already set: key"):
        executor.configure(client_conf, rule_conf3)
