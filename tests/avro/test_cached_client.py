#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
#
# Copyright 2019 Confluent Inc.
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

#
# derived from https://github.com/verisign/python-confluent-schemaregistry.git
#

import pytest

from confluent_kafka.avro.cached_schema_registry_client import CachedSchemaRegistryClient


def test_cert_no_key():
    with pytest.raises(ValueError):
        CachedSchemaRegistryClient(url='https://127.0.0.1:65534',
                                   cert_location='/path/to/cert')


def test_cert_with_key():
    client = CachedSchemaRegistryClient(url='https://127.0.0.1:65534',
                                        cert_location='/path/to/cert',
                                        key_location='/path/to/key')
    assert ('/path/to/cert', '/path/to/key') == client._session.cert


def test_cert_path():
    client = CachedSchemaRegistryClient(url='https://127.0.0.1:65534',
                                        ca_location='/path/to/ca')
    assert '/path/to/ca' == client._session.verify


def test_init_with_dict():
    client = CachedSchemaRegistryClient({
        'url': 'https://127.0.0.1:65534',
        'ssl.certificate.location': '/path/to/cert',
        'ssl.key.location': '/path/to/key'
    })
    assert 'https://127.0.0.1:65534' == client.url


def test_empty_url():
    with pytest.raises(ValueError):
        CachedSchemaRegistryClient({'url': ''})


def test_invalid_type_url():
    with pytest.raises(TypeError):
        CachedSchemaRegistryClient(url=1)


def test_invalid_type_url_dict():
    with pytest.raises(TypeError):
        CachedSchemaRegistryClient({"url": 1})


def test_invalid_url():
    with pytest.raises(ValueError):
        CachedSchemaRegistryClient({
            'url': 'example.com:65534'
        })


def test_basic_auth_url():
    client = CachedSchemaRegistryClient({
        'url': 'https://user_url:secret_url@127.0.0.1:65534',
    })
    assert ('user_url', 'secret_url') == client._session.auth


def test_basic_auth_userinfo():
    client = CachedSchemaRegistryClient({
        'url': 'https://user_url:secret_url@127.0.0.1:65534',
        'basic.auth.credentials.source': 'user_info',
        'basic.auth.user.info': 'user_userinfo:secret_userinfo'
    })
    assert ('user_userinfo', 'secret_userinfo') == client._session.auth


def test_basic_auth_sasl_inherit():
    client = CachedSchemaRegistryClient({
        'url': 'https://user_url:secret_url@127.0.0.1:65534',
        'basic.auth.credentials.source': 'SASL_INHERIT',
        'sasl.mechanism': 'PLAIN',
        'sasl.username': 'user_sasl',
        'sasl.password': 'secret_sasl'
    })
    assert ('user_sasl', 'secret_sasl') == client._session.auth


def test_basic_auth_invalid():
    with pytest.raises(ValueError):
        CachedSchemaRegistryClient({
            'url': 'https://user_url:secret_url@127.0.0.1:65534',
            'basic.auth.credentials.source': 'VAULT',
        })


def test_invalid_conf():
    with pytest.raises(ValueError):
        CachedSchemaRegistryClient({
            'url': 'https://user_url:secret_url@127.0.0.1:65534',
            'basic.auth.credentials.source': 'SASL_INHERIT',
            'sasl.username': 'user_sasl',
            'sasl.password': 'secret_sasl',
            'invalid.conf': 1,
            'invalid.conf2': 2
        })
