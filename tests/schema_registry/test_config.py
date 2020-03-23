#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright 2020 Confluent Inc.
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

from confluent_kafka.schema_registry.schema_registry_client import \
    SchemaRegistryClient

TEST_URL = 'http://SchemaRegistry:65534'
TEST_USERNAME = 'sr_user'
TEST_USER_PASSWORD = 'sr_user_secret'

"""
Tests to ensure all configurations are handled correctly.

"""


def test_config_url_invalid():
    conf = {'url': 'htt://SchemaRegistry:65534'}
    with pytest.raises(ValueError) as e:
        SchemaRegistryClient(conf)
    assert e.match('Invalid url htt://SchemaRegistry:65534')


def test_config_url_invalid_type():
    conf = {'url': dict()}
    with pytest.raises(ValueError, match="url must be an instance of str not <(.*)>$"):
        SchemaRegistryClient(conf)


def test_config_url_None():
    conf = {}
    with pytest.raises(ValueError, match="url must be an instance of str not"
                                         " <type 'NoneType'>$"):
        SchemaRegistryClient(conf)


def test_config_url_trailing_slash():
    conf = {'url': 'http://SchemaRegistry:65534/'}
    test_client = SchemaRegistryClient(conf)
    assert test_client.url == TEST_URL


def test_config_ssl_certificate():
    conf = {'url': TEST_URL,
            'ssl.certificate.location': '/ssl/certificates/client',
            'ssl.key.location': '/ssl/keys/client'}
    test_client = SchemaRegistryClient(conf)
    assert test_client._certificate == ('/ssl/certificates/client',
                                        '/ssl/keys/client')


def test_config_ssl_certificate_no_key():
    conf = {'url': TEST_URL,
            'ssl.certificate.location': '/ssl/certificates/client'}
    test_client = SchemaRegistryClient(conf)
    assert test_client._certificate == '/ssl/certificates/client'


def test_config_ssl_key_no_certificate():
    conf = {'url': TEST_URL,
            'ssl.key.location': '/ssl/keys/client'}
    with pytest.raises(ValueError, match="ssl.certificate.location required"
                                         " when configuring ssl.key.location"):
        SchemaRegistryClient(conf)


def test_config_auth_url():
    conf = {
        'url': 'http://'
               + TEST_USERNAME + ":"
               + TEST_USER_PASSWORD + '@SchemaRegistry:65534'}
    test_client = SchemaRegistryClient(conf)
    assert test_client._auth == (TEST_USERNAME, TEST_USER_PASSWORD)


def test_config_auth_url_and_userinfo():
    conf = {
        'url': 'http://'
               + TEST_USERNAME + ":"
               + TEST_USER_PASSWORD + '@SchemaRegistry:65534',
        'basic.auth.credentials.source': 'user_info',
        'basic.auth.user.info': TEST_USERNAME + ":" + TEST_USER_PASSWORD}

    with pytest.raises(ValueError, match="basic.auth.credentials.source"
                                         " configured for USER_INFO with"
                                         " credentials in the URL."
                                         " Remove userinfo from the url or"
                                         " or configure"
                                         " basic.auth.credentials.source to"
                                         r" URL\(default\)"):
        SchemaRegistryClient(conf)


def test_config_auth_url_and_sasl_inherit():
    conf = {
        'url': 'http://'
               + TEST_USERNAME + ":"
               + TEST_USER_PASSWORD
               + '@SchemaRegistry:65534',
        'basic.auth.credentials.source': 'SASL_INHERIT',
        'sasl.mechanism': 'PLAIN',
        'sasl.username': TEST_USERNAME,
        'sasl.password': TEST_USER_PASSWORD}

    with pytest.raises(ValueError, match="basic.auth.credentials.source"
                                         " configured for SASL_INHERIT with"
                                         " credentials in the URL."
                                         " Remove userinfo from the url or"
                                         " or configure"
                                         " basic.auth.credentials.source to"
                                         r" URL\(default\)$"):
        SchemaRegistryClient(conf)


def test_config_auth_userinfo():
    conf = {'url': TEST_URL,
            'basic.auth.credentials.source': 'user_info',
            'basic.auth.user.info': TEST_USERNAME + ':' + TEST_USER_PASSWORD}

    test_client = SchemaRegistryClient(conf)
    assert test_client._auth == (TEST_USERNAME, TEST_USER_PASSWORD)


def test_config_auth_userinfo_invalid():
    conf = {'url': TEST_URL,
            'basic.auth.credentials.source': 'USER_INFO',
            'basic.auth.userinfo': 'lookmanocolon'}

    with pytest.raises(ValueError, match="basic.auth.user.info must be in the"
                                         " form of {username}:{password}$"):
        SchemaRegistryClient(conf)


def test_config_auth_sasl_inherit():
    conf = {'url': TEST_URL,
            'basic.auth.credentials.source': 'SASL_INHERIT',
            'sasl.mechanism': 'PLAIN',
            'sasl.username': TEST_USERNAME,
            'sasl.password': TEST_USER_PASSWORD}

    test_client = SchemaRegistryClient(conf)
    assert test_client._auth == (TEST_USERNAME, TEST_USER_PASSWORD)


def test_config_auth_sasl_inherit_not_supported():
    conf = {'url': TEST_URL,
            'basic.auth.credentials.source': 'SASL_INHERIT',
            'sasl.mechanism': 'gssapi'}  # also tests str.upper()}

    with pytest.raises(ValueError, match=r"sasl.mechanism\(s\) GSSAPI is not"
                                         " supported by"
                                         " basic.auth.credentials.source"
                                         " SASL_INHERIT"):
        SchemaRegistryClient(conf)


def test_config_auth_sasl_inherit_no_username():
    conf = {'url': TEST_URL,
            'basic.auth.credentials.source': 'SASL_INHERIT'}
    with pytest.raises(ValueError, match="sasl.username required when"
                                         " basic.auth.credentials.source"
                                         " configured to SASL_INHERIT"):
        SchemaRegistryClient(conf)


def test_config_auth_credential_source_invalid():
    conf = {'url': TEST_URL,
            'basic.auth.credentials.source': 'VAULT'}

    with pytest.raises(ValueError, match="basic.auth.credentials.source must"
                                         r" be one of (\[(.*)\]) not VAULT$"):
        SchemaRegistryClient(conf)


def test_config_unknown_prop():
    conf = {'url': TEST_URL,
            'basic.auth.credentials.source': 'SASL_INHERIT',
            'sasl.username': 'user_sasl',
            'sasl.password': 'secret_sasl',
            'invalid.conf': 1,
            'invalid.conf2': 2}

    with pytest.raises(ValueError, match=r"Unrecognized property\(ies\)"
                                         r" (\[(.*)\])$"):
        SchemaRegistryClient(conf)
