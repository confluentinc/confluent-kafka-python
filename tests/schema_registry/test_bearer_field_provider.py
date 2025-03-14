#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright 2025 Confluent Inc.
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
import time
from unittest.mock import Mock, patch

from confluent_kafka.schema_registry.schema_registry_client import (_OAuthClient, _StaticFieldProvider,
                                                                    _CustomOAuthClient, SchemaRegistryClient)
from confluent_kafka.schema_registry.error import OAuthTokenError

"""
Tests to ensure OAuth client is set up correctly.

"""


def custom_oauth_function(config: dict) -> dict:
    return config


TEST_TOKEN = 'token123'
TEST_CLUSTER = 'lsrc-cluster'
TEST_POOL = 'pool-id'
TEST_FUNCTION = custom_oauth_function
TEST_CONFIG = {'bearer.auth.token': TEST_TOKEN, 'bearer.auth.logical.cluster': TEST_CLUSTER,
               'bearer.auth.identity.pool.id': TEST_POOL}
TEST_URL = 'http://SchemaRegistry:65534'


def test_expiry():
    oauth_client = _OAuthClient('id', 'secret', 'scope', 'endpoint', TEST_CLUSTER, TEST_POOL, 2, 1000, 20000)
    oauth_client.token = {'expires_at': time.time() + 2, 'expires_in': 1}
    assert not oauth_client.token_expired()
    time.sleep(1.5)
    assert oauth_client.token_expired()


def test_get_token():
    oauth_client = _OAuthClient('id', 'secret', 'scope', 'endpoint', TEST_CLUSTER, TEST_POOL, 2, 1000, 20000)

    def update_token1():
        oauth_client.token = {'expires_at': 0, 'expires_in': 1, 'access_token': '123'}

    def update_token2():
        oauth_client.token = {'expires_at': time.time() + 2, 'expires_in': 1, 'access_token': '1234'}

    oauth_client.generate_access_token = Mock(side_effect=update_token1)
    oauth_client.get_access_token()
    assert oauth_client.generate_access_token.call_count == 1
    assert oauth_client.token['access_token'] == '123'

    oauth_client.generate_access_token = Mock(side_effect=update_token2)
    oauth_client.get_access_token()
    # Call count resets to 1 after reassigning generate_access_token
    assert oauth_client.generate_access_token.call_count == 1
    assert oauth_client.token['access_token'] == '1234'

    oauth_client.get_access_token()
    assert oauth_client.generate_access_token.call_count == 1


def test_generate_token_retry_logic():
    oauth_client = _OAuthClient('id', 'secret', 'scope', 'endpoint', TEST_CLUSTER, TEST_POOL, 5, 1000, 20000)

    with (patch("confluent_kafka.schema_registry.schema_registry_client.time.sleep") as mock_sleep,
          patch("confluent_kafka.schema_registry.schema_registry_client.full_jitter") as mock_jitter):

        with pytest.raises(OAuthTokenError):
            oauth_client.generate_access_token()

        assert mock_sleep.call_count == 5
        assert mock_jitter.call_count == 5


def test_static_field_provider():
    static_field_provider = _StaticFieldProvider(TEST_TOKEN, TEST_CLUSTER, TEST_POOL)
    bearer_fields = static_field_provider.get_bearer_fields()

    assert bearer_fields == TEST_CONFIG


def test_custom_oauth_client():
    custom_oauth_client = _CustomOAuthClient(TEST_FUNCTION, TEST_CONFIG)

    assert custom_oauth_client.get_bearer_fields() == custom_oauth_client.get_bearer_fields()


def test_bearer_field_headers_missing():
    def empty_custom(config):
        return {}

    conf = {'url': TEST_URL,
            'bearer.auth.credentials.source': 'CUSTOM',
            'bearer.auth.custom.provider.function': empty_custom,
            'bearer.auth.custom.provider.config': TEST_CONFIG}

    headers = {'Accept': "application/vnd.schemaregistry.v1+json,"
                         " application/vnd.schemaregistry+json,"
                         " application/json"}

    client = SchemaRegistryClient(conf)

    with pytest.raises(ValueError, match=r"Missing required bearer auth fields, "
                                         r"needs to be set in config or custom function: (.*)"):
        client._rest_client.handle_bearer_auth(headers)


def test_bearer_field_headers_valid():
    conf = {'url': TEST_URL,
            'bearer.auth.credentials.source': 'CUSTOM',
            'bearer.auth.custom.provider.function': TEST_FUNCTION,
            'bearer.auth.custom.provider.config': TEST_CONFIG}

    client = SchemaRegistryClient(conf)

    headers = {'Accept': "application/vnd.schemaregistry.v1+json,"
                         " application/vnd.schemaregistry+json,"
                         " application/json"}

    client._rest_client.handle_bearer_auth(headers)

    assert 'Authorization' in headers
    assert 'Confluent-Identity-Pool-Id' in headers
    assert 'target-sr-cluster' in headers
    assert headers['Authorization'] == "Bearer {}".format(TEST_CONFIG['bearer.auth.token'])
    assert headers['Confluent-Identity-Pool-Id'] == TEST_CONFIG['bearer.auth.identity.pool.id']
    assert headers['target-sr-cluster'] == TEST_CONFIG['bearer.auth.logical.cluster']
