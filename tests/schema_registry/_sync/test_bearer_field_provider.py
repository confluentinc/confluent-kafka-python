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
import time
from unittest.mock import Mock, patch

import pytest

from confluent_kafka.schema_registry._sync.schema_registry_client import (
    SchemaRegistryClient,
    _CustomOAuthClient,
    _OAuthClient,
)
from confluent_kafka.schema_registry.common.schema_registry_client import _StaticFieldProvider
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
TEST_CONFIG = {
    'bearer.auth.token': TEST_TOKEN,
    'bearer.auth.logical.cluster': TEST_CLUSTER,
    'bearer.auth.identity.pool.id': TEST_POOL,
}
TEST_URL = 'http://SchemaRegistry:65534'


def test_expiry():
    oauth_client = _OAuthClient('id', 'secret', 'scope', 'endpoint', TEST_CLUSTER, 2, 1000, 20000, TEST_POOL)
    # Use consistent test data: expires_at and expires_in should match
    # Token expires in 2 seconds, with 0.8 threshold, should refresh after 1.6 seconds (when 0.4s remaining)
    oauth_client.token = {'expires_at': time.time() + 2, 'expires_in': 2}
    assert not oauth_client.token_expired()
    time.sleep(1.7)  # After 1.7 seconds, only 0.3s remaining (< 0.4s threshold), should be expired
    assert oauth_client.token_expired()


def test_get_token():
    oauth_client = _OAuthClient('id', 'secret', 'scope', 'endpoint', TEST_CLUSTER, 2, 1000, 20000, TEST_POOL)

    def update_token1():
        oauth_client.token = {'expires_at': 0, 'expires_in': 1, 'access_token': '123'}

    def update_token2():
        # Use consistent test data: expires_at and expires_in should match
        oauth_client.token = {'expires_at': time.time() + 2, 'expires_in': 2, 'access_token': '1234'}

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
    oauth_client = _OAuthClient('id', 'secret', 'scope', 'endpoint', TEST_CLUSTER, 5, 1000, 20000, TEST_POOL)

    with (
        patch("confluent_kafka.schema_registry._sync.schema_registry_client.time.sleep") as mock_sleep,
        patch("confluent_kafka.schema_registry._sync.schema_registry_client.full_jitter") as mock_jitter,
    ):

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

    assert custom_oauth_client.get_bearer_fields() == TEST_CONFIG


def test_bearer_field_headers_missing():
    def empty_custom(config):
        return {}

    conf = {
        'url': TEST_URL,
        'bearer.auth.credentials.source': 'CUSTOM',
        'bearer.auth.custom.provider.function': empty_custom,
        'bearer.auth.custom.provider.config': TEST_CONFIG,
    }

    headers = {
        'Accept': "application/vnd.schemaregistry.v1+json," " application/vnd.schemaregistry+json," " application/json"
    }

    client = SchemaRegistryClient(conf)

    with pytest.raises(
        ValueError, match=r"Missing required bearer auth fields, " r"needs to be set in config or custom function: (.*)"
    ):
        client._rest_client.handle_bearer_auth(headers)


def test_bearer_field_headers_valid():
    conf = {
        'url': TEST_URL,
        'bearer.auth.credentials.source': 'CUSTOM',
        'bearer.auth.custom.provider.function': TEST_FUNCTION,
        'bearer.auth.custom.provider.config': TEST_CONFIG,
    }

    client = SchemaRegistryClient(conf)

    headers = {
        'Accept': "application/vnd.schemaregistry.v1+json," " application/vnd.schemaregistry+json," " application/json"
    }

    client._rest_client.handle_bearer_auth(headers)

    assert 'Authorization' in headers
    assert 'Confluent-Identity-Pool-Id' in headers
    assert 'target-sr-cluster' in headers
    assert headers['Authorization'] == "Bearer {}".format(TEST_CONFIG['bearer.auth.token'])
    assert headers['Confluent-Identity-Pool-Id'] == TEST_CONFIG['bearer.auth.identity.pool.id']
    assert headers['target-sr-cluster'] == TEST_CONFIG['bearer.auth.logical.cluster']


def test_bearer_field_headers_optional_identity_pool():
    """Test that identity pool is optional and header is omitted when not provided."""

    def custom_oauth_no_pool(config: dict) -> dict:
        return {
            'bearer.auth.token': TEST_TOKEN,
            'bearer.auth.logical.cluster': TEST_CLUSTER,
            # bearer.auth.identity.pool.id is intentionally omitted
        }

    conf = {
        'url': TEST_URL,
        'bearer.auth.credentials.source': 'CUSTOM',
        'bearer.auth.custom.provider.function': custom_oauth_no_pool,
        'bearer.auth.custom.provider.config': {},
    }

    client = SchemaRegistryClient(conf)

    headers = {
        'Accept': "application/vnd.schemaregistry.v1+json," " application/vnd.schemaregistry+json," " application/json"
    }

    client._rest_client.handle_bearer_auth(headers)

    assert 'Authorization' in headers
    assert 'target-sr-cluster' in headers
    assert headers['Authorization'] == "Bearer {}".format(TEST_TOKEN)
    assert headers['target-sr-cluster'] == TEST_CLUSTER
    # Confluent-Identity-Pool-Id should NOT be present when identity pool is omitted
    assert 'Confluent-Identity-Pool-Id' not in headers


def test_bearer_field_headers_comma_separated_pools():
    """Test that comma-separated pool IDs are passed through as-is in the header."""
    comma_separated_pools = 'pool-1,pool-2,pool-3'

    def custom_oauth_multi_pool(config: dict) -> dict:
        return {
            'bearer.auth.token': TEST_TOKEN,
            'bearer.auth.logical.cluster': TEST_CLUSTER,
            'bearer.auth.identity.pool.id': comma_separated_pools,
        }

    conf = {
        'url': TEST_URL,
        'bearer.auth.credentials.source': 'CUSTOM',
        'bearer.auth.custom.provider.function': custom_oauth_multi_pool,
        'bearer.auth.custom.provider.config': {},
    }

    client = SchemaRegistryClient(conf)

    headers = {
        'Accept': "application/vnd.schemaregistry.v1+json," " application/vnd.schemaregistry+json," " application/json"
    }

    client._rest_client.handle_bearer_auth(headers)

    assert 'Authorization' in headers
    assert 'Confluent-Identity-Pool-Id' in headers
    assert 'target-sr-cluster' in headers
    # Verify comma-separated value is passed through unchanged
    assert headers['Confluent-Identity-Pool-Id'] == comma_separated_pools


def test_static_token_optional_identity_pool():
    """Test that STATIC_TOKEN credential source works without identity pool."""
    conf = {
        'url': TEST_URL,
        'bearer.auth.credentials.source': 'STATIC_TOKEN',
        'bearer.auth.token': TEST_TOKEN,
        'bearer.auth.logical.cluster': TEST_CLUSTER,
        # bearer.auth.identity.pool.id is intentionally omitted
    }

    client = SchemaRegistryClient(conf)

    headers = {
        'Accept': "application/vnd.schemaregistry.v1+json," " application/vnd.schemaregistry+json," " application/json"
    }

    client._rest_client.handle_bearer_auth(headers)

    assert 'Authorization' in headers
    assert 'target-sr-cluster' in headers
    assert 'Confluent-Identity-Pool-Id' not in headers


def test_static_token_comma_separated_pools():
    """Test that STATIC_TOKEN credential source supports comma-separated pool IDs."""
    comma_separated_pools = 'pool-abc,pool-def,pool-ghi'

    conf = {
        'url': TEST_URL,
        'bearer.auth.credentials.source': 'STATIC_TOKEN',
        'bearer.auth.token': TEST_TOKEN,
        'bearer.auth.logical.cluster': TEST_CLUSTER,
        'bearer.auth.identity.pool.id': comma_separated_pools,
    }

    client = SchemaRegistryClient(conf)

    headers = {
        'Accept': "application/vnd.schemaregistry.v1+json," " application/vnd.schemaregistry+json," " application/json"
    }

    client._rest_client.handle_bearer_auth(headers)

    assert 'Confluent-Identity-Pool-Id' in headers
    assert headers['Confluent-Identity-Pool-Id'] == comma_separated_pools


def test_static_field_provider_optional_pool():
    """Test that _StaticFieldProvider works with optional identity pool."""

    from confluent_kafka.schema_registry.common.schema_registry_client import _StaticFieldProvider

    def check_provider():
        static_field_provider = _StaticFieldProvider(TEST_TOKEN, TEST_CLUSTER, None)
        bearer_fields = static_field_provider.get_bearer_fields()

        assert bearer_fields['bearer.auth.token'] == TEST_TOKEN
        assert bearer_fields['bearer.auth.logical.cluster'] == TEST_CLUSTER
        assert 'bearer.auth.identity.pool.id' not in bearer_fields

    check_provider()
