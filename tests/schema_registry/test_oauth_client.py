import pytest
import time
from unittest.mock import Mock, patch

from confluent_kafka.schema_registry.schema_registry_client import _OAuthClient
from confluent_kafka.schema_registry.error import OAuthTokenError

"""
Tests to ensure OAuth client is set up correctly.

"""


def test_expiry():
    oauth_client = _OAuthClient('id', 'secret', 'scope', 'endpoint', 2, 1000, 20000)
    oauth_client.token = {'expires_at': time.time() + 2, 'expires_in': 1}
    assert not oauth_client.token_expired()
    time.sleep(1.5)
    assert oauth_client.token_expired()


def test_get_token():
    oauth_client = _OAuthClient('id', 'secret', 'scope', 'endpoint', 2, 1000, 20000)
    assert not oauth_client.token

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
    oauth_client = _OAuthClient('id', 'secret', 'scope', 'endpoint', 5, 1000, 20000)

    with (patch("confluent_kafka.schema_registry.schema_registry_client.time.sleep") as mock_sleep,
          patch("confluent_kafka.schema_registry.schema_registry_client.full_jitter") as mock_jitter):

        with pytest.raises(OAuthTokenError):
            oauth_client.generate_access_token()

        assert mock_sleep.call_count == 5
        assert mock_jitter.call_count == 5
