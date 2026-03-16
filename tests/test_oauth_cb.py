#!/usr/bin/env python

import asyncio
import time

import pytest

from confluent_kafka import KafkaException
from confluent_kafka.aio import AIOConsumer
from tests.common import TestConsumer


def get_oauth_config(oauth_cb, session_timeout_ms=1000):
    return {
        'group.id': 'test',
        'security.protocol': 'sasl_plaintext',
        'sasl.mechanisms': 'OAUTHBEARER',
        'session.timeout.ms': session_timeout_ms,
        'sasl.oauthbearer.config': 'oauth_cb',
        'oauth_cb': oauth_cb,
    }


def test_oauth_cb():
    """Tests oauth_cb."""
    seen_oauth_cb = False

    def oauth_cb(oauth_config):
        nonlocal seen_oauth_cb
        seen_oauth_cb = True
        assert oauth_config == 'oauth_cb'
        return 'token', time.time() + 300.0

    conf = get_oauth_config(oauth_cb)

    kc = TestConsumer(conf)
    assert seen_oauth_cb  # callback is expected to happen during client init
    kc.close()


def test_oauth_cb_principal_sasl_extensions():
    """Tests oauth_cb."""
    seen_oauth_cb = False

    def oauth_cb(oauth_config):
        nonlocal seen_oauth_cb
        seen_oauth_cb = True
        assert oauth_config == 'oauth_cb'
        return 'token', time.time() + 300.0, oauth_config, {"extone": "extoneval", "exttwo": "exttwoval"}

    conf = get_oauth_config(oauth_cb, session_timeout_ms=100)

    kc = TestConsumer(conf)
    assert seen_oauth_cb  # callback is expected to happen during client init
    kc.close()


def test_oauth_cb_failure():
    """
    Tests oauth_cb for a case when it fails to return a token.
    We expect the client init to fail
    """

    def oauth_cb(oauth_config):
        raise Exception

    conf = get_oauth_config(oauth_cb)

    with pytest.raises(KafkaException):
        TestConsumer(conf)


def test_oauth_cb_token_refresh_success():
    """
    Tests whether oauth callback gets called multiple times by the background thread
    """
    oauth_cb_count = 0

    def oauth_cb(oauth_config):
        nonlocal oauth_cb_count
        oauth_cb_count += 1
        assert oauth_config == 'oauth_cb'
        return 'token', time.time() + 3  # token is returned with an expiry of 3 seconds

    conf = get_oauth_config(oauth_cb)

    kc = TestConsumer(conf)  # callback is expected to happen during client init
    assert oauth_cb_count == 1

    # Check every 1 second for up to 5 seconds for callback count to increase
    max_wait_sec = 5
    elapsed_sec = 0
    while oauth_cb_count == 1 and elapsed_sec < max_wait_sec:
        time.sleep(1)
        elapsed_sec += 1

    kc.close()
    assert oauth_cb_count > 1


def test_oauth_cb_token_refresh_failure():
    """
    Tests whether oauth callback gets called again if token refresh failed in one of the calls after init
    """
    oauth_cb_count = 0

    def oauth_cb(oauth_config):
        nonlocal oauth_cb_count
        oauth_cb_count += 1
        assert oauth_config == 'oauth_cb'
        if oauth_cb_count == 2:
            raise Exception
        return 'token', time.time() + 3  # token is returned with an expiry of 3 seconds

    conf = get_oauth_config(oauth_cb)

    kc = TestConsumer(conf)  # callback is expected to happen during client init
    assert oauth_cb_count == 1

    # Check every 1 second for up to 15 seconds for callback count to increase
    # Call back failure causes a refresh attempt after 10 secs, so ideally 2 callbacks should happen within 15 secs
    max_wait_sec = 15
    elapsed_sec = 0
    while oauth_cb_count <= 2 and elapsed_sec < max_wait_sec:
        time.sleep(1)
        elapsed_sec += 1

    kc.close()
    assert oauth_cb_count > 2


# Async IO Consumer Tests


@pytest.mark.asyncio
async def test_aio_oauth_cb():
    """Tests oauth_cb with AIOConsumer."""
    seen_oauth_cb = False

    def oauth_cb(oauth_config):
        nonlocal seen_oauth_cb
        seen_oauth_cb = True
        assert oauth_config == 'oauth_cb'
        return 'token', time.time() + 300.0

    conf = get_oauth_config(oauth_cb)

    kc = AIOConsumer(conf)
    assert seen_oauth_cb  # callback is expected to happen during client init
    await kc.close()


@pytest.mark.asyncio
async def test_aio_oauth_cb_principal_sasl_extensions():
    """Tests oauth_cb with principal and SASL extensions with AIOConsumer."""
    seen_oauth_cb = False

    def oauth_cb(oauth_config):
        nonlocal seen_oauth_cb
        seen_oauth_cb = True
        assert oauth_config == 'oauth_cb'
        return 'token', time.time() + 300.0, oauth_config, {"extone": "extoneval", "exttwo": "exttwoval"}

    conf = get_oauth_config(oauth_cb, session_timeout_ms=100)

    kc = AIOConsumer(conf)
    assert seen_oauth_cb  # callback is expected to happen during client init
    await kc.close()


@pytest.mark.asyncio
async def test_aio_oauth_cb_failure():
    """
    Tests oauth_cb for a case when it fails to return a token with AIOConsumer.
    We expect the client init to fail
    """

    def oauth_cb(oauth_config):
        raise Exception

    conf = get_oauth_config(oauth_cb)

    with pytest.raises(KafkaException):
        AIOConsumer(conf)


@pytest.mark.asyncio
async def test_aio_oauth_cb_token_refresh_success():
    """
    Tests whether oauth callback gets called multiple times by the background thread with AIOConsumer
    """
    oauth_cb_count = 0

    def oauth_cb(oauth_config):
        nonlocal oauth_cb_count
        oauth_cb_count += 1
        assert oauth_config == 'oauth_cb'
        return 'token', time.time() + 3  # token is returned with an expiry of 3 seconds

    conf = get_oauth_config(oauth_cb)

    kc = AIOConsumer(conf)  # callback is expected to happen during client init
    assert oauth_cb_count == 1

    # Check every 1 second for up to 5 seconds for callback count to increase
    max_wait_sec = 5
    elapsed_sec = 0
    while oauth_cb_count == 1 and elapsed_sec < max_wait_sec:
        await asyncio.sleep(1)
        elapsed_sec += 1

    await kc.close()
    assert oauth_cb_count > 1


@pytest.mark.asyncio
async def test_aio_oauth_cb_token_refresh_failure():
    """
    Tests whether oauth callback gets called again if token refresh failed in one of the calls
    after init with AIOConsumer
    """
    oauth_cb_count = 0

    def oauth_cb(oauth_config):
        nonlocal oauth_cb_count
        oauth_cb_count += 1
        assert oauth_config == 'oauth_cb'
        if oauth_cb_count == 2:
            raise Exception
        return 'token', time.time() + 3  # token is returned with an expiry of 3 seconds

    conf = get_oauth_config(oauth_cb)

    kc = AIOConsumer(conf)  # callback is expected to happen during client init
    assert oauth_cb_count == 1

    # Check every 1 second for up to 15 seconds for callback count to increase
    # Call back failure causes a refresh attempt after 10 secs, so ideally 2 callbacks should happen within 15 secs
    max_wait_sec = 15
    elapsed_sec = 0
    while oauth_cb_count <= 2 and elapsed_sec < max_wait_sec:
        await asyncio.sleep(1)
        elapsed_sec += 1

    await kc.close()
    assert oauth_cb_count > 2
