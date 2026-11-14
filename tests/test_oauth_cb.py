#!/usr/bin/env python
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

import asyncio
import threading
import time

import pytest

from confluent_kafka import KafkaError, KafkaException, Producer, ShareConsumer
from confluent_kafka.admin import AdminClient
from confluent_kafka.aio import AIOConsumer
from tests.common import TestConsumer, TestUtils


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


def test_oauth_cb_token_refresh_invalid_return():
    """
    A token refresh whose oauth_cb returns an invalid value is retried, as when
    the callback raises, and error_cb gets the callback's error. The client
    used to never refresh its token again.
    """
    oauth_cb_count = 0
    errors = []

    def oauth_cb(oauth_config):
        nonlocal oauth_cb_count
        oauth_cb_count += 1
        if oauth_cb_count == 2:
            return 'just-a-token-string'  # not a (token, expiry, ...) tuple
        return 'token', time.time() + 3

    conf = get_oauth_config(oauth_cb)
    conf['error_cb'] = errors.append
    kc = TestConsumer(conf)
    assert oauth_cb_count == 1

    # The refresh fails about 2.4 seconds in and is retried 10 seconds later.
    deadline = time.monotonic() + 16
    while oauth_cb_count <= 2 and time.monotonic() < deadline:
        kc.poll(0.5)

    kc.close()
    assert oauth_cb_count > 2
    assert any(
        e.code() == KafkaError._AUTHENTICATION and "oauth_cb failed: TypeError: expect returned value" in e.str()
        for e in errors
    ), errors


def _raise_runtime_error(oauth_config):
    raise RuntimeError("no token today")


def _return_bare_string(oauth_config):
    return 'just-a-token-string'  # not a (token, expiry, ...) tuple


OAUTH_CB_FAILURES = {
    'raises': (_raise_runtime_error, "oauth_cb failed: RuntimeError: no token today"),
    'invalid-return': (_return_bare_string, "oauth_cb failed: TypeError: expect returned value from oauth_cb"),
}


def _make_client(kind, oauth_cb, error_cb):
    conf = {
        'security.protocol': 'sasl_plaintext',
        'sasl.mechanisms': 'OAUTHBEARER',
        'sasl.oauthbearer.config': 'oauth_cb',
        'oauth_cb': oauth_cb,
        'error_cb': error_cb,
    }
    if kind == 'producer':
        return Producer(conf)
    if kind == 'admin':
        return AdminClient(conf)
    if kind == 'share-consumer':
        return ShareConsumer(dict(conf, **{'group.id': 'test', 'bootstrap.servers': 'localhost:19999'}))
    return TestConsumer(dict(conf, **{'group.id': 'test', 'session.timeout.ms': 1000}))


@pytest.mark.parametrize('failure', sorted(OAUTH_CB_FAILURES))
@pytest.mark.parametrize('kind', ['consumer', 'producer', 'admin', 'share-consumer'])
def test_oauth_cb_failure_fails_construction_with_its_error(kind, failure):
    """
    A failed oauth_cb makes the constructor raise the callback's error right
    away. It used to wait out the 10 second token timeout and raise a timeout
    error, or a SystemError for a consumer with an error_cb.
    """
    oauth_cb, message = OAUTH_CB_FAILURES[failure]
    errors = []

    start = time.monotonic()
    with pytest.raises(KafkaException) as exc_info:
        _make_client(kind, oauth_cb, errors.append)
    assert time.monotonic() - start < 5

    error = exc_info.value.args[0]
    assert error.code() == KafkaError.SASL_AUTHENTICATION_FAILED
    assert message in error.str()


def test_oauth_cb_token_wait_is_interruptible():
    """
    Ctrl+C while the constructor waits for the first token raises
    KeyboardInterrupt. The wait used to ignore it, then fail with a
    SystemError once the token came.
    """

    def oauth_cb(oauth_config):
        time.sleep(2)
        return 'token', time.time() + 300.0

    interrupt_thread = threading.Thread(target=lambda: TestUtils.send_sigint_after_delay(0.3))
    interrupt_thread.daemon = True
    interrupt_thread.start()

    with pytest.raises(KeyboardInterrupt):
        TestConsumer(get_oauth_config(oauth_cb))


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
