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

"""M5 — client integration contract tests.

Two flavours:

- Flavour 1: pure-Python contract-shape tests. Verify that
  ``AwsStsTokenProvider.token`` returns the 4-tuple shape the C-extension
  oauth_cb contract expects (see confluent_kafka.c:2291, `PyArg_ParseTuple`
  format "sd|sO!").

- Flavour 2: integration with Producer / Consumer / AdminClient. Build a
  client with ``oauth_cb=provider.token`` and verify the callback is invoked
  by librdkafka during client initialisation. Matches the pattern at
  tests/test_oauth_cb.py:38-52.
"""

import base64
import json
import time
from datetime import datetime, timezone
from unittest.mock import patch

import pytest
from botocore.stub import Stubber

from confluent_kafka.oauthbearer.aws import AwsOAuthConfig, AwsStsTokenProvider


def _build_jwt(sub: str) -> str:
    body = json.dumps({"sub": sub}).encode("utf-8")
    encoded = base64.urlsafe_b64encode(body).decode("ascii").rstrip("=")
    return f"h.{encoded}.s"


def _make_provider_with_canned_response(
    sub: str = "arn:aws:iam::1:role/R",
    expiration: datetime = None,
) -> AwsStsTokenProvider:
    if expiration is None:
        expiration = datetime.fromtimestamp(time.time() + 300, tz=timezone.utc)
    provider = AwsStsTokenProvider(
        AwsOAuthConfig(region="us-east-1", audience="https://example.com")
    )
    stubber = Stubber(provider._sts)
    # Queue enough responses to cover multiple refreshes during client init.
    for _ in range(10):
        stubber.add_response(
            "get_web_identity_token",
            {"WebIdentityToken": _build_jwt(sub), "Expiration": expiration},
        )
    stubber.activate()
    return provider


# =========================================================================
# Flavour 1 — pure contract-shape tests (no librdkafka, no network)
# =========================================================================


def test_token_returns_four_tuple():
    provider = _make_provider_with_canned_response()
    result = provider.token()
    assert isinstance(result, tuple)
    assert len(result) == 4


def test_token_element_types():
    provider = _make_provider_with_canned_response(sub="arn:aws:iam::1:role/R")
    token_str, expiry, principal, extensions = provider.token()
    assert isinstance(token_str, str)
    assert isinstance(expiry, float)
    assert isinstance(principal, str)
    assert isinstance(extensions, dict)


def test_token_expiry_is_epoch_seconds_not_ms():
    exp = datetime(2026, 4, 21, 6, 6, 47, 641000, tzinfo=timezone.utc)
    provider = AwsStsTokenProvider(
        AwsOAuthConfig(region="us-east-1", audience="https://example.com")
    )
    stubber = Stubber(provider._sts)
    stubber.add_response(
        "get_web_identity_token",
        {"WebIdentityToken": _build_jwt("arn:aws:iam::1:role/R"), "Expiration": exp},
    )
    with stubber:
        _, expiry, _, _ = provider.token()
    assert expiry == exp.timestamp()
    # Sanity: 2026-04-21 in epoch seconds is around 1776836807.
    assert 1.7e9 < expiry < 1.8e9


def test_token_unpacks_as_oauth_cb_contract():
    """The return value must unpack cleanly as (token, expiry[, principal, extensions])
    which is what PyArg_ParseTuple(result, "sd|sO!", ...) at confluent_kafka.c:2291
    requires. We simulate that C-side unpacking in pure Python.
    """
    provider = _make_provider_with_canned_response()
    result = provider.token()
    # Two-element unpack (minimum librdkafka contract)
    token, expiry = result[0], result[1]
    assert isinstance(token, str) and isinstance(expiry, float)
    # Four-element unpack (with principal + extensions)
    token, expiry, principal, extensions = result
    assert isinstance(principal, str) and isinstance(extensions, dict)


def test_token_signature_takes_one_string_arg():
    """oauth_cb is invoked by the C extension as `cb(oauthbearer_config_str)`;
    see confluent_kafka.c:2285. Our provider.token must accept a single string
    positional argument.
    """
    provider = _make_provider_with_canned_response()
    # Mimic the C extension's invocation shape
    result = provider.token("some-sasl.oauthbearer.config-value")
    assert len(result) == 4


# =========================================================================
# Flavour 2 — Producer / Consumer / AdminClient integration
# =========================================================================


def _oauth_client_config(oauth_cb, session_timeout_ms=1000):
    """Mirror tests/test_oauth_cb.py:27-36 oauth_cb config shape."""
    return {
        "group.id": "test-aws-oauth",
        "security.protocol": "sasl_plaintext",
        "sasl.mechanisms": "OAUTHBEARER",
        "session.timeout.ms": session_timeout_ms,
        "sasl.oauthbearer.config": "test",
        "oauth_cb": oauth_cb,
    }


def test_consumer_invokes_provider_token_during_init():
    from confluent_kafka import Consumer

    provider = _make_provider_with_canned_response()
    invocations = []
    real_token = provider.token

    def instrumented(oauthbearer_config=""):
        invocations.append(oauthbearer_config)
        return real_token(oauthbearer_config)

    provider.token = instrumented  # type: ignore[method-assign]
    consumer = Consumer(_oauth_client_config(provider.token))
    try:
        # Callback fires during client init for the initial token refresh.
        assert len(invocations) >= 1
        assert invocations[0] == "test"
    finally:
        consumer.close()


def test_producer_invokes_provider_token_during_init():
    from confluent_kafka import Producer

    provider = _make_provider_with_canned_response()
    invocations = []
    real_token = provider.token

    def instrumented(oauthbearer_config=""):
        invocations.append(oauthbearer_config)
        return real_token(oauthbearer_config)

    provider.token = instrumented  # type: ignore[method-assign]
    cfg = {
        "security.protocol": "sasl_plaintext",
        "sasl.mechanisms": "OAUTHBEARER",
        "sasl.oauthbearer.config": "test",
        "oauth_cb": provider.token,
    }
    producer = Producer(cfg)
    try:
        # Producer construction also triggers an initial refresh.
        # Poll briefly to give librdkafka time to call the cb.
        producer.poll(0.5)
        assert len(invocations) >= 1
    finally:
        producer.flush(1.0)


def test_admin_client_invokes_provider_token_during_init():
    from confluent_kafka.admin import AdminClient

    provider = _make_provider_with_canned_response()
    invocations = []
    real_token = provider.token

    def instrumented(oauthbearer_config=""):
        invocations.append(oauthbearer_config)
        return real_token(oauthbearer_config)

    provider.token = instrumented  # type: ignore[method-assign]
    cfg = {
        "bootstrap.servers": "localhost:9092",
        "security.protocol": "sasl_plaintext",
        "sasl.mechanisms": "OAUTHBEARER",
        "sasl.oauthbearer.config": "test",
        "oauth_cb": provider.token,
    }
    admin = AdminClient(cfg)
    try:
        # Give librdkafka a moment to service the initial refresh event.
        admin.poll(0.5)
        assert len(invocations) >= 1
    finally:
        # AdminClient has no close(); rely on GC.
        del admin


def test_callback_exception_converted_to_set_token_failure():
    """If provider.token raises (e.g. STS AccessDenied), the C-extension
    oauth_cb swallows the Python exception and calls
    rd_kafka_oauthbearer_set_token_failure (see confluent_kafka.c:2337-2344).

    Downstream, librdkafka retries the refresh and eventually times out its
    SASL handshake with a KafkaException — that is the _expected_ failure
    shape, not a Python-side RuntimeError leak. This test proves the C-side
    conversion is happening: the original RuntimeError never reaches the
    caller.
    """
    import pytest as _pytest

    from confluent_kafka import Consumer, KafkaException

    def always_raises(oauthbearer_config=""):
        raise RuntimeError("simulated-sts-failure-marker-xyz")

    # Building the Consumer triggers the initial oauth refresh. librdkafka will
    # time out after ~10s of set_token_failure calls and raise a KafkaException
    # from the constructor. The ORIGINAL RuntimeError must be nowhere in sight.
    with _pytest.raises(KafkaException) as exc_info:
        Consumer(_oauth_client_config(always_raises))
    msg = str(exc_info.value)
    assert "simulated-sts-failure-marker-xyz" not in msg, (
        "Original Python exception leaked through the C layer"
    )
    assert "OAuth" in msg or "SASL" in msg
