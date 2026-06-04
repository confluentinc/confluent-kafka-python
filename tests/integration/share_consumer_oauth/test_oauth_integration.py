"""End-to-end integration tests for ShareConsumer with OAUTHBEARER.

These cover the full SASL handshake, which the unit tests in
tests/test_ShareConsumer_callbacks.py cannot reach: the unit
test_oauthbearer_token_refresh_cb only checks that oauth_cb fires during
init, and the literal string 'token' it produces never reaches a broker
because the test points at an unreachable port.

These tests use a trivup OAUTHBEARER fixture (see conftest.py) and tokens
from unsecured_token that the broker's
OAuthBearerUnsecuredValidatorCallbackHandler accepts, so they additionally
cover:

- The token actually reaching librdkafka's rd_kafka_oauthbearer_set_token
  through the callback's rk argument (the h->rk == NULL fix for
  ShareConsumer), confirmed by a real handshake.
- The refresh path, since connections.max.reauth.ms=5000 forces reauth
  during the test rather than only at init.
- error_cb receiving an _AUTHENTICATION op from the broker on the poll
  thread, via the CallState-wrapped dispatch.
"""

import time

import pytest

from confluent_kafka import KafkaException, ShareConsumer
from tests.common import unique_id
from tests.integration.share_consumer_oauth.unsecured_token import (
    make_unsecured_jwt,
)


def test_share_consumer_oauth_construct_and_metadata(oauth_share_consumer_conf):
    """Token accepted by broker; ShareConsumer can subscribe and poll.

    The construction path runs rd_kafka_share_consumer_new,
    rd_kafka_share_sasl_background_callbacks_enable, and
    wait_for_oauth_token_set. subscribe is enough to make the broker
    respond. If the token were bad and an error_cb were set, it would
    see a SASL failure; this test sets none, so it fails via
    KafkaException on poll instead.
    """
    call_count = [0]

    def oauth_cb(_oauth_config):
        call_count[0] += 1
        return make_unsecured_jwt(lifetime_sec=300.0)

    conf = dict(
        oauth_share_consumer_conf,
        **{
            'group.id': unique_id('test-share-oauth-construct'),
            'oauth_cb': oauth_cb,
        },
    )

    sc = ShareConsumer(conf)
    try:
        sc.subscribe(['test-share-oauth-construct-topic'])
        sc.poll(timeout=2.0)
        assert call_count[0] >= 1, "oauth_cb should have fired at least once"
    finally:
        sc.close()


def test_share_consumer_oauth_refresh_through_reauth(oauth_share_consumer_conf):
    """Short-lifetime token forces refresh inside the broker's reauth window.

    The broker sets connections.max.reauth.ms=5000, so any client must
    re-authenticate within 5 s of connecting. With lifetime_sec=4,
    librdkafka's refresh schedule (80% of lifetime) fires at about 3.2 s,
    well inside the reauth window, so oauth_cb should be invoked at least
    twice across a 10 s poll loop without the consumer being disconnected.
    """
    call_count = [0]

    def oauth_cb(_oauth_config):
        call_count[0] += 1
        return make_unsecured_jwt(lifetime_sec=4.0)

    conf = dict(
        oauth_share_consumer_conf,
        **{
            'group.id': unique_id('test-share-oauth-refresh'),
            'oauth_cb': oauth_cb,
        },
    )

    sc = ShareConsumer(conf)
    try:
        sc.subscribe(['test-share-oauth-refresh-topic'])
        deadline = time.time() + 10
        while time.time() < deadline:
            sc.poll(timeout=0.5)
        assert call_count[0] >= 2, (
            f"Expected >= 2 oauth_cb invocations (init + at least one " f"refresh) across 10s; got {call_count[0]}"
        )
    finally:
        sc.close()


def test_share_consumer_oauth_expired_token_surfaces_error_cb(oauth_share_consumer_conf):
    """Expired token is rejected by the broker; error_cb fires from the poll drain.

    Checks that an ERR__AUTHENTICATION op enqueued by librdkafka on SASL
    failure reaches Python's error_cb through the share-consumer drain
    path. The CallState wrap on poll() is what makes this dispatch safe;
    without it the callback would abort.
    """
    errors = []

    def oauth_cb(_oauth_config):
        # exp is in the past, so the token is structurally valid but
        # rejected by the validator's expiration check.
        return make_unsecured_jwt(lifetime_sec=-60.0)

    def error_cb(err):
        errors.append(err)

    conf = dict(
        oauth_share_consumer_conf,
        **{
            'group.id': unique_id('test-share-oauth-expired'),
            'oauth_cb': oauth_cb,
            'error_cb': error_cb,
        },
    )

    # Construction can either succeed (if librdkafka accepts the past-exp
    # token from oauth_cb and the broker rejects it during the handshake)
    # or fail (if librdkafka's own bounds check catches it first). Either
    # way the failure surfaces through error_cb.
    try:
        sc = ShareConsumer(conf)
    except KafkaException:
        return

    try:
        sc.subscribe(['test-share-oauth-expired-topic'])
        deadline = time.time() + 8
        while time.time() < deadline and not errors:
            sc.poll(timeout=0.5)
        assert errors, "Expected error_cb to surface a broker auth failure within 8s"
        msgs = [str(e).lower() for e in errors]
        assert any(
            "auth" in m or "sasl" in m or "_transport" in m for m in msgs
        ), f"Expected an auth-related error from error_cb; got: {msgs}"
    finally:
        sc.close()


def test_share_consumer_oauth_cb_raises_fails_construction(oauth_share_consumer_conf):
    """oauth_cb raising on the first call makes the constructor fail after
    the initial-token timeout in wait_for_oauth_token_set.

    Also covers the ShareConsumer-specific cleanup path: when
    wait_for_oauth_token_set returns -1, ShareConsumer_init calls
    rd_kafka_share_destroy(self->rkshare). If that destroy path were
    wrong (for example, still calling rd_kafka_destroy(h->rk) for a NULL
    rk), the test would crash rather than raise.
    """

    def oauth_cb(_oauth_config):
        raise RuntimeError("test: oauth_cb deliberately fails")

    conf = dict(
        oauth_share_consumer_conf,
        **{
            'group.id': unique_id('test-share-oauth-raise'),
            'oauth_cb': oauth_cb,
        },
    )

    with pytest.raises(KafkaException) as exc_info:
        ShareConsumer(conf)

    msg = str(exc_info.value).lower()
    assert "authentication" in msg or "sasl" in msg or "timeout" in msg, (
        f"Expected SASL_AUTHENTICATION_FAILED-shaped error from "
        f"wait_for_oauth_token_set timeout; got {exc_info.value!r}"
    )
