#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Unit tests for ShareConsumer callback dispatch.

Covers the legacy librdkafka callbacks routed through common_conf_setup —
error_cb, throttle_cb, stats_cb, log_cb, and oauthbearer_token_refresh_cb —
plus the share-consumer-specific paths where they may be dispatched
(poll, commit_sync, commit_async). Split out of test_ShareConsumer.py so
the callback contract can be evolved without churning the constructor /
subscribe / lifecycle test file.
"""

import logging
import time

import pytest

from confluent_kafka import Consumer, KafkaError, KafkaException, Producer, ShareConsumer
from tests.common import unique_id


def _librdkafka_has_openssl():
    """Detect whether the linked librdkafka was built with OpenSSL.

    Without OpenSSL, OAUTHBEARER config keys (sasl.oauthbearer.config,
    security.protocol=sasl_ssl/sasl_plaintext + OAUTHBEARER mechanism) are
    rejected at conf-set time, before the binding's oauth wiring ever runs.
    Probes the simplest gate: try to set security.protocol=sasl_ssl on a
    Producer. The only failure that maps to "no OpenSSL" is the explicit
    `OpenSSL not available at build time` rejection — any other error
    (e.g. missing Kerberos keytab when GSSAPI is the default mechanism)
    means we got past the OpenSSL gate, so OpenSSL is available.
    """
    try:
        Producer({'security.protocol': 'sasl_ssl'})
    except KafkaException as exc:
        return 'OpenSSL not available' not in str(exc)
    return True


_OPENSSL_AVAILABLE = _librdkafka_has_openssl()


def test_error_cb():
    """Test that error_cb fires for ShareConsumer when broker is unreachable."""
    error_called = []

    def my_error_cb(error):
        error_called.append(error)

    sc = ShareConsumer(
        {
            'group.id': unique_id('test-share-error-cb'),
            'bootstrap.servers': 'localhost:19999',
            'socket.timeout.ms': 100,
            'error_cb': my_error_cb,
        }
    )

    sc.subscribe(['test-topic'])
    sc.poll(timeout=0.5)

    assert len(error_called) > 0, "error_cb should have been called"
    assert isinstance(error_called[0], KafkaError)
    assert error_called[0].code() in (KafkaError._TRANSPORT, KafkaError._ALL_BROKERS_DOWN)
    sc.close()


def test_error_cb_exception_propagates():
    """Test that an exception raised in error_cb propagates to poll.

    Scope: only the poll-time propagation path. The teardown disables the
    callback's raise behaviour before close() so this test isn't coupled to
    close-time semantics — those are pinned down separately in
    test_error_cb_exception_during_close.
    """
    error_called = []
    raising = [True]

    def error_cb_that_raises(error):
        error_called.append(error)
        if raising[0]:
            raise RuntimeError("Test exception from error_cb")

    sc = ShareConsumer(
        {
            'group.id': unique_id('test-share-error-cb-exc'),
            'bootstrap.servers': 'localhost:19999',
            'socket.timeout.ms': 100,
            'error_cb': error_cb_that_raises,
        }
    )

    sc.subscribe(['test-topic'])

    with pytest.raises(RuntimeError) as exc_info:
        sc.poll(timeout=0.5)

    assert "Test exception from error_cb" in str(exc_info.value)
    assert len(error_called) > 0

    # Disarm before close so this test only asserts poll-time behavior.
    raising[0] = False
    sc.close()


def test_error_cb_disarm_before_close():
    """Pin down the disarm-before-close recipe for an infallible close().

    Whether close() surfaces a callback-raised exception is timing-dependent:
    librdkafka may or may not have OP_ERR ops queued in rk_rep at the moment
    of the close drain, depending on broker state and rate-limiter timing.
    See librdkafka rdkafka.c:5143-5159 (the close drain loop, which ignores
    the YIELD flag — "Ignore YIELD, we need to finish").

    Rather than test that flaky path, this test pins the user-facing
    workaround: gate error_cb's raise behavior behind a flag, flip the flag
    off before close(). This makes close() deterministic regardless of
    librdkafka's internal queue state.

    Replaces the previously skipped test_error_cb_exception_during_close,
    which was asserting a librdkafka contract that doesn't exist.
    """
    raising = [True]
    error_called = []

    def error_cb_that_raises(error):
        error_called.append(error)
        if raising[0]:
            raise RuntimeError("Intentional exception from error_cb")

    sc = ShareConsumer(
        {
            'group.id': unique_id('test-share-disarm-before-close'),
            'bootstrap.servers': 'localhost:19999',
            'socket.timeout.ms': 100,
            'error_cb': error_cb_that_raises,
        }
    )
    sc.subscribe(['test-topic'])

    # Confirm the callback IS armed and surfaces through poll(): without
    # this, the "disarm" step below would be a no-op and the test would
    # silently lose its point.
    with pytest.raises(RuntimeError, match="Intentional exception from error_cb"):
        sc.poll(timeout=0.5)

    assert error_called, "error_cb should have fired before disarm"
    invocations_before_close = len(error_called)

    # Disarm — subsequent error_cb invocations no-op instead of raising.
    raising[0] = False

    # close() must return cleanly even if librdkafka dispatches OP_ERR ops
    # from its close drain. The disarmed callback can be invoked, but it
    # cannot raise.
    sc.close()

    # Sanity: any post-disarm invocations were accepted, just not raising.
    assert len(error_called) >= invocations_before_close


def test_throttle_cb():
    """Test that throttle_cb can be registered without crashing.

    throttle_cb requires broker-side throttling to fire, which can't be
    triggered in a unit test. We verify it can be set and doesn't crash.
    """
    throttle_called = []

    def my_throttle_cb(event):
        throttle_called.append(event)

    sc = ShareConsumer(
        {
            'group.id': unique_id('test-share-throttle-cb'),
            'bootstrap.servers': 'localhost:19999',
            'socket.timeout.ms': 100,
            'throttle_cb': my_throttle_cb,
        }
    )

    sc.subscribe(['test-topic'])

    # throttle_cb won't fire without broker throttling — just verify no crash
    sc.poll(timeout=0.2)
    sc.close()


def test_stats_cb_rejected():
    """stats_cb is rejected on ShareConsumer at construction time.

    The KIP-932 share-consumer JSON statistics surface isn't designed yet:
    librdkafka's stats blob references cgrp / per-partition state that
    doesn't translate to share groups, so what users would get back is
    incomplete and misleading. Rather than silently accept the callback
    and feed it half-baked data, the binding rejects stats_cb at config
    time (ShareConsumer.c — pre-filter over args[0] + kwargs before
    common_conf_setup is called).

    Pinned in both forms: dict-config and kwargs-form, for both stats_cb
    and the matching statistics.interval.ms rejection (no callback → no
    point starting the timer).
    """
    config = {
        'group.id': unique_id('test-share-stats-cb-rejected'),
        'bootstrap.servers': 'localhost:9092',
    }

    def my_stats_cb(stats_json_str):
        pass

    with pytest.raises(ValueError, match='stats_cb is not supported'):
        ShareConsumer({**config, 'stats_cb': my_stats_cb})

    with pytest.raises(ValueError, match='stats_cb is not supported'):
        ShareConsumer(config, stats_cb=my_stats_cb)

    with pytest.raises(ValueError, match='statistics.interval.ms is not supported'):
        ShareConsumer({**config, 'statistics.interval.ms': 200})

    with pytest.raises(ValueError, match='statistics.interval.ms is not supported'):
        ShareConsumer(config, **{'statistics.interval.ms': 200})


def test_log_cb():
    """Test that log_cb routes librdkafka log records to the Python logger.

    Mirrors test_log.py::test_consumer_logger_logging_in_given_format —
    debug=msg is enough to produce a synchronous INIT record on construction.
    ShareConsumer_init forwards the share consumer's log queue onto rk_rep
    via rd_kafka_share_set_log_queue(), so log records flow through the same
    drain that share_consume_batch performs on the poll thread.
    """
    log_records = []

    class _CollectingHandler(logging.Handler):
        def emit(self, record):
            log_records.append(record)

    logger = logging.getLogger(unique_id('test-share-log-cb'))
    logger.setLevel(logging.DEBUG)
    logger.addHandler(_CollectingHandler())

    sc = ShareConsumer(
        {
            'group.id': unique_id('test-share-log-cb'),
            'bootstrap.servers': 'localhost:19999',
            'socket.timeout.ms': 100,
            'debug': 'msg',
            'logger': logger,
        }
    )

    sc.poll(timeout=0.2)
    sc.close()

    assert any('INIT' in r.getMessage() for r in log_records), "log_cb should have routed an INIT record to the logger"


def test_log_cb_requires_polling():
    """Share consumer has no background log drainer — records sit in
    rk_logq (forwarded to rk_rep) until poll() / commit_sync /
    commit_async runs.

    Distinguishes ShareConsumer from regular Consumer in user-visible
    behavior: a busy app thread that pauses polling silently accumulates
    log records rather than streaming them to the logger. Pin this
    contract in code so docs (Task #14) can cite a test, not speculation.
    """
    log_records = []

    class _CollectingHandler(logging.Handler):
        def emit(self, record):
            log_records.append(record)

    logger = logging.getLogger(unique_id('test-share-log-needs-poll'))
    logger.setLevel(logging.DEBUG)
    logger.addHandler(_CollectingHandler())

    sc = ShareConsumer(
        {
            'group.id': unique_id('test-share-log-needs-poll'),
            'bootstrap.servers': 'localhost:19999',
            'socket.timeout.ms': 100,
            'debug': 'msg',
            'logger': logger,
        }
    )

    # Without poll(), the share consumer never drains its log queue.
    # librdkafka emits INIT records during rd_kafka_share_consumer_new
    # but they sit in rk_logq → rk_rep until something pumps the queue.
    time.sleep(0.3)
    assert not log_records, (
        f"no log records should surface before poll(); got " f"{[r.getMessage() for r in log_records]}"
    )

    # One poll() drains the queue and INIT records reach the logger.
    sc.poll(timeout=0.2)
    assert log_records, "poll() should drain the share consumer log queue"
    sc.close()


@pytest.mark.skipif(
    not _OPENSSL_AVAILABLE,
    reason="librdkafka built without OpenSSL — OAUTHBEARER config rejected at conf-set time",
)
def test_oauthbearer_token_refresh_cb():
    """Test that oauth_cb is invoked when ShareConsumer needs a SASL token.

    Mirrors test_oauth_cb.py::test_oauth_cb. ShareConsumer_init calls
    rd_kafka_share_sasl_background_callbacks_enable + wait_for_oauth_token_set,
    so the callback fires on librdkafka's background thread during init and
    the constructor blocks until the initial token is set.

    Requires librdkafka built with OpenSSL; on builds without it the
    sasl.oauthbearer.config key is rejected at conf-set time before our
    wiring runs.
    """
    oauth_cb_called = []

    def my_oauth_cb(oauth_config):
        oauth_cb_called.append(oauth_config)
        return 'token', time.time() + 300.0

    sc = ShareConsumer(
        {
            'group.id': unique_id('test-share-oauth-cb'),
            'bootstrap.servers': 'localhost:19999',
            'socket.timeout.ms': 100,
            'security.protocol': 'sasl_plaintext',
            'sasl.mechanisms': 'OAUTHBEARER',
            'sasl.oauthbearer.config': 'oauth_cb',
            'oauth_cb': my_oauth_cb,
        }
    )

    assert oauth_cb_called, "oauth_cb should have been called during init"
    assert oauth_cb_called[0] == 'oauth_cb'
    sc.close()


@pytest.mark.skipif(
    not _OPENSSL_AVAILABLE,
    reason="librdkafka built without OpenSSL — OAUTHBEARER config rejected at conf-set time",
)
@pytest.mark.parametrize('client_kind', ['producer', 'consumer', 'share_consumer'])
def test_oauth_cb_fires_across_client_types(client_kind):
    """oauth_cb should fire for a producer, consumer and share consumer alike.

    The C callbacks now use the rk that librdkafka passes in rather than
    h->rk, which is NULL on a ShareConsumer. Producers and consumers have a
    non-NULL h->rk, so we run the same oauth config through all three to
    confirm that switch didn't break them.

    No poll() needed: every client blocks until the first token is set during
    init, so oauth_cb has already run by the time the constructor returns.
    """
    oauth_configs = []

    def my_oauth_cb(oauth_config):
        oauth_configs.append(oauth_config)
        return 'token', time.time() + 300.0

    conf = {
        'bootstrap.servers': 'localhost:19999',
        'socket.timeout.ms': 100,
        'security.protocol': 'sasl_plaintext',
        'sasl.mechanisms': 'OAUTHBEARER',
        'sasl.oauthbearer.config': 'oauth_cb',
        'oauth_cb': my_oauth_cb,
    }

    if client_kind == 'producer':
        client = Producer(conf)
    elif client_kind == 'consumer':
        client = Consumer({**conf, 'group.id': unique_id('test-oauth-consumer')})
    else:
        client = ShareConsumer({**conf, 'group.id': unique_id('test-oauth-share')})

    try:
        assert oauth_configs, f"oauth_cb should fire during {client_kind} init"
        assert oauth_configs[0] == 'oauth_cb'
    finally:
        # Producer exposes no close(); GC-time destroy is enough for it.
        if hasattr(client, 'close'):
            client.close()


@pytest.mark.skipif(
    not _OPENSSL_AVAILABLE,
    reason="librdkafka built without OpenSSL — OAUTHBEARER config rejected at conf-set time",
)
def test_mixed_callbacks_coexist_on_share_consumer():
    """oauth_cb, error_cb and log_cb wired onto a single ShareConsumer.

    oauth_cb and error_cb are the callbacks that switched from h->rk to the
    rk argument, so this checks they still fire when set together and that
    log_cb riding along doesn't get in the way. The three arrive by different
    routes: oauth_cb on the SASL background thread during init, error_cb and
    log_cb out of the poll() drain (the dead broker gives a transport error,
    debug=msg gives INIT log records).
    """
    oauth_configs = []
    errors = []
    log_records = []

    def my_oauth_cb(oauth_config):
        oauth_configs.append(oauth_config)
        return 'token', time.time() + 300.0

    def my_error_cb(err):
        errors.append(err)

    class _CollectingHandler(logging.Handler):
        def emit(self, record):
            log_records.append(record)

    logger = logging.getLogger(unique_id('test-share-mixed-cb'))
    logger.setLevel(logging.DEBUG)
    logger.addHandler(_CollectingHandler())

    sc = ShareConsumer(
        {
            'group.id': unique_id('test-share-mixed-cb'),
            'bootstrap.servers': 'localhost:19999',
            'socket.timeout.ms': 100,
            'security.protocol': 'sasl_plaintext',
            'sasl.mechanisms': 'OAUTHBEARER',
            'sasl.oauthbearer.config': 'oauth_cb',
            'oauth_cb': my_oauth_cb,
            'error_cb': my_error_cb,
            'debug': 'msg',
            'logger': logger,
        }
    )

    # Already fired: the constructor blocks until the background thread
    # sets the first token.
    assert oauth_configs, "oauth_cb should fire during init"
    assert oauth_configs[0] == 'oauth_cb'

    sc.subscribe(['test-topic'])

    # error_cb and the buffered log records come out of the poll drain.
    # Poll a few times in case the first one beats the broker-down error
    # into the queue.
    deadline = time.monotonic() + 3.0
    while not errors and time.monotonic() < deadline:
        sc.poll(timeout=0.5)

    assert errors, "error_cb should fire from the poll drain"
    assert isinstance(errors[0], KafkaError)
    assert any(
        'INIT' in record.getMessage() for record in log_records
    ), "log_cb should forward INIT records once poll drains the log queue"

    sc.close()


def test_error_cb_dispatches_during_commit_sync():
    """Verifies the CallState wrap in ShareConsumer_commit_sync.

    rd_kafka_share_commit_sync drains rk_rep on entry (librdkafka
    rdkafka.c:4316), so a pending OP_ERR fires error_cb from inside
    commit_sync. Before the wrap, the trampoline's CallState_get()
    assertion would abort the process. With the wrap, dispatch is clean
    and commit_sync returns normally.
    """
    error_called = []

    def my_error_cb(err):
        error_called.append(err)

    sc = ShareConsumer(
        {
            'group.id': unique_id('test-share-cb-commit-sync'),
            'bootstrap.servers': 'localhost:19999',
            'socket.timeout.ms': 100,
            'share.acknowledgement.mode': 'implicit',
            'error_cb': my_error_cb,
        }
    )
    sc.subscribe(['test-topic'])

    # Let broker-connection-refused OP_ERR ops accumulate without polling
    # them, so commit_sync's entry drain has something to dispatch.
    time.sleep(0.3)

    result = sc.commit_sync(timeout=0.5)

    assert result == {}, "no pending acks expected"
    assert error_called, "error_cb should have fired from commit_sync's drain"
    sc.close()


def test_error_cb_dispatches_during_commit_async():
    """Verifies the CallState wrap in ShareConsumer_commit_async.

    rd_kafka_share_commit_async drains rk_rep on entry (librdkafka
    rdkafka.c:4246). Same crash-vs-clean contract as commit_sync.
    """
    error_called = []

    def my_error_cb(err):
        error_called.append(err)

    sc = ShareConsumer(
        {
            'group.id': unique_id('test-share-cb-commit-async'),
            'bootstrap.servers': 'localhost:19999',
            'socket.timeout.ms': 100,
            'share.acknowledgement.mode': 'implicit',
            'error_cb': my_error_cb,
        }
    )
    sc.subscribe(['test-topic'])

    time.sleep(0.3)

    sc.commit_async()  # returns immediately; no exception expected

    assert error_called, "error_cb should have fired from commit_async's drain"
    sc.close()
