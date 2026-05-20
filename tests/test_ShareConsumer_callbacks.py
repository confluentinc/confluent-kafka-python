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

import json
import logging
import time

import pytest

from confluent_kafka import KafkaError, ShareConsumer
from tests.common import unique_id


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


def test_stats_cb():
    """Test that stats_cb fires with valid JSON for ShareConsumer.

    librdkafka emits a stats event every statistics.interval.ms regardless of
    broker reachability, so we don't need a live broker to verify dispatch.
    """
    seen_stats_cb = False

    def my_stats_cb(stats_json_str):
        nonlocal seen_stats_cb
        seen_stats_cb = True
        stats_json = json.loads(stats_json_str)
        assert len(stats_json['name']) > 0

    sc = ShareConsumer(
        {
            'group.id': unique_id('test-share-stats-cb'),
            'bootstrap.servers': 'localhost:19999',
            'socket.timeout.ms': 100,
            'statistics.interval.ms': 200,
            'stats_cb': my_stats_cb,
        }
    )

    sc.subscribe(['test-topic'])

    deadline = time.monotonic() + 5.0
    while not seen_stats_cb and time.monotonic() < deadline:
        sc.poll(timeout=0.1)

    assert seen_stats_cb, "stats_cb should have been called"
    sc.close()


# TODO KIP-932: ShareConsumer_init does not call rd_kafka_set_log_queue() —
# librdkafka exposes no rd_kafka_share_set_log_queue() wrapper today
# (rd_kafka_t* is opaque inside rd_kafka_share_t). common_conf_setup still
# sets log.queue=true + log_cb when `logger` is configured, but the log
# queue is never forwarded to the polled queue, so records never reach the
# Python logger. Flip @pytest.mark.skip off once the wrapper lands and
# ShareConsumer_init forwards the queue.
@pytest.mark.skip(reason="TODO KIP-932: rd_kafka_share_set_log_queue() not yet exposed by librdkafka")
def test_log_cb():
    """Test that log_cb routes librdkafka log records to the Python logger.

    Mirrors test_log.py::test_consumer_logger_logging_in_given_format —
    debug=msg is enough to produce a synchronous INIT record on construction
    once the log queue is wired into the polled queue.
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

    assert any('INIT' in r.getMessage() for r in log_records), \
        "log_cb should have routed an INIT record to the logger"


# TODO KIP-932: ShareConsumer_init does not call
# rd_kafka_sasl_background_callbacks_enable() nor wait_for_oauth_token_set()
# — librdkafka exposes no share-consumer wrapper for either. The oauth_cb is
# still registered via common_conf_setup but it isn't dispatched on the user
# thread, so the callback never fires for ShareConsumer today. Flip
# @pytest.mark.skip off once both calls land.
@pytest.mark.skip(reason="TODO KIP-932: SASL background callback wiring not yet exposed for share consumer")
def test_oauthbearer_token_refresh_cb():
    """Test that oauth_cb is invoked when ShareConsumer needs a SASL token.

    Mirrors test_oauth_cb.py::test_oauth_cb — the callback should fire during
    client init once wait_for_oauth_token_set() is wired into
    ShareConsumer_init.
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
