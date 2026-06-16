#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Unit tests for ShareConsumer callback dispatch.

Covers the librdkafka callbacks routed through common_conf_setup
(error_cb, throttle_cb, stats_cb, log_cb, and
oauthbearer_token_refresh_cb), plus the share-consumer paths where they
may be dispatched (poll, commit_sync, commit_async). Split out of
test_ShareConsumer.py so the callback tests can change without disturbing
the constructor, subscribe, and lifecycle tests.
"""

import logging
import threading
import time

import pytest

from confluent_kafka import Consumer, KafkaError, KafkaException, Producer, ShareConsumer
from tests.common import unique_id


def _librdkafka_has_openssl():
    """Detect whether the linked librdkafka was built with OpenSSL.

    Without OpenSSL, OAUTHBEARER config keys (sasl.oauthbearer.config,
    security.protocol=sasl_ssl/sasl_plaintext with the OAUTHBEARER
    mechanism) are rejected at conf-set time, before the binding's oauth
    wiring runs. This tries to set security.protocol=sasl_ssl on a
    Producer. The only failure that means "no OpenSSL" is the explicit
    "OpenSSL not available at build time" rejection. Any other error
    (for example, a missing Kerberos keytab when GSSAPI is the default
    mechanism) means we got past the OpenSSL gate, so OpenSSL is available.
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

    This covers only the poll-time propagation path. The teardown disables
    the callback's raise behaviour before close() so this test isn't
    coupled to close-time behaviour, which is covered separately in
    test_error_cb_disarm_before_close.
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
    """Show the disarm-before-close pattern for a close() that won't raise.

    Whether close() surfaces a callback-raised exception is timing
    dependent: librdkafka may or may not have OP_ERR ops queued in rk_rep
    at the moment of the close drain, depending on broker state and
    rate-limiter timing. See librdkafka rdkafka.c:5143-5159 (the close
    drain loop, which ignores the YIELD flag: "Ignore YIELD, we need to
    finish").

    Rather than test that flaky path, this test covers the user-facing
    workaround: gate error_cb's raise behavior behind a flag and flip the
    flag off before close(). This makes close() deterministic regardless
    of librdkafka's internal queue state.

    Replaces the previously skipped test_error_cb_exception_during_close,
    which asserted a librdkafka contract that doesn't exist.
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

    # Confirm the callback is armed and surfaces through poll(). Without
    # this, the "disarm" step below would be a no-op and the test would
    # silently lose its point.
    with pytest.raises(RuntimeError, match="Intentional exception from error_cb"):
        sc.poll(timeout=0.5)

    assert error_called, "error_cb should have fired before disarm"
    invocations_before_close = len(error_called)

    # Disarm: subsequent error_cb invocations no-op instead of raising.
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

    # throttle_cb won't fire without broker throttling; just verify no crash
    sc.poll(timeout=0.2)
    sc.close()


def test_stats_cb_rejected():
    """stats_cb is rejected on ShareConsumer at construction time.

    The KIP-932 share-consumer JSON statistics surface isn't designed
    yet. librdkafka's stats output references cgrp and per-partition state
    that doesn't translate to share groups, so what users would get back
    is incomplete and misleading. Rather than silently accept the callback
    and feed it incomplete data, the binding rejects stats_cb at config
    time (ShareConsumer.c, a pre-filter over args[0] and kwargs before
    common_conf_setup is called).

    Checked in both forms, dict-config and kwargs-form, for both stats_cb
    and the matching statistics.interval.ms rejection (with no callback
    there is no point starting the timer).
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

    Mirrors test_log.py::test_consumer_logger_logging_in_given_format:
    debug=msg is enough to produce a synchronous INIT record on
    construction. ShareConsumer_init forwards the share consumer's log
    queue onto rk_rep via rd_kafka_share_set_log_queue(), so log records
    flow through the same drain that share_poll performs on the
    poll thread.
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

    # poll() raises _STATE unless we're subscribed, so subscribe — no broker
    # needed, the log queue drains before that check.
    sc.subscribe(['test-topic'])
    sc.poll(timeout=0.2)
    sc.close()

    assert any('INIT' in r.getMessage() for r in log_records), "log_cb should have routed an INIT record to the logger"


def test_log_cb_requires_polling():
    """Share consumer has no background log drainer, so records sit in
    rk_logq (forwarded to rk_rep) until poll(), commit_sync, or
    commit_async runs.

    This is a user-visible difference from the regular Consumer: a busy
    app thread that pauses polling silently accumulates log records rather
    than streaming them to the logger. Captured here so docs (Task #14)
    can cite a test rather than speculation.
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

    # poll() needs a subscription or it raises _STATE. subscribe() is async and
    # drains nothing, so the no-logs-yet check below still holds.
    sc.subscribe(['test-topic'])

    # Without poll(), the share consumer never drains its log queue.
    # librdkafka emits INIT records during rd_kafka_share_consumer_new
    # but they sit in rk_logq (forwarded to rk_rep) until something
    # drains the queue.
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
    reason="librdkafka built without OpenSSL; OAUTHBEARER config rejected at conf-set time",
)
def test_oauthbearer_token_refresh_cb():
    """Test that oauth_cb is invoked when ShareConsumer needs a SASL token.

    Mirrors test_oauth_cb.py::test_oauth_cb. ShareConsumer_init calls
    rd_kafka_share_sasl_background_callbacks_enable and
    wait_for_oauth_token_set, so the callback fires on librdkafka's
    background thread during init and the constructor blocks until the
    initial token is set.

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
    reason="librdkafka built without OpenSSL; OAUTHBEARER config rejected at conf-set time",
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
    reason="librdkafka built without OpenSSL; OAUTHBEARER config rejected at conf-set time",
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
    commit_sync. Before the wrap, the callback's CallState_get()
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


def _collecting_logger(name_suffix):
    """Return (logger, records) where records is appended to by a handler."""
    records = []

    class _CollectingHandler(logging.Handler):
        def emit(self, record):
            records.append(record)

    logger = logging.getLogger(unique_id(name_suffix))
    logger.setLevel(logging.DEBUG)
    logger.addHandler(_CollectingHandler())
    return logger, records


def _share_oauth_conf(group_suffix, oauth_cb):
    """OAUTHBEARER config for the oauth tests.

    Like test_oauth_cb.get_oauth_config, minus session.timeout.ms which the
    share consumer rejects.
    """
    return {
        'group.id': unique_id(group_suffix),
        'bootstrap.servers': 'localhost:19999',
        'socket.timeout.ms': 100,
        'security.protocol': 'sasl_plaintext',
        'sasl.mechanisms': 'OAUTHBEARER',
        'sasl.oauthbearer.config': 'oauth_cb',
        'oauth_cb': oauth_cb,
    }


def test_error_cb_resolve_failure():
    """A bad hostname should reach error_cb as _RESOLVE.

    The .invalid TLD never resolves, so we get _RESOLVE instead of the
    connection-refused path test_error_cb already covers.
    """
    codes = []

    def my_error_cb(error):
        codes.append(error.code())

    sc = ShareConsumer(
        {
            'group.id': unique_id('test-share-error-resolve'),
            'bootstrap.servers': 'no-such-host-xyz.invalid:9092',
            'socket.timeout.ms': 200,
            'error_cb': my_error_cb,
        }
    )
    sc.subscribe(['test-topic'])

    deadline = time.monotonic() + 3.0
    while time.monotonic() < deadline and KafkaError._RESOLVE not in codes:
        sc.poll(timeout=0.3)

    assert KafkaError._RESOLVE in codes, f"expected _RESOLVE among error_cb codes, got {codes}"
    sc.close()


def test_error_cb_raise_propagates_from_commit_sync():
    """If error_cb raises, the exception comes back out of commit_sync().

    commit_sync drains pending errors on entry, so a raising error_cb makes
    the commit itself re-raise rather than swallow it.
    """
    raising = [True]

    def error_cb_that_raises(error):
        if raising[0]:
            raise RuntimeError("boom from error_cb in commit_sync")

    sc = ShareConsumer(
        {
            'group.id': unique_id('test-share-cb-raise-commit-sync'),
            'bootstrap.servers': 'localhost:19999',
            'socket.timeout.ms': 100,
            'share.acknowledgement.mode': 'implicit',
            'error_cb': error_cb_that_raises,
        }
    )
    sc.subscribe(['test-topic'])
    time.sleep(0.3)  # let some connection-refused errors pile up first

    with pytest.raises(RuntimeError, match="boom from error_cb in commit_sync"):
        sc.commit_sync(timeout=0.5)

    raising[0] = False  # stop raising so close() is clean
    sc.close()


def test_error_cb_raise_propagates_from_commit_async():
    """Same as the commit_sync case, but for commit_async().

    commit_async drains errors on entry too, so a raising error_cb re-raises
    here as well.
    """
    raising = [True]

    def error_cb_that_raises(error):
        if raising[0]:
            raise RuntimeError("boom from error_cb in commit_async")

    sc = ShareConsumer(
        {
            'group.id': unique_id('test-share-cb-raise-commit-async'),
            'bootstrap.servers': 'localhost:19999',
            'socket.timeout.ms': 100,
            'share.acknowledgement.mode': 'implicit',
            'error_cb': error_cb_that_raises,
        }
    )
    sc.subscribe(['test-topic'])
    time.sleep(0.3)

    with pytest.raises(RuntimeError, match="boom from error_cb in commit_async"):
        sc.commit_async()

    raising[0] = False
    sc.close()


def test_error_cb_fires_during_close_without_polling():
    """error_cb still fires from close() even if we never poll().

    Errors queue up undelivered until something drains them, and close()
    does that drain. So a consumer closed without ever polling still reports
    what piled up.
    """
    calls = []

    def my_error_cb(error):
        calls.append(error.code())

    sc = ShareConsumer(
        {
            'group.id': unique_id('test-share-cb-close-drain'),
            'bootstrap.servers': 'localhost:19999',
            'socket.timeout.ms': 100,
            'error_cb': my_error_cb,
        }
    )
    sc.subscribe(['test-topic'])
    time.sleep(0.4)  # let errors queue up without draining them

    assert not calls, "without poll(), no error_cb should fire before close()"
    sc.close()
    assert calls, "error_cb should fire from the close() drain"


def test_error_cb_cross_thread_call_raises_conflict():
    """Calling the consumer from another thread while error_cb runs -> _CONFLICT.

    The poll thread holds the share lock while error_cb runs, so a second
    thread calling commit_async() at the same time gets _CONFLICT (like
    Java's ConcurrentModificationException).
    """
    captured = []
    in_callback = threading.Event()
    release_callback = threading.Event()

    def my_error_cb(error):
        # block here on the first call so the worker thread races us
        if not in_callback.is_set():
            in_callback.set()
            release_callback.wait(timeout=3.0)

    sc = ShareConsumer(
        {
            'group.id': unique_id('test-share-cb-cross-thread'),
            'bootstrap.servers': 'localhost:19999',
            'socket.timeout.ms': 100,
            'error_cb': my_error_cb,
        }
    )

    def worker():
        if in_callback.wait(timeout=3.0):
            try:
                sc.commit_async()
            except Exception as exc:
                captured.append(exc)
            finally:
                release_callback.set()

    sc.subscribe(['test-topic'])
    thread = threading.Thread(target=worker)
    thread.start()
    sc.poll(timeout=1.5)  # fires error_cb here; worker races us from the other thread
    thread.join(timeout=5.0)
    release_callback.set()  # in case error_cb never fired

    assert captured, "worker thread should have caught a cross-thread error"
    assert isinstance(captured[0], KafkaException)
    assert captured[0].args[0].code() == KafkaError._CONFLICT
    sc.close()


@pytest.mark.skip(
    reason="Calling the consumer from inside error_cb segfaults today: librdkafka's "
    "reentrancy guard only covers the ack-commit callback, not error_cb/log_cb. "
    "Un-skip and check for _STATE once that's fixed."
)
def test_error_cb_reentrant_call_raises_state():
    """Calling the consumer from inside error_cb (same thread) should raise _STATE.

    This is the behavior we want once the reentrancy guard is fixed (matches
    the ack-commit callback and Java's IllegalStateException). Skipped for
    now because it crashes.
    """
    captured = []
    reenter = [True]
    holder = {}

    def my_error_cb(error):
        sc = holder.get('sc')
        if sc is not None and reenter[0]:
            reenter[0] = False
            try:
                sc.poll(timeout=0)
            except KafkaException as exc:
                captured.append(exc)

    sc = ShareConsumer(
        {
            'group.id': unique_id('test-share-cb-reentrant-error'),
            'bootstrap.servers': 'localhost:19999',
            'socket.timeout.ms': 100,
            'error_cb': my_error_cb,
        }
    )
    holder['sc'] = sc
    sc.subscribe(['test-topic'])

    deadline = time.monotonic() + 3.0
    while time.monotonic() < deadline and not captured:
        sc.poll(timeout=0.3)

    assert captured and captured[0].args[0].code() == KafkaError._STATE
    sc.close()


@pytest.mark.skip(
    reason="Same segfault as test_error_cb_reentrant_call_raises_state: the reentrancy "
    "guard doesn't cover log_cb yet. Un-skip and check for _STATE once it does."
)
def test_log_cb_reentrant_call_raises_state():
    """Same reentrancy contract as error_cb, but from inside log_cb.

    log_cb runs on the polling thread too, so calling the consumer from it
    should raise _STATE. Skipped because it crashes today.
    """
    captured = []
    reenter = [True]
    holder = {}

    class _ReentrantHandler(logging.Handler):
        def emit(self, record):
            sc = holder.get('sc')
            if sc is not None and reenter[0]:
                reenter[0] = False
                try:
                    sc.poll(timeout=0)
                except KafkaException as exc:
                    captured.append(exc)

    logger = logging.getLogger(unique_id('test-share-cb-reentrant-log'))
    logger.setLevel(logging.DEBUG)
    logger.addHandler(_ReentrantHandler())

    sc = ShareConsumer(
        {
            'group.id': unique_id('test-share-cb-reentrant-log'),
            'bootstrap.servers': 'localhost:19999',
            'socket.timeout.ms': 100,
            'debug': 'msg',
            'logger': logger,
        }
    )
    holder['sc'] = sc
    sc.subscribe(['test-topic'])
    sc.poll(timeout=0.3)

    assert captured and captured[0].args[0].code() == KafkaError._STATE
    sc.close()


def test_oauth_cb_principal_and_sasl_extensions():
    """oauth_cb can return the full (token, lifetime, principal, extensions) tuple.

    The extra principal and SASL-extensions elements are accepted and
    construction completes (it blocks until the first token is set).
    """
    seen = []

    def my_oauth_cb(oauth_config):
        seen.append(oauth_config)
        return 'token', time.time() + 300.0, oauth_config, {'extone': 'extoneval', 'exttwo': 'exttwoval'}

    sc = ShareConsumer(_share_oauth_conf('test-share-oauth-ext', my_oauth_cb))
    assert seen and seen[0] == 'oauth_cb'
    sc.close()


def test_oauth_cb_token_refresh_success_multiple_calls():
    """The background thread re-invokes oauth_cb as the token expires.

    A short (3s) token lifetime triggers a refresh, so oauth_cb fires more
    than once without any poll() call.
    """
    count = [0]

    def my_oauth_cb(oauth_config):
        count[0] += 1
        return 'token', time.time() + 3.0

    sc = ShareConsumer(_share_oauth_conf('test-share-oauth-refresh-ok', my_oauth_cb))
    assert count[0] == 1, "oauth_cb should fire once during init"

    deadline = time.monotonic() + 6.0
    while count[0] == 1 and time.monotonic() < deadline:
        time.sleep(0.5)

    sc.close()
    assert count[0] > 1, "background thread should refresh the token at least once"


def test_oauth_cb_token_refresh_failure_and_recovery():
    """A failed token refresh is retried and recovers.

    The 2nd call raises; librdkafka retries ~10s later and the 3rd call
    succeeds. One bad refresh shouldn't wedge the token loop.
    """
    count = [0]

    def my_oauth_cb(oauth_config):
        count[0] += 1
        if count[0] == 2:
            raise RuntimeError("intentional refresh failure")
        return 'token', time.time() + 3.0

    sc = ShareConsumer(_share_oauth_conf('test-share-oauth-refresh-fail', my_oauth_cb))
    assert count[0] == 1

    # init + failing refresh + the ~10s retry, so give it a wide window
    deadline = time.monotonic() + 16.0
    while count[0] <= 2 and time.monotonic() < deadline:
        time.sleep(0.5)

    sc.close()
    assert count[0] > 2, "oauth_cb should be retried and recover after a refresh failure"


def test_oauth_cb_malformed_return_fails_construction():
    """A bad oauth_cb return value fails construction cleanly, no crash.

    Returning a bare string instead of a (token, lifetime, ...) tuple means
    no valid token is ever set, so construction times out and raises.
    """

    def my_oauth_cb(oauth_config):
        return 'just-a-token-string'  # not a (token, lifetime, ...) tuple

    with pytest.raises(KafkaException) as exc_info:
        ShareConsumer(_share_oauth_conf('test-share-oauth-malformed', my_oauth_cb))

    message = str(exc_info.value).lower()
    assert 'token' in message or 'authentication' in message or 'sasl' in message


def test_log_cb_drains_on_commit_sync():
    """commit_sync() drains the log queue, same as poll().

    There's no background log thread, so records sit queued until poll or a
    commit runs. test_log_cb covers poll; this covers commit_sync.
    """
    logger, records = _collecting_logger('test-share-log-commit-sync')
    sc = ShareConsumer(
        {
            'group.id': unique_id('test-share-log-commit-sync'),
            'bootstrap.servers': 'localhost:19999',
            'socket.timeout.ms': 100,
            'debug': 'msg',
            'logger': logger,
            'share.acknowledgement.mode': 'implicit',
        }
    )
    sc.subscribe(['test-topic'])
    time.sleep(0.2)
    assert not records, "no records should surface before any drain"

    sc.commit_sync(timeout=0.5)
    assert any('INIT' in r.getMessage() for r in records), "commit_sync should drain the log queue"
    sc.close()


def test_log_cb_drains_on_commit_async():
    """commit_async() drains the log queue too."""
    logger, records = _collecting_logger('test-share-log-commit-async')
    sc = ShareConsumer(
        {
            'group.id': unique_id('test-share-log-commit-async'),
            'bootstrap.servers': 'localhost:19999',
            'socket.timeout.ms': 100,
            'debug': 'msg',
            'logger': logger,
            'share.acknowledgement.mode': 'implicit',
        }
    )
    sc.subscribe(['test-topic'])
    time.sleep(0.2)
    assert not records, "no records should surface before any drain"

    sc.commit_async()
    assert any('INIT' in r.getMessage() for r in records), "commit_async should drain the log queue"
    sc.close()


def test_log_cb_record_format_and_fields():
    """Log records come through as proper LogRecords with the right fields.

    test_log_cb just checks the message; here we also check the logger name
    and that debug=msg gives DEBUG-level records.
    """
    logger, records = _collecting_logger('test-share-log-format')
    sc = ShareConsumer(
        {
            'group.id': unique_id('test-share-log-format'),
            'bootstrap.servers': 'localhost:19999',
            'socket.timeout.ms': 100,
            'debug': 'msg',
            'logger': logger,
        }
    )
    sc.subscribe(['test-topic'])
    sc.poll(timeout=0.2)
    sc.close()

    init_records = [r for r in records if 'INIT' in r.getMessage()]
    assert init_records, "expected an INIT log record"
    record = init_records[0]
    assert record.name == logger.name
    assert record.levelno == logging.DEBUG
    assert record.levelname == 'DEBUG'


def test_log_cb_close_drains_pending_records():
    """close() drains queued log records even if poll() never ran.

    Pairs with test_log_cb_requires_polling, which shows nothing surfaces
    before a drain.
    """
    logger, records = _collecting_logger('test-share-log-close')
    sc = ShareConsumer(
        {
            'group.id': unique_id('test-share-log-close'),
            'bootstrap.servers': 'localhost:19999',
            'socket.timeout.ms': 100,
            'debug': 'msg',
            'logger': logger,
        }
    )
    sc.subscribe(['test-topic'])
    time.sleep(0.2)
    assert not records, "no records should surface before any drain"

    sc.close()
    assert any('INIT' in r.getMessage() for r in records), "close() should drain the log queue"


def test_statistics_interval_ms_zero_explicit_rejected():
    """statistics.interval.ms is rejected even when set to 0.

    The check looks at whether the key is present, not its value, so the
    usual "0 to disable" trick is rejected too (int and str alike).
    """
    config = {
        'group.id': unique_id('test-share-stats-interval-zero'),
        'bootstrap.servers': 'localhost:9092',
    }
    for value in (0, '0'):
        with pytest.raises(ValueError, match='statistics.interval.ms is not supported'):
            ShareConsumer({**config, 'statistics.interval.ms': value})


def test_stats_cb_rejection_precedes_group_id_check():
    """stats_cb is rejected before group.id is even checked.

    The incompatible-config check runs first, so leaving out group.id still
    gives the stats_cb error, not a missing-group.id one.
    """

    def my_stats_cb(stats_json_str):
        pass

    with pytest.raises(ValueError, match='stats_cb is not supported'):
        ShareConsumer({'bootstrap.servers': 'localhost:9092', 'stats_cb': my_stats_cb})


def test_log_cb_delivers_always_on_warning_without_debug():
    """Always-on librdkafka logs reach log_cb even with no debug context set.

    test_log_cb relies on debug=msg INIT records; here no 'debug' is
    configured. A socket.timeout.ms below fetch.wait.max.ms (500) by >1s
    triggers librdkafka's always-on CONFWARN at WARNING level, proving the
    forwarded log queue delivers non-debug records too. (The facility is the
    leading token of the record message.) The record drains on the first
    poll().
    """
    logger, records = _collecting_logger('test-share-log-always-on')
    sc = ShareConsumer(
        {
            'group.id': unique_id('test-share-log-always-on'),
            'bootstrap.servers': 'localhost:19999',
            'socket.timeout.ms': 100,  # < fetch.wait.max.ms (500) -> always-on CONFWARN
            'logger': logger,
        }
    )
    sc.subscribe(['test-topic'])
    sc.poll(timeout=0.2)
    sc.close()

    warnings = [r for r in records if r.levelno >= logging.WARNING]
    assert warnings, f"expected an always-on WARNING without debug; got {[r.getMessage() for r in records]}"
    assert any('fetch.wait.max.ms' in r.getMessage() for r in warnings), "expected the CONFWARN about fetch.wait.max.ms"
