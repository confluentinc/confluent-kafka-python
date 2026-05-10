#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Unit tests for ShareConsumer class.
"""

import threading

import pytest

from confluent_kafka import KafkaError, KafkaException, ShareConsumer
from tests.common import (
    TestShareConsumer,
    TestUtils,
    unique_id,
)


@pytest.fixture
def share_consumer():
    """Default-configured ShareConsumer with teardown.

    Each test gets a unique group.id so librdkafka's per-group internal state
    can't leak from one test into the next.
    """
    sc = TestShareConsumer(
        {
            'group.id': unique_id('test-share-group'),
            'socket.timeout.ms': 100,
        }
    )
    yield sc
    sc.close()


def test_constructor_requires_config():
    """ShareConsumer constructor requires a configuration dict."""
    with pytest.raises(TypeError) as ex:
        ShareConsumer()
    assert ex.match('expected configuration dict')


def test_constructor_with_valid_config(share_consumer):
    """ShareConsumer can be created with valid configuration."""
    assert share_consumer is not None


def test_constructor_dict_with_kwargs():
    """ShareConsumer accepts a positional config dict + keyword arguments
    (cimpl.pyi overload form 2).

    The positional dict carries Kafka config; the kwargs carry runtime extras
    like `logger`. If the C extension rejected this form, mypy would bless
    user code that crashed at runtime.
    """
    import logging

    sc = ShareConsumer(
        {
            'group.id': unique_id('test-share-form2'),
            'bootstrap.servers': 'localhost:9092',
            'socket.timeout.ms': 100,
        },
        logger=logging.getLogger('test-share-form2'),
    )
    sc.close()


def test_constructor_kwargs_only():
    """ShareConsumer accepts configuration entirely via keyword arguments
    (cimpl.pyi overload form 3), spread from a dict at the call site.
    """
    config = {
        'group.id': unique_id('test-share-form3'),
        'bootstrap.servers': 'localhost:9092',
        'socket.timeout.ms': 100,
    }
    sc = ShareConsumer(**config)
    sc.close()


# TODO KIP-932: re-enable once ShareConsumer rejects on_commit at config time.
# Today consumer_conf_set_special accepts on_commit for any consumer type and
# ShareConsumer_clear0 has to compensate with a DECREF dance. The test pins the
# desired contract; flip @pytest.mark.skip off when the rejection is wired in.
@pytest.mark.skip(reason="TODO KIP-932: on_commit rejection not implemented yet")
def test_constructor_rejects_on_commit():
    """Share consumers have no offset-commit concept. Setting on_commit
    in the positional config dict OR as a kwarg must be rejected at
    construction time so the misconfiguration is visible to callers
    instead of being silently held by librdkafka."""
    config = {
        'group.id': unique_id('test-share-no-commit'),
        'bootstrap.servers': 'localhost:9092',
    }
    cb = lambda *a, **kw: None  # noqa: E731

    with pytest.raises(ValueError, match='on_commit is not supported'):
        ShareConsumer({**config, 'on_commit': cb})

    with pytest.raises(ValueError, match='on_commit is not supported'):
        ShareConsumer(config, on_commit=cb)


def test_subscription_on_fresh_consumer(share_consumer):
    """A consumer that has never called subscribe() reports an empty
    subscription. Locks down the no-subscription representation so a future
    librdkafka change (e.g. None instead of []) is caught immediately."""
    assert share_consumer.subscription() == []


def test_subscribe_replaces_previous(share_consumer):
    """subscribe() replaces — does NOT extend — the previous subscription.
    Locks down the documented merge-vs-replace contract."""
    share_consumer.subscribe(['topic-a'])
    share_consumer.subscribe(['topic-b'])
    assert share_consumer.subscription() == ['topic-b']


def test_subscribe(share_consumer):
    """Test subscribe() method."""
    share_consumer.subscribe(['test-topic'])

    subscription = share_consumer.subscription()
    assert subscription is not None
    assert 'test-topic' in subscription


def test_unsubscribe(share_consumer):
    """Test unsubscribe() method."""
    share_consumer.subscribe(['test-topic'])
    share_consumer.unsubscribe()

    subscription = share_consumer.subscription()
    assert len(subscription) == 0


def test_poll_no_broker(share_consumer):
    """Test poll() returns empty list when no broker available."""
    share_consumer.subscribe(['test-topic'])

    messages = share_consumer.poll(timeout=0.1)
    assert messages == []


def test_context_manager():
    """Test that ShareConsumer works as a context manager and closes on exit."""
    with ShareConsumer(
        {
            'group.id': unique_id('test-share-ctx'),
            'bootstrap.servers': 'localhost:9092',
            'socket.timeout.ms': 100,
        }
    ) as sc:
        assert sc is not None
        sc.subscribe(['test-topic'])
        subscription = sc.subscription()
        assert 'test-topic' in subscription

    # After exiting the context manager, the consumer should be closed
    with pytest.raises(RuntimeError) as ex:
        sc.subscribe(['test-topic'])
    assert ex.match('Share consumer closed')


def test_close_idempotent():
    """Test that close() can be called multiple times."""
    sc = ShareConsumer(
        {
            'group.id': unique_id('test-share-close-idem'),
            'bootstrap.servers': 'localhost:9092',
            'socket.timeout.ms': 100,
        }
    )

    sc.close()
    # TODO: a second close() on an already-closed share consumer should
    # raise a "Share consumer closed" RuntimeError (consistent with the
    # post-close behavior of the other ShareConsumer methods). Today it
    # silently no-ops, matching Consumer.close(); flip the assertion when
    # the underlying behavior is changed.
    sc.close()


def test_any_method_after_close_throws_exception():
    """Test that all operations on a closed consumer raise RuntimeError."""
    sc = ShareConsumer(
        {
            'group.id': unique_id('test-share-after-close'),
            'bootstrap.servers': 'localhost:9092',
            'socket.timeout.ms': 100,
        }
    )

    sc.subscribe(['test-topic'])
    sc.close()

    with pytest.raises(RuntimeError) as ex:
        sc.subscribe(['test'])
    assert ex.match('Share consumer closed')

    with pytest.raises(RuntimeError) as ex:
        sc.unsubscribe()
    assert ex.match('Share consumer closed')

    with pytest.raises(RuntimeError) as ex:
        sc.subscription()
    assert ex.match('Share consumer closed')

    with pytest.raises(RuntimeError) as ex:
        sc.poll(timeout=0.1)
    assert ex.match('Share consumer closed')


def test_required_group_id():
    """Test that group.id is required."""
    with pytest.raises(ValueError) as ex:
        ShareConsumer(
            {
                'bootstrap.servers': 'localhost:9092',
            }
        )
    assert ex.match('group.id must be set')


def test_subscribe_with_non_list_raises(share_consumer):
    """subscribe() must reject non-list arguments."""
    with pytest.raises(TypeError, match='expected list'):
        share_consumer.subscribe('not_a_list')
    with pytest.raises(TypeError, match='expected list'):
        share_consumer.subscribe(None)


def test_subscribe_with_empty_list_raises(share_consumer):
    """librdkafka rejects an empty subscription with _INVALID_ARG."""
    with pytest.raises(KafkaException) as exc_info:
        share_consumer.subscribe([])
    assert exc_info.value.args[0].code() == KafkaError._INVALID_ARG


def test_poll_with_non_numeric_timeout_raises(share_consumer):
    """poll(timeout=...) must reject non-numeric values."""
    share_consumer.subscribe(['test-topic'])
    with pytest.raises(TypeError):
        share_consumer.poll(timeout='bad')
    with pytest.raises(TypeError):
        share_consumer.poll(timeout=None)


# TODO: subscribe([123, 456]) and subscribe([None]) currently silently
# coerce non-string items to topic names via PyObject_Str (str(123) -> "123",
# str(None) -> "None"). This is inherited from Consumer's cfl_PyObject_Unistr
# helper. Strict isinstance(item, str) checking would catch buggy callers but
# is a backward-incompatible change. Add a negative test for these once the
# behavior is tightened.


def test_poll_interruptible_by_signal():
    """ShareConsumer.poll uses chunked polling so SIGINT surfaces as
    KeyboardInterrupt instead of being swallowed until the librdkafka timeout
    expires. Verifies the chunked loop in ShareConsumer.c::ShareConsumer_poll
    actually checks for signals between chunks. Mirrors the pattern in
    test_Wakeable.py for the regular Consumer.
    """
    sc1 = TestShareConsumer(
        {
            'group.id': unique_id('test-poll-signal-finite'),
            'socket.timeout.ms': 100,
        }
    )
    sc1.subscribe(['test-topic'])

    interrupt_thread = threading.Thread(target=lambda: TestUtils.send_sigint_after_delay(0.4))
    interrupt_thread.daemon = True
    interrupt_thread.start()

    interrupted = False
    try:
        sc1.poll(timeout=5.0)  # 5s budget — interrupt should fire well before
    except KeyboardInterrupt:
        interrupted = True
    finally:
        sc1.close()

    assert interrupted, "poll(timeout=5.0) should have been interrupted by SIGINT"

    sc2 = TestShareConsumer(
        {
            'group.id': unique_id('test-poll-signal-infinite'),
            'socket.timeout.ms': 100,
        }
    )
    sc2.subscribe(['test-topic'])

    interrupt_thread = threading.Thread(target=lambda: TestUtils.send_sigint_after_delay(0.4))
    interrupt_thread.daemon = True
    interrupt_thread.start()

    interrupted = False
    try:
        sc2.poll()  # infinite timeout
    except KeyboardInterrupt:
        interrupted = True
    finally:
        sc2.close()

    assert interrupted, "poll() (infinite) should have been interrupted by SIGINT"


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


# TODO KIP-932: this test pins a coin flip — whether close() surfaces a
# user-callback exception depends on whether librdkafka happens to dispatch
# an error event during the close drain, which depends on internal queue
# timing and rate-limiter state. Locally it tends not to raise; on CI it
# does. Replace with a test of the disarm-before-close recipe (set the
# callback's raise flag to False, then close — guaranteed clean) once the
# share-consumer error-handling docs settle.
# Validate & Handle this in upcoming callback PRs
@pytest.mark.skip(reason="TODO KIP-932: timing-dependent; replace with disarm-recipe test")
def test_error_cb_exception_during_close():
    """Pin down close() behavior when error_cb is still rigged to raise.

    During close, librdkafka may dispatch one final round of error events
    (e.g. _ALL_BROKERS_DOWN). If a user callback raises in that path, the
    exception surfaces from close() — close does not silently swallow it.

    Callers should treat close() as fallible and put their own raise paths
    behind a guard if they need close() to be infallible.
    """

    def error_cb_that_raises(error):
        raise RuntimeError("error_cb raises during close")

    sc = ShareConsumer(
        {
            'group.id': unique_id('test-share-close-cb-raise'),
            'bootstrap.servers': 'localhost:19999',
            'socket.timeout.ms': 100,
            'error_cb': error_cb_that_raises,
        }
    )
    sc.subscribe(['test-topic'])

    # Drain the initial poll() exception so the consumer reaches a steady
    # broker-unreachable state before close.
    with pytest.raises(RuntimeError):
        sc.poll(timeout=0.5)

    # Whatever close() does, it MUST be deterministic. Capture and assert.
    close_raised = None
    try:
        sc.close()
    except RuntimeError as exc:
        close_raised = exc

    # Document the current contract: close() does NOT pump user-facing
    # callbacks that would re-raise. If this assertion ever flips, the
    # contract has changed and the docstring above needs updating.
    assert close_raised is None, (
        f"close() raised: {close_raised!r}; if intentional, update the "
        f"test docstring and treat close() as fallible in user code."
    )


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
