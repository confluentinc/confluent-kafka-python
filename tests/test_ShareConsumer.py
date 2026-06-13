#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Unit tests for ShareConsumer class.
"""

import threading
import time

import pytest

from confluent_kafka import AcknowledgeType, KafkaError, KafkaException, ShareConsumer
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


def test_constructor_rejects_on_commit():
    """Share consumers have no offset-commit concept. Setting on_commit
    in the positional config dict OR as a kwarg must be rejected at
    construction time so the misconfiguration is visible to callers
    instead of being silently held by librdkafka.

    Wired via ShareConsumer_init's pre-filter pass over args[0] + kwargs
    (ShareConsumer.c), which scans for share-incompatible keys before
    handing off to common_conf_setup. Same mechanism as stats_cb /
    statistics.interval.ms rejection in
    test_ShareConsumer_callbacks.py::test_stats_cb_rejected.
    """
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


def test_commit_does_not_hang_on_unreachable_broker():
    """Commit on a fresh, unsubscribed consumer pointed at an unreachable
    broker returns immediately (no acks pending). The interesting case
    — pending acks against an unreachable broker — needs wire-frame
    mocking to exercise."""
    sc = ShareConsumer(
        {
            'bootstrap.servers': '127.0.0.1:1',
            'group.id': unique_id('test-share-commit-unreachable'),
            'share.acknowledgement.mode': 'implicit',
            'socket.timeout.ms': 1000,
        }
    )
    try:
        # Don't subscribe — librdkafka has crashed in the past when commit
        # is called on a subscribed-but-never-connected consumer.
        start = time.monotonic()
        result = sc.commit_sync(timeout=2.0)
        elapsed = time.monotonic() - start

        assert result == {}
        assert elapsed < 5.0, f'commit hung for {elapsed:.2f}s'
    finally:
        sc.close()


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

    with pytest.raises(RuntimeError) as ex:
        sc.commit_sync(timeout=0.1)
    assert ex.match('Share consumer closed')

    with pytest.raises(RuntimeError) as ex:
        sc.commit_async()
    assert ex.match('Share consumer closed')

    with pytest.raises(RuntimeError) as ex:
        sc.set_sasl_credentials('user', 'pass')
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


def test_subscribe_with_empty_list_unsubscribes(share_consumer):
    """subscribe([]) is equivalent to unsubscribe(): an empty topic list
    clears the current subscription instead of raising."""
    share_consumer.subscribe(['test-topic'])
    assert share_consumer.subscription() == ['test-topic']

    share_consumer.subscribe([])  # no exception
    assert share_consumer.subscription() == []


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


def test_acknowledge_rejects_non_message_argument(share_consumer):
    """acknowledge() must reject non-Message arguments."""
    for bad in (None, 'not-a-message', 42, object(), []):
        with pytest.raises(TypeError):
            share_consumer.acknowledge(bad, AcknowledgeType.ACCEPT)


def test_set_sasl_credentials_accepts_strings(share_consumer):
    """Setting credentials doesn't touch the network, so it works on an
    unconnected consumer and just returns None."""
    assert share_consumer.set_sasl_credentials('user', 'secret') is None
    # keyword form
    assert share_consumer.set_sasl_credentials(username='user2', password='s2') is None


def test_set_sasl_credentials_rejects_bad_arguments(share_consumer):
    """set_sasl_credentials() requires exactly two string arguments."""
    with pytest.raises(TypeError):
        share_consumer.set_sasl_credentials()  # missing both
    with pytest.raises(TypeError):
        share_consumer.set_sasl_credentials('user')  # missing password
    with pytest.raises(TypeError):
        share_consumer.set_sasl_credentials(123, 'pw')  # non-str username
    with pytest.raises(TypeError):
        share_consumer.set_sasl_credentials('user', None)  # non-str password


def test_acknowledge_offset_rejects_non_str_topic(share_consumer):
    """acknowledge_offset() must reject non-str topic."""
    for bad in (None, 42, object(), []):
        with pytest.raises(TypeError):
            share_consumer.acknowledge_offset(bad, 0, 0, AcknowledgeType.ACCEPT)


def test_acknowledge_offset_rejects_non_int_partition(share_consumer):
    """acknowledge_offset() must reject non-int partition."""
    for bad in ('str', None, object(), [], 1.5):
        with pytest.raises(TypeError):
            share_consumer.acknowledge_offset('topic', bad, 0, AcknowledgeType.ACCEPT)


def test_acknowledge_offset_rejects_non_int_offset(share_consumer):
    """acknowledge_offset() must reject non-int offset."""
    for bad in ('str', None, object(), [], 1.5):
        with pytest.raises(TypeError):
            share_consumer.acknowledge_offset('topic', 0, bad, AcknowledgeType.ACCEPT)


def test_acknowledge_offset_rejects_negative_partition(share_consumer):
    """librdkafka rejects negative partition with _INVALID_ARG."""
    with pytest.raises(KafkaException) as ex:
        share_consumer.acknowledge_offset('topic', -1, 0, AcknowledgeType.ACCEPT)
    assert ex.value.args[0].code() == KafkaError._INVALID_ARG


def test_acknowledge_offset_rejects_negative_offset(share_consumer):
    """librdkafka rejects negative offset with _INVALID_ARG."""
    with pytest.raises(KafkaException) as ex:
        share_consumer.acknowledge_offset('topic', 0, -1, AcknowledgeType.ACCEPT)
    assert ex.value.args[0].code() == KafkaError._INVALID_ARG


def test_commit_sync_rejects_non_numeric_timeout(share_consumer):
    """commit_sync(timeout=...) must reject non-numeric values."""
    for bad in ('str', None, object(), []):
        with pytest.raises(TypeError):
            share_consumer.commit_sync(timeout=bad)


def test_commit_sync_rejects_unknown_kwargs(share_consumer):
    """commit_sync() must reject unknown keyword arguments."""
    with pytest.raises(TypeError):
        share_consumer.commit_sync(unknown_kwarg=1.0)


def test_commit_async_rejects_any_argument(share_consumer):
    """commit_async() takes no arguments."""
    with pytest.raises(TypeError):
        share_consumer.commit_async(1.0)
    with pytest.raises(TypeError):
        share_consumer.commit_async(timeout=1.0)


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
