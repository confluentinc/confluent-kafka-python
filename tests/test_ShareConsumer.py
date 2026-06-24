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

"""
Unit tests for ShareConsumer class.
"""

import gc
import sys
import threading
import time

import pytest

from confluent_kafka import (
    AcknowledgeType,
    ConcurrentModificationException,
    IllegalStateException,
    KafkaError,
    KafkaException,
    Message,
    Messages,
    ShareConsumer,
)
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
    """Share consumers acknowledge records instead of committing offsets,
    so on_commit has nothing to fire on. Setting it in the positional
    config dict OR as a kwarg must be rejected at construction time so the
    misconfiguration is visible to callers instead of being silently held
    as a callback that never runs.

    Wired via ShareConsumer_init's pre-filter pass over args[0] + kwargs
    (ShareConsumer.c), which scans for inapplicable keys before handing
    off to common_conf_setup.
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


def test_subscribe_multiple_topics(share_consumer):
    """subscribe() with several topics: subscription() reports all of them.

    The result comes back sorted rather than in insertion order, so compare
    order-agnostically — what matters is that every topic survives the round
    trip, not the ordering."""
    share_consumer.subscribe(['topic-c', 'topic-a', 'topic-b'])
    assert sorted(share_consumer.subscription()) == ['topic-a', 'topic-b', 'topic-c']


def test_subscribe_idempotent_and_incremental(share_consumer):
    """Re-subscribing to the same set is idempotent, growing the topic list
    grows the subscription, and a repeated unsubscribe() is a harmless no-op.
    """
    # Incremental: each subscribe() fully replaces the prior set, so a growing
    # list yields a growing subscription.
    share_consumer.subscribe(['a'])
    assert share_consumer.subscription() == ['a']
    share_consumer.subscribe(['a', 'b'])
    assert sorted(share_consumer.subscription()) == ['a', 'b']
    share_consumer.subscribe(['a', 'b', 'c'])
    assert sorted(share_consumer.subscription()) == ['a', 'b', 'c']

    # Idempotent: subscribing to the same set repeatedly doesn't duplicate it.
    share_consumer.subscribe(['x', 'y'])
    share_consumer.subscribe(['x', 'y'])
    share_consumer.subscribe(['x', 'y'])
    assert sorted(share_consumer.subscription()) == ['x', 'y']

    # Repeated unsubscribe is a no-op, not an error.
    share_consumer.unsubscribe()
    share_consumer.unsubscribe()
    assert share_consumer.subscription() == []


def test_unsubscribe(share_consumer):
    """Test unsubscribe() method."""
    share_consumer.subscribe(['test-topic'])
    share_consumer.unsubscribe()

    subscription = share_consumer.subscription()
    assert len(subscription) == 0


def test_unsubscribe_without_subscription_is_noop(share_consumer):
    """unsubscribe() before any subscribe() is a no-op: it returns None and
    leaves the subscription empty rather than raising."""
    assert share_consumer.subscription() == []
    assert share_consumer.unsubscribe() is None
    assert share_consumer.subscription() == []


def test_poll_no_broker(share_consumer):
    """Test poll() returns an empty batch when no broker available."""
    share_consumer.subscribe(['test-topic'])

    messages = share_consumer.poll(timeout=0.1)
    assert messages.is_empty()


def test_poll_returns_messages(share_consumer):
    """poll() returns a Messages, not a bare list."""
    share_consumer.subscribe(['test-topic'])

    out = share_consumer.poll(timeout=0.1)
    assert isinstance(out, Messages)
    assert out.is_empty()
    assert out.count() == 0


def test_poll_without_subscription_raises_state(share_consumer):
    """poll() before any subscribe() raises IllegalStateException(_STATE).

    The "not subscribed" check fires before any broker I/O, so this returns
    immediately without a broker. We pin the exception type but not the
    message text: depending on timing it can be either "not subscribed" or
    "consumer group not initialized", both of which are _STATE."""
    with pytest.raises(IllegalStateException) as ex:
        share_consumer.poll(timeout=0.1)
    assert str(ex.value)


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
    """All operations on a closed consumer raise IllegalStateException — a
    RuntimeError subclass whose message (str(exc)) is the _STATE error."""
    sc = ShareConsumer(
        {
            'group.id': unique_id('test-share-after-close'),
            'bootstrap.servers': 'localhost:9092',
            'socket.timeout.ms': 100,
        }
    )

    sc.subscribe(['test-topic'])
    sc.close()

    with pytest.raises(IllegalStateException) as ex:
        sc.subscribe(['test'])
    assert str(ex.value)
    assert ex.match('Share consumer closed')

    with pytest.raises(IllegalStateException) as ex:
        sc.unsubscribe()
    assert str(ex.value)
    assert ex.match('Share consumer closed')

    with pytest.raises(IllegalStateException) as ex:
        sc.subscription()
    assert str(ex.value)
    assert ex.match('Share consumer closed')

    with pytest.raises(IllegalStateException) as ex:
        sc.poll(timeout=0.1)
    assert str(ex.value)
    assert ex.match('Share consumer closed')

    # The closed-state check happens before argument parsing, so acknowledge(None)
    # raises the closed-consumer IllegalStateException rather than a TypeError about
    # the non-Message argument.
    with pytest.raises(IllegalStateException) as ex:
        sc.acknowledge(None, AcknowledgeType.ACCEPT)
    assert str(ex.value)
    assert ex.match('Share consumer closed')

    with pytest.raises(IllegalStateException) as ex:
        sc.acknowledge_offset('test-topic', 0, 0, AcknowledgeType.ACCEPT)
    assert str(ex.value)
    assert ex.match('Share consumer closed')

    with pytest.raises(IllegalStateException) as ex:
        sc.commit_sync(timeout=0.1)
    assert str(ex.value)
    assert ex.match('Share consumer closed')

    with pytest.raises(IllegalStateException) as ex:
        sc.commit_async()
    assert str(ex.value)
    assert ex.match('Share consumer closed')

    with pytest.raises(IllegalStateException) as ex:
        sc.set_sasl_credentials('user', 'pass')
    assert str(ex.value)
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
    """subscribe([]) is equivalent to unsubscribe(): an empty topic list clears
    the current subscription instead of raising, after which poll() raises
    _STATE (not subscribed).

    This is an empty *list* — an empty topic *name* is a different case and
    still raises _INVALID_ARG (test_subscribe_rejects_empty_and_duplicate_topic_names).
    """
    share_consumer.subscribe(['test-topic'])
    assert share_consumer.subscription() == ['test-topic']

    assert share_consumer.subscribe([]) is None
    assert share_consumer.subscription() == []

    with pytest.raises(IllegalStateException) as ex:
        share_consumer.poll(timeout=0.1)
    assert str(ex.value)


def test_subscribe_rejects_empty_and_duplicate_topic_names(share_consumer):
    """An empty topic name and duplicate topic names are rejected with
    _INVALID_ARG. (An empty *list* is a different case — it unsubscribes.)"""
    with pytest.raises(ValueError) as ex:
        share_consumer.subscribe([''])
    assert str(ex.value)

    with pytest.raises(ValueError) as ex:
        share_consumer.subscribe(['dup-topic', 'dup-topic'])
    assert str(ex.value)


def test_subscribe_accepts_caret_topic_as_literal_name(share_consumer):
    """A '^'-prefixed name is accepted and stored verbatim — it's treated as a
    literal topic name, not a regex pattern. Whether it matches any topic is a
    broker-side question; here we just confirm it's accepted and round-trips."""
    share_consumer.subscribe(['^literal-name'])
    assert share_consumer.subscription() == ['^literal-name']


def test_poll_with_non_numeric_timeout_raises(share_consumer):
    """poll(timeout=...) must reject non-numeric values."""
    share_consumer.subscribe(['test-topic'])
    with pytest.raises(TypeError):
        share_consumer.poll(timeout='bad')
    with pytest.raises(TypeError):
        share_consumer.poll(timeout=None)


def test_poll_returns_list_type(share_consumer):
    """poll() hands back a list (a batch of messages), never None or a bare
    Message. With no broker reachable the batch is just empty."""
    share_consumer.subscribe(['test-topic'])
    result = share_consumer.poll(timeout=0.1)
    assert isinstance(result, list)
    assert result == []


def test_poll_accepts_int_timeout(share_consumer):
    """poll() takes an int timeout, not just a float; it's coerced the same way.
    The rejection side is covered by test_poll_with_non_numeric_timeout_raises."""
    share_consumer.subscribe(['test-topic'])
    result = share_consumer.poll(timeout=1)
    assert result == []


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


def test_acknowledge_none_topic_message_rejected(share_consumer):
    """acknowledge() of a Message with no topic (topic() is None) is rejected
    with _INVALID_ARG rather than crashing on the missing topic.

    partition/offset are valid, so the absent topic is the only thing wrong.
    A missing topic is checked before the ack-mode check, which is why the
    default implicit-mode fixture works here — the same call with a real topic
    would instead return _STATE (an explicit ack in implicit mode)."""
    msg = Message(partition=0, offset=0)
    assert msg.topic() is None
    with pytest.raises(ValueError) as ex:
        share_consumer.acknowledge(msg, AcknowledgeType.ACCEPT)
    assert str(ex.value)


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
    with pytest.raises(ValueError) as ex:
        share_consumer.acknowledge_offset('topic', -1, 0, AcknowledgeType.ACCEPT)
    assert str(ex.value)


def test_acknowledge_offset_rejects_negative_offset(share_consumer):
    """librdkafka rejects negative offset with _INVALID_ARG."""
    with pytest.raises(ValueError) as ex:
        share_consumer.acknowledge_offset('topic', 0, -1, AcknowledgeType.ACCEPT)
    assert str(ex.value)


def test_acknowledge_offset_rejects_out_of_range_ack_type():
    """An out-of-range AcknowledgeType is rejected with _INVALID_ARG.

    Only ACCEPT(1), RELEASE(2) and REJECT(3) are valid. The ack_type is only
    checked after the ack-mode check, and an explicit ack in implicit mode
    returns _STATE first — so this needs an explicit-mode consumer (not the
    shared implicit fixture) to actually reach the type check. topic/partition/
    offset are valid, so the bad ack_type is the only thing wrong. acknowledge()
    takes the same path, so this covers both ack APIs."""
    sc = ShareConsumer(
        {
            'group.id': unique_id('test-share-bad-ack-type'),
            'bootstrap.servers': 'localhost:9092',
            'socket.timeout.ms': 100,
            'share.acknowledgement.mode': 'explicit',
        }
    )
    try:
        # 0 sits just below ACCEPT(1), 4 just above REJECT(3); 999 is far out.
        for bad_ack_type in (0, 4, 999):
            with pytest.raises(ValueError) as ex:
                sc.acknowledge_offset('test-topic', 0, 0, bad_ack_type)
            assert str(ex.value), f'ack_type={bad_ack_type} should be rejected with _INVALID_ARG'
    finally:
        sc.close()


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


def test_concurrent_thread_access_raises_conflict():
    """A ShareConsumer is not safe for concurrent use: touching it from a
    second thread while another thread is inside poll() raises
    ConcurrentModificationException(_CONFLICT).

    Ownership is held by whichever thread is currently in a call, for the whole
    duration of that call (including poll()'s blocking wait), so a second
    thread's call is rejected. No broker needed — the guard is local and stays
    held across the idle poll, so the hammer thread reliably hits it.
    commit_async() makes a good probe: it's guarded but returns immediately.
    """
    sc = TestShareConsumer(
        {
            'group.id': unique_id('test-share-conflict'),
            'socket.timeout.ms': 100,
        }
    )
    sc.subscribe(['test-topic'])

    conflicts = []
    other_errors = []
    stop = threading.Event()

    def hammer():
        while not stop.is_set():
            try:
                sc.commit_async()
            except ConcurrentModificationException as exc:
                # The exception type alone signals _CONFLICT - no KafkaError code to inspect.
                conflicts.append(exc)
            except Exception as exc:  # noqa: BLE001 - record anything unexpected
                other_errors.append(repr(exc))

    hammer_thread = threading.Thread(target=hammer, daemon=True)
    hammer_thread.start()
    try:
        # Keep the consumer busy inside poll() until the hammer thread sees a
        # conflict, or give up after a few seconds. The main thread can lose the
        # race too if the hammer briefly grabs ownership — that's fine, swallow
        # it and keep polling.
        deadline = time.monotonic() + 3.0
        while not conflicts and time.monotonic() < deadline:
            try:
                sc.poll(timeout=0.2)
            except ConcurrentModificationException:
                pass
    finally:
        stop.set()
        hammer_thread.join(timeout=2.0)
        sc.close()

    assert conflicts, "second-thread access during poll() should have raised _CONFLICT"
    assert all(isinstance(exc, ConcurrentModificationException) for exc in conflicts)
    assert not other_errors, f"unexpected errors from second thread: {[str(e) for e in other_errors]}"


def test_dealloc_without_close_destroys_handle():
    """Dropping a ShareConsumer without close() must let dealloc destroy the
    handle cleanly.

    close() destroys the handle and NULLs it, so a consumer that gets closed
    leaves dealloc nothing to do. Not closing is the only way to reach
    dealloc's destroy path. A regression there (use-after-free / double-free)
    crashes the interpreter; a milder error surfaces as an unraisable exception
    from the destructor, which we capture and assert against.
    """
    sc = ShareConsumer(
        {
            'group.id': unique_id('test-share-dealloc-no-close'),
            'bootstrap.servers': 'localhost:9092',
        }
    )
    sc.subscribe(['test-topic'])

    unraisables = []
    prev_hook = sys.unraisablehook
    sys.unraisablehook = lambda args: unraisables.append(args)
    try:
        # Drop the last reference: refcounting runs dealloc right here. The
        # collect() is a safety net in case a callback ever forms a cycle.
        del sc
        gc.collect()
    finally:
        sys.unraisablehook = prev_hook

    assert unraisables == [], f"dealloc raised: {[u.exc_value for u in unraisables]}"


def test_share_exceptions_exported():
    """Both new types are defined in cimpl, re-exported from the package, and
    listed in __all__."""
    import confluent_kafka
    from confluent_kafka import cimpl

    assert confluent_kafka.IllegalStateException is cimpl.IllegalStateException is IllegalStateException
    assert (
        confluent_kafka.ConcurrentModificationException
        is cimpl.ConcurrentModificationException
        is ConcurrentModificationException
    )
    assert 'IllegalStateException' in confluent_kafka.__all__
    assert 'ConcurrentModificationException' in confluent_kafka.__all__


@pytest.mark.parametrize('exc_type', [IllegalStateException, ConcurrentModificationException])
def test_share_exception_subclass_contract(exc_type):
    """Subclass RuntimeError (so pre-KIP-932 `except RuntimeError` still
    catches them) but deliberately NOT KafkaException (so `except
    KafkaException` won't silently swallow them)."""
    assert issubclass(exc_type, RuntimeError)
    assert not issubclass(exc_type, KafkaException)


@pytest.mark.parametrize(
    'trigger, exc_type',
    [
        (lambda sc: sc.poll(timeout=0.1), IllegalStateException),
        (lambda sc: sc.subscribe(['']), ValueError),
    ],
    ids=['_STATE->IllegalStateException', '_INVALID_ARG->ValueError'],
)
def test_error_code_maps_to_python_exception(share_consumer, trigger, exc_type):
    """Each reachable error code surfaces as its mapped Python type. The type
    conveys the code; args[0] is the plain message string (not a KafkaError),
    and librdkafka's detail is preserved in it.

    _CONFLICT -> ConcurrentModificationException needs two threads, so it has
    its own test (test_concurrent_thread_access_raises_conflict)."""
    with pytest.raises(exc_type) as ex:
        trigger(share_consumer)
    assert not isinstance(ex.value.args[0], KafkaError)
    assert str(ex.value)  # original message preserved, not blanked


def test_state_error_is_catchable_as_runtimeerror(share_consumer):
    """Compat bridge: because IllegalStateException is a RuntimeError, code
    written against the pre-KIP-932 closed-consumer RuntimeError keeps working
    without having to catch KafkaException."""
    with pytest.raises(RuntimeError) as ex:
        share_consumer.poll(timeout=0.1)
    assert isinstance(ex.value, IllegalStateException)
    assert str(ex.value)
