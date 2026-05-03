#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Unit tests for ShareConsumer class.
"""

import threading
import time

import pytest

from confluent_kafka import KafkaError, KafkaException, Producer, ShareConsumer
from tests.common import (
    DEFAULT_BOOTSTRAP_SERVERS,
    TestShareConsumer,
    TestUtils,
    drain_share_consumers,
    unique_id,
    warmup_share_consumers,
)


@pytest.fixture
def share_consumer():
    """Default-configured ShareConsumer with teardown."""
    sc = TestShareConsumer(
        {
            'group.id': 'test-share-group',
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
            'group.id': 'test-share-group',
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
            'group.id': 'test-share-group',
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
            'group.id': 'test-share-group',
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


@pytest.mark.integration
def test_concurrent_consumers():
    """Two consumers in the same share group must receive disjoint records."""
    bootstrap = 'localhost:9092'
    run_id = int(time.time() * 1000)
    topic = f'test-share-concurrent-{run_id}'
    group_id = f'test-share-group-{run_id}'
    n_messages = 30

    producer = Producer({'bootstrap.servers': bootstrap})
    producer.produce(topic, value=b'priming')
    producer.flush(timeout=10.0)

    consumer_config = {
        'group.id': group_id,
        'bootstrap.servers': bootstrap,
    }
    sc1 = ShareConsumer(consumer_config)
    sc2 = ShareConsumer(consumer_config)

    try:
        sc1.subscribe([topic])
        sc2.subscribe([topic])

        # Drive heartbeats so both consumers register with the share
        # coordinator before any test messages are produced.
        warmup_deadline = time.time() + 8.0
        while time.time() < warmup_deadline:
            sc1.poll(timeout=0.5)
            sc2.poll(timeout=0.5)

        for i in range(n_messages):
            producer.produce(topic, value=f'msg-{i}'.encode())
        producer.flush(timeout=10.0)

        offsets1 = set()
        offsets2 = set()
        deadline = time.time() + 20.0
        while time.time() < deadline:
            for sc, offsets in ((sc1, offsets1), (sc2, offsets2)):
                for msg in sc.poll(timeout=0.5):
                    if msg.error() is None:
                        offsets.add((msg.topic(), msg.partition(), msg.offset()))
            if len(offsets1) + len(offsets2) >= n_messages:
                break

        all_offsets = offsets1 | offsets2
        overlap = offsets1 & offsets2

        assert overlap == set(), f"Same record delivered to both consumers: {overlap}"
        assert len(all_offsets) == n_messages, (
            f"Expected {n_messages} unique records across both consumers, "
            f"got {len(all_offsets)} (sc1={len(offsets1)}, sc2={len(offsets2)})"
        )

    finally:
        sc1.close()
        sc2.close()


@pytest.mark.integration
def test_basic_consume_records():
    """Single share consumer reads all produced records with correct values."""
    topic = unique_id('test-share-basic')
    group_id = unique_id('test-share-basic')
    n = 10

    producer = Producer({'bootstrap.servers': DEFAULT_BOOTSTRAP_SERVERS})
    producer.produce(topic, value=b'priming')
    producer.flush(timeout=10.0)

    sc = TestShareConsumer({'group.id': group_id})
    try:
        sc.subscribe([topic])
        warmup_share_consumers([sc])

        expected = [f'msg-{i}'.encode() for i in range(n)]
        for v in expected:
            producer.produce(topic, value=v)
        producer.flush(timeout=10.0)

        received = drain_share_consumers([sc], n)[0]
        values = sorted(m.value() for m in received)
        assert values == sorted(expected), f"Value mismatch: expected {sorted(expected)}, got {values}"
    finally:
        sc.close()


@pytest.mark.integration
def test_message_fields_preserved():
    """Key, value, and headers round-trip intact through ShareConsumer."""
    topic = unique_id('test-share-fields')
    group_id = unique_id('test-share-fields')

    producer = Producer({'bootstrap.servers': DEFAULT_BOOTSTRAP_SERVERS})
    producer.produce(topic, value=b'priming')
    producer.flush(timeout=10.0)

    sc = TestShareConsumer({'group.id': group_id})
    try:
        sc.subscribe([topic])
        warmup_share_consumers([sc])

        produced = []
        for i in range(5):
            key = f'k-{i}'.encode()
            value = f'v-{i}'.encode()
            headers = [(f'h-{i}', f'hv-{i}'.encode())]
            producer.produce(topic, key=key, value=value, headers=headers)
            produced.append((key, value, headers))
        producer.flush(timeout=10.0)

        received = drain_share_consumers([sc], 5)[0]
        assert len(received) == 5

        got = sorted([(m.key(), m.value(), m.headers()) for m in received])
        exp = sorted(produced)
        assert got == exp, f"Field mismatch: expected {exp}, got {got}"
    finally:
        sc.close()


@pytest.mark.integration
def test_multi_topic_subscription():
    """Subscribe to multiple topics; records from all topics are delivered."""
    base = unique_id('test-share-multi')
    topic_a = f'{base}-a'
    topic_b = f'{base}-b'
    group_id = unique_id('test-share-multi')
    n_per_topic = 5

    producer = Producer({'bootstrap.servers': DEFAULT_BOOTSTRAP_SERVERS})
    producer.produce(topic_a, value=b'priming')
    producer.produce(topic_b, value=b'priming')
    producer.flush(timeout=10.0)

    sc = TestShareConsumer({'group.id': group_id})
    try:
        sc.subscribe([topic_a, topic_b])
        warmup_share_consumers([sc])

        for i in range(n_per_topic):
            producer.produce(topic_a, value=f'a-{i}'.encode())
            producer.produce(topic_b, value=f'b-{i}'.encode())
        producer.flush(timeout=10.0)

        received = drain_share_consumers([sc], 2 * n_per_topic)[0]
        topics_seen = {m.topic() for m in received}
        assert topics_seen == {topic_a, topic_b}, f"Expected both topics, got {topics_seen}"
        assert (
            len(received) == 2 * n_per_topic
        ), f"Expected {2 * n_per_topic} records across both topics, got {len(received)}"
    finally:
        sc.close()


@pytest.mark.integration
def test_records_before_join_not_delivered():
    """KIP-932: records produced before consumer joins must not be delivered."""
    topic = unique_id('test-share-prejoin')
    group_id = unique_id('test-share-prejoin')
    n = 20

    producer = Producer({'bootstrap.servers': DEFAULT_BOOTSTRAP_SERVERS})
    for i in range(n):
        producer.produce(topic, value=f'pre-{i}'.encode())
    producer.flush(timeout=10.0)

    sc = TestShareConsumer({'group.id': group_id})
    try:
        sc.subscribe([topic])
        # Combined warmup + drain — pre-join records (if delivered at all)
        # would arrive within this window.
        received = []
        deadline = time.time() + 8.0
        while time.time() < deadline:
            for m in sc.poll(timeout=0.5):
                if m.error() is None:
                    received.append(m)

        assert received == [], (
            f"Pre-join records were delivered ({len(received)} messages); "
            f"share consumers must only see records produced after join"
        )
    finally:
        sc.close()


@pytest.mark.integration
def test_three_consumers_no_overlap():
    """Three consumers in same share group: no overlap, full coverage."""
    topic = unique_id('test-share-three')
    group_id = unique_id('test-share-three')
    n = 30

    producer = Producer({'bootstrap.servers': DEFAULT_BOOTSTRAP_SERVERS})
    producer.produce(topic, value=b'priming')
    producer.flush(timeout=10.0)

    consumers = [TestShareConsumer({'group.id': group_id}) for _ in range(3)]

    try:
        for sc in consumers:
            sc.subscribe([topic])
        warmup_share_consumers(consumers)

        for i in range(n):
            producer.produce(topic, value=f'msg-{i}'.encode())
        producer.flush(timeout=10.0)

        received = drain_share_consumers(consumers, n)
        offset_sets = [{(m.topic(), m.partition(), m.offset()) for m in r} for r in received]

        for i in range(len(offset_sets)):
            for j in range(i + 1, len(offset_sets)):
                overlap = offset_sets[i] & offset_sets[j]
                assert overlap == set(), f"Consumers {i} and {j} both received: {overlap}"

        union = set().union(*offset_sets)
        assert len(union) == n, (
            f"Expected {n} unique records, got {len(union)} " f"(per-consumer counts: {[len(s) for s in offset_sets]})"
        )
    finally:
        for sc in consumers:
            sc.close()


@pytest.mark.integration
def test_independent_share_groups():
    """Two consumers in different share groups each see all records."""
    topic = unique_id('test-share-independent')
    group_a = unique_id('test-share-group-a')
    group_b = unique_id('test-share-group-b')
    n = 10

    producer = Producer({'bootstrap.servers': DEFAULT_BOOTSTRAP_SERVERS})
    producer.produce(topic, value=b'priming')
    producer.flush(timeout=10.0)

    sc_a = TestShareConsumer({'group.id': group_a})
    sc_b = TestShareConsumer({'group.id': group_b})

    try:
        sc_a.subscribe([topic])
        sc_b.subscribe([topic])
        warmup_share_consumers([sc_a, sc_b])

        for i in range(n):
            producer.produce(topic, value=f'msg-{i}'.encode())
        producer.flush(timeout=10.0)

        received_a, received_b = drain_share_consumers([sc_a, sc_b], 2 * n)

        offsets_a = {(m.topic(), m.partition(), m.offset()) for m in received_a}
        offsets_b = {(m.topic(), m.partition(), m.offset()) for m in received_b}

        assert len(offsets_a) == n, f"Group A got {len(offsets_a)} unique records, expected {n}"
        assert len(offsets_b) == n, f"Group B got {len(offsets_b)} unique records, expected {n}"
        assert offsets_a == offsets_b, "Both groups should see the same set of records"
    finally:
        sc_a.close()
        sc_b.close()


@pytest.mark.integration
def test_implicit_ack_no_redelivery():
    """Records consumed in poll N are implicitly accepted on later polls; no redelivery."""
    topic = unique_id('test-share-ack')
    group_id = unique_id('test-share-ack')
    n = 10

    producer = Producer({'bootstrap.servers': DEFAULT_BOOTSTRAP_SERVERS})
    producer.produce(topic, value=b'priming')
    producer.flush(timeout=10.0)

    sc = TestShareConsumer({'group.id': group_id})
    try:
        sc.subscribe([topic])
        warmup_share_consumers([sc])

        for i in range(n):
            producer.produce(topic, value=f'msg-{i}'.encode())
        producer.flush(timeout=10.0)

        seen = set()
        deadline = time.time() + 20.0
        while time.time() < deadline and len(seen) < n:
            for m in sc.poll(timeout=0.5):
                if m.error() is None:
                    seen.add((m.partition(), m.offset()))

        assert len(seen) == n, f"Failed to consume all {n} records (got {len(seen)})"

        # Continue polling — implicit ack should accept previously delivered
        # records, so no redelivery should occur.
        extras = []
        for _ in range(8):
            for m in sc.poll(timeout=0.5):
                if m.error() is None:
                    extras.append((m.partition(), m.offset()))

        assert extras == [], f"Records were redelivered after implicit ack: {extras}"
    finally:
        sc.close()


@pytest.mark.integration
def test_unsubscribe_stops_delivery():
    """After unsubscribe, future polls return no records even when broker has new ones."""
    topic = unique_id('test-share-unsub')
    group_id = unique_id('test-share-unsub')

    producer = Producer({'bootstrap.servers': DEFAULT_BOOTSTRAP_SERVERS})
    producer.produce(topic, value=b'priming')
    producer.flush(timeout=10.0)

    sc = TestShareConsumer({'group.id': group_id})
    try:
        sc.subscribe([topic])
        warmup_share_consumers([sc])

        for i in range(5):
            producer.produce(topic, value=f'msg-{i}'.encode())
        producer.flush(timeout=10.0)

        first_batch = drain_share_consumers([sc], 5)[0]
        assert len(first_batch) == 5, f"Pre-unsubscribe phase incomplete (got {len(first_batch)}/5)"

        sc.unsubscribe()

        for i in range(5, 10):
            producer.produce(topic, value=f'msg-{i}'.encode())
        producer.flush(timeout=10.0)

        post = []
        deadline = time.time() + 5.0
        while time.time() < deadline:
            for m in sc.poll(timeout=0.5):
                if m.error() is None:
                    post.append(m)

        assert post == [], f"Records delivered after unsubscribe: {len(post)} messages"
    finally:
        sc.close()


@pytest.mark.integration
def test_resubscribe_to_different_topic():
    """subscribe() replaces (does not extend) the prior subscription."""
    base = unique_id('test-share-resub')
    topic_a = f'{base}-a'
    topic_b = f'{base}-b'
    group_id = unique_id('test-share-resub')

    producer = Producer({'bootstrap.servers': DEFAULT_BOOTSTRAP_SERVERS})
    producer.produce(topic_a, value=b'priming')
    producer.produce(topic_b, value=b'priming')
    producer.flush(timeout=10.0)

    sc = TestShareConsumer({'group.id': group_id})
    try:
        sc.subscribe([topic_a])
        warmup_share_consumers([sc])

        sc.subscribe([topic_b])
        warmup_share_consumers([sc])

        for i in range(5):
            producer.produce(topic_a, value=f'a-{i}'.encode())
            producer.produce(topic_b, value=f'b-{i}'.encode())
        producer.flush(timeout=10.0)

        received = drain_share_consumers([sc], 5)[0]
        topics = {m.topic() for m in received}
        assert topics == {topic_b}, f"Resubscribe should drop topic_a; got topics {topics}"
        assert len(received) == 5, f"Expected 5 topic_b records, got {len(received)}"
    finally:
        sc.close()


@pytest.mark.integration
def test_messages_in_offset_order_single_consumer():
    """Within each partition, single consumer sees records in offset order."""
    topic = unique_id('test-share-order')
    group_id = unique_id('test-share-order')
    n = 30

    producer = Producer({'bootstrap.servers': DEFAULT_BOOTSTRAP_SERVERS})
    producer.produce(topic, value=b'priming')
    producer.flush(timeout=10.0)

    sc = TestShareConsumer({'group.id': group_id})
    try:
        sc.subscribe([topic])
        warmup_share_consumers([sc])

        for i in range(n):
            producer.produce(topic, value=f'msg-{i}'.encode())
        producer.flush(timeout=10.0)

        per_partition = {}
        total = 0
        deadline = time.time() + 20.0
        while time.time() < deadline and total < n:
            for m in sc.poll(timeout=0.5):
                if m.error() is None:
                    per_partition.setdefault(m.partition(), []).append(m.offset())
                    total += 1

        assert total == n, f"Expected {n} records, got {total}"

        for p, offsets in per_partition.items():
            assert offsets == sorted(offsets), f"Partition {p} offsets out of order: {offsets}"
    finally:
        sc.close()


def test_error_cb():
    """Test that error_cb fires for ShareConsumer when broker is unreachable."""
    error_called = []

    def my_error_cb(error):
        error_called.append(error)

    sc = ShareConsumer(
        {
            'group.id': 'test-share-error-cb',
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
    """Test that an exception raised in error_cb propagates to poll."""
    error_called = []
    raising = [True]

    def error_cb_that_raises(error):
        error_called.append(error)
        if raising[0]:
            raise RuntimeError("Test exception from error_cb")

    sc = ShareConsumer(
        {
            'group.id': 'test-share-error-cb-exc',
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

    raising[0] = False
    sc.close()


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
            'group.id': 'test-share-throttle-cb',
            'bootstrap.servers': 'localhost:19999',
            'socket.timeout.ms': 100,
            'throttle_cb': my_throttle_cb,
        }
    )

    sc.subscribe(['test-topic'])

    # throttle_cb won't fire without broker throttling — just verify no crash
    sc.poll(timeout=0.2)
    sc.close()
