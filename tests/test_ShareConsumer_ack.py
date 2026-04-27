#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Integration tests for ShareConsumer acknowledgement.

Needs a Kafka broker with KIP-932 enabled at localhost:9092. Tests skip
themselves when the broker isn't reachable.
"""

import time
import uuid

import pytest

from confluent_kafka import (
    AcknowledgeType,
    KafkaException,
    Producer,
    ShareConsumer,
)
from confluent_kafka.admin import (
    AdminClient,
    AlterConfigOpType,
    ConfigEntry,
    ConfigResource,
)


BOOTSTRAP_SERVERS = 'localhost:9092'
POLL_TIMEOUT = 10.0  # share session warm-up takes a few seconds


# --- helpers ---------------------------------------------------------------


def _unique_id():
    return uuid.uuid4().hex[:8]


def _broker_reachable():
    try:
        p = Producer({'bootstrap.servers': BOOTSTRAP_SERVERS,
                      'socket.timeout.ms': 2000})
        return len(p.list_topics(timeout=2.0).brokers) > 0
    except Exception:
        return False


broker_required = pytest.mark.skipif(
    not _broker_reachable(),
    reason='Kafka broker with KIP-932 not reachable at localhost:9092',
)


def _produce(topic, count, prefix='msg'):
    """Produce `count` messages, return the list of values."""
    p = Producer({'bootstrap.servers': BOOTSTRAP_SERVERS})
    values = [f'{prefix}-{i}'.encode() for i in range(count)]
    for v in values:
        p.produce(topic, value=v)
    p.flush(timeout=10)
    return values


def _create_topic(topic):
    """Force topic auto-creation and wait for metadata to propagate so a
    subsequent subscribe() doesn't race with topic creation."""
    p = Producer({'bootstrap.servers': BOOTSTRAP_SERVERS})
    p.produce(topic, value=b'__init__')
    p.flush(timeout=10)
    deadline = time.monotonic() + 5.0
    while time.monotonic() < deadline:
        md = p.list_topics(topic, timeout=2.0)
        if topic in md.topics and md.topics[topic].partitions:
            return
        time.sleep(0.1)
    raise RuntimeError(f'topic {topic!r} did not appear in metadata')


def _consumer(group_id, mode='implicit', extra=None):
    config = {
        'bootstrap.servers': BOOTSTRAP_SERVERS,
        'group.id': group_id,
        'share.acknowledgement.mode': mode,
    }
    if extra:
        config.update(extra)
    return ShareConsumer(config)


def _warmup(consumer, seconds=8.0):
    """Drive empty polls until the share session reaches Stable.

    The SPSO is initialised to `latest` once partitions are assigned, so
    anything we produce *before* the session is up will be skipped by the
    broker (AcquiredRecordsArrayCnt=0). Always warm up before producing.
    """
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        consumer.poll(timeout=1.0)


def _collect(consumer, n, timeout, ack_type=None):
    """Poll until we've gathered `n` records or `timeout` seconds elapse.

    In explicit mode librdkafka raises __STATE on the next poll() if any
    records from the previous batch are still unacked, so callers using
    explicit mode should pass `ack_type` to ack inline as records arrive.
    """
    deadline = time.monotonic() + timeout
    out = []
    while len(out) < n and time.monotonic() < deadline:
        for m in consumer.poll(timeout=2.0):
            if m.error() is None:
                out.append(m)
                if ack_type is not None:
                    consumer.acknowledge(m, ack_type)
    return out


# --- implicit mode ---------------------------------------------------------


@pytest.mark.integration
@broker_required
def test_implicit_mode_acknowledge_raises():
    """Calling acknowledge() in implicit mode is a programming error — let
    the next poll/close auto-commit instead."""
    topic = f'ack_implicit_{_unique_id()}'
    group = f'g_{_unique_id()}'
    _create_topic(topic)

    sc = _consumer(group, mode='implicit')
    try:
        sc.subscribe([topic])
        _warmup(sc)
        _produce(topic, 1)

        msgs = _collect(sc, 1, POLL_TIMEOUT)
        assert msgs, 'no messages received'

        with pytest.raises(KafkaException) as ex:
            sc.acknowledge(msgs[0], AcknowledgeType.ACCEPT)
        assert ex.value.args[0].code() == -172  # __STATE
    finally:
        sc.close()


@pytest.mark.integration
@broker_required
def test_implicit_mode_autocommits_on_next_poll():
    """A second poll() in implicit mode commits the previous batch.
    A fresh consumer in the same group should see no leftovers."""
    topic = f'autocommit_{_unique_id()}'
    group = f'g_{_unique_id()}'
    _create_topic(topic)

    sc1 = _consumer(group, mode='implicit')
    try:
        sc1.subscribe([topic])
        _warmup(sc1)
        produced = _produce(topic, 3)

        first = _collect(sc1, 3, POLL_TIMEOUT)
        assert len(first) == 3

        sc1.poll(timeout=2.0)  # commits the previous batch
    finally:
        sc1.close()

    sc2 = _consumer(group, mode='implicit')
    try:
        sc2.subscribe([topic])
        _warmup(sc2)
        leftovers = _collect(sc2, 1, timeout=5.0)
        assert leftovers == [], (
            f'expected no redelivery, got {[m.value() for m in leftovers]} '
            f'(produced {produced})'
        )
    finally:
        sc2.close()


# --- explicit mode: ACCEPT / REJECT / RELEASE ------------------------------


@pytest.mark.integration
@broker_required
def test_accept_prevents_redelivery():
    topic = f'accept_{_unique_id()}'
    group = f'g_{_unique_id()}'
    _create_topic(topic)

    sc1 = _consumer(group, mode='explicit')
    try:
        sc1.subscribe([topic])
        _warmup(sc1)
        _produce(topic, 3)

        msgs = _collect(sc1, 3, POLL_TIMEOUT, ack_type=AcknowledgeType.ACCEPT)
        assert len(msgs) == 3

        sc1.poll(timeout=2.0)  # flush ACCEPTs
    finally:
        sc1.close()

    sc2 = _consumer(group, mode='explicit')
    try:
        sc2.subscribe([topic])
        _warmup(sc2)
        leftovers = _collect(sc2, 1, timeout=5.0)
        assert leftovers == [], 'ACCEPT should prevent redelivery'
    finally:
        sc2.close()


@pytest.mark.integration
@broker_required
def test_reject_prevents_redelivery():
    topic = f'reject_{_unique_id()}'
    group = f'g_{_unique_id()}'
    _create_topic(topic)

    sc1 = _consumer(group, mode='explicit')
    try:
        sc1.subscribe([topic])
        _warmup(sc1)
        _produce(topic, 3)

        msgs = _collect(sc1, 3, POLL_TIMEOUT, ack_type=AcknowledgeType.REJECT)
        assert len(msgs) == 3

        sc1.poll(timeout=2.0)  # flush REJECTs
    finally:
        sc1.close()

    sc2 = _consumer(group, mode='explicit')
    try:
        sc2.subscribe([topic])
        _warmup(sc2)
        leftovers = _collect(sc2, 1, timeout=5.0)
        assert leftovers == [], 'REJECT should permanently archive'
    finally:
        sc2.close()


@pytest.mark.integration
@broker_required
def test_release_causes_redelivery():
    """A released record comes back on a subsequent poll, same offset."""
    topic = f'release_{_unique_id()}'
    group = f'g_{_unique_id()}'
    _create_topic(topic)

    sc = _consumer(group, mode='explicit')
    try:
        sc.subscribe([topic])
        _warmup(sc)
        _produce(topic, 1)

        first_batch = _collect(sc, 1, POLL_TIMEOUT)
        assert first_batch
        first = first_batch[0]
        coords = (first.topic(), first.partition(), first.offset())

        sc.acknowledge(first, AcknowledgeType.RELEASE)

        deadline = time.monotonic() + 15.0
        seen_again = False
        while time.monotonic() < deadline and not seen_again:
            for m in sc.poll(timeout=2.0):
                if m.error() is None:
                    if (m.topic(), m.partition(), m.offset()) == coords:
                        seen_again = True
                    sc.acknowledge(m, AcknowledgeType.ACCEPT)
                    if seen_again:
                        break
        assert seen_again, f'released record {coords} never came back'
    finally:
        sc.close()


# --- explicit mode: forgotten acks block the next poll --------------------


@pytest.mark.integration
@broker_required
def test_unacked_records_block_next_poll():
    """If we leave records unacked in explicit mode, librdkafka raises
    __STATE on the next poll() — the equivalent of Java's
    IllegalStateException. Acking the leftovers clears the block."""
    topic = f'partial_{_unique_id()}'
    group = f'g_{_unique_id()}'
    _create_topic(topic)

    sc = _consumer(group, mode='explicit')
    try:
        sc.subscribe([topic])
        _warmup(sc)
        _produce(topic, 1)

        # Grab at least one record, deliberately don't ack it.
        first_batch = []
        deadline = time.monotonic() + POLL_TIMEOUT
        while not first_batch and time.monotonic() < deadline:
            for m in sc.poll(timeout=2.0):
                if m.error() is None:
                    first_batch.append(m)
        assert first_batch, 'never received any messages'

        with pytest.raises(KafkaException) as ex:
            sc.poll(timeout=2.0)
        err = ex.value.args[0]
        assert err.code() == -172
        assert 'not been acknowledged' in err.str()

        # Recovery: ack and the next poll should be fine.
        for m in first_batch:
            sc.acknowledge(m, AcknowledgeType.ACCEPT)
        sc.poll(timeout=2.0)
    finally:
        sc.close()


# --- queue-style distribution across multiple consumers --------------------


@pytest.mark.integration
@broker_required
def test_two_consumers_share_workload():
    """Two consumers in the same share group should split the records
    between them with no overlap."""
    topic = f'distribute_{_unique_id()}'
    group = f'g_{_unique_id()}'
    n = 30
    _create_topic(topic)

    sc1 = _consumer(group, mode='explicit')
    sc2 = _consumer(group, mode='explicit')
    try:
        sc1.subscribe([topic])
        sc2.subscribe([topic])
        _warmup(sc1)
        _warmup(sc2)

        produced = set(_produce(topic, n))

        seen1, seen2 = [], []
        deadline = time.monotonic() + 30.0
        while len(seen1) + len(seen2) < n and time.monotonic() < deadline:
            for sc, bucket in ((sc1, seen1), (sc2, seen2)):
                for m in sc.poll(timeout=1.0):
                    if m.error() is None:
                        bucket.append(m.value())
                        sc.acknowledge(m, AcknowledgeType.ACCEPT)

        s1, s2 = set(seen1), set(seen2)
        assert s1 | s2 == produced, f'missing records: {produced - (s1 | s2)}'
        assert s1 & s2 == set(), f'duplicate delivery: {s1 & s2}'
    finally:
        sc1.close()
        sc2.close()


# --- lock expiry, delivery limit, atomicity, transactions -----------------

# Default broker lock duration is 30s. We override it via AdminClient at
# group level to shorten the wait — broker enforces a minimum
# (group.share.min.record.lock.duration.ms, default 15000).
SHORT_LOCK_MS = 15000
LOCK_EXPIRY_BUFFER_S = 5  # seconds beyond SHORT_LOCK_MS before we consider it expired


def _set_group_configs(group_id, configs):
    """Set dynamic share-group configs (broker/group-level things like
    share.record.lock.duration.ms or share.isolation.level). Re-raises
    KafkaException so the test can decide whether to fail or skip based
    on the actual error code."""
    admin = AdminClient({'bootstrap.servers': BOOTSTRAP_SERVERS})
    res = ConfigResource(ConfigResource.Type.GROUP, group_id)
    for name, value in configs.items():
        res.add_incremental_config(
            ConfigEntry(name, str(value),
                        incremental_operation=AlterConfigOpType.SET)
        )
    for f in admin.incremental_alter_configs([res]).values():
        f.result(timeout=10)


@pytest.mark.integration
@broker_required
def test_lock_elapsed_acknowledge_does_not_consume_record():
    """The Java spec says acknowledging a lock-expired record raises
    InvalidRecordStateException. librdkafka's current behaviour is more
    lenient — the late acknowledge() returns OK, but the broker treats it
    as a no-op so the record is still re-deliverable. Either way, the
    record must NOT silently disappear."""
    topic = f'lock_ack_{_unique_id()}'
    group = f'g_{_unique_id()}'
    _create_topic(topic)

    try:
        _set_group_configs(group,
                           {'share.record.lock.duration.ms': SHORT_LOCK_MS})
    except KafkaException as e:
        pytest.skip(f'cannot lower share group lock duration: {e}')

    sc = _consumer(group, mode='explicit')
    try:
        sc.subscribe([topic])
        _warmup(sc)
        _produce(topic, 1)

        msgs = _collect(sc, 1, POLL_TIMEOUT)
        assert msgs, 'no messages received'
        first = msgs[0]
        coords = (first.topic(), first.partition(), first.offset())

        # Sleep past the lock duration — broker reverts record to Available.
        time.sleep(SHORT_LOCK_MS / 1000.0 + LOCK_EXPIRY_BUFFER_S)

        # Acking the now-stale message: librdkafka may raise or silently
        # accept. Either way, the record is the broker's authority.
        try:
            sc.acknowledge(first, AcknowledgeType.ACCEPT)
        except KafkaException as e:
            code = e.args[0].code()
            assert code in (-172, 121), f'unexpected error code {code}'
            return  # behaved per Java spec — done

        # Late ack didn't raise. The record should still come back from the
        # broker because the ack arrived after the lock expired.
        deadline = time.monotonic() + 20.0
        seen_again = False
        while not seen_again and time.monotonic() < deadline:
            try:
                batch = sc.poll(timeout=2.0)
            except KafkaException:
                continue
            for m in batch:
                if (
                    m.error() is None
                    and (m.topic(), m.partition(), m.offset()) == coords
                ):
                    seen_again = True
                    sc.acknowledge(m, AcknowledgeType.ACCEPT)
                    break
        assert seen_again, (
            'lock-elapsed record disappeared — late ack should have been '
            'rejected by broker, leaving the record available for redelivery'
        )
    finally:
        sc.close()


@pytest.mark.integration
@broker_required
def test_lock_elapsed_record_redelivered_to_same_consumer():
    """A record whose lock expired returns to Available and is redelivered.
    With a single consumer running, that consumer gets it back."""
    topic = f'lock_redeliver_{_unique_id()}'
    group = f'g_{_unique_id()}'
    _create_topic(topic)

    try:
        _set_group_configs(group,
                           {'share.record.lock.duration.ms': SHORT_LOCK_MS})
    except KafkaException as e:
        pytest.skip(f'cannot lower share group lock duration: {e}')

    sc = _consumer(group, mode='explicit')
    try:
        sc.subscribe([topic])
        _warmup(sc)
        _produce(topic, 1)

        first_batch = _collect(sc, 1, POLL_TIMEOUT)
        assert first_batch
        first = first_batch[0]
        coords = (first.topic(), first.partition(), first.offset())

        # Wait for the lock to expire on the broker.
        time.sleep(SHORT_LOCK_MS / 1000.0 + LOCK_EXPIRY_BUFFER_S)

        # Clear librdkafka's "unacked" book-keeping for the now-stale
        # record. The broker will ignore this ack since the lock expired,
        # but it lets us poll() again without librdkafka raising __STATE.
        sc.acknowledge(first, AcknowledgeType.ACCEPT)

        deadline = time.monotonic() + 15.0
        seen_again = False
        while not seen_again and time.monotonic() < deadline:
            for m in sc.poll(timeout=2.0):
                if m.error() is None and (
                    m.topic(), m.partition(), m.offset()
                ) == coords:
                    seen_again = True
                    sc.acknowledge(m, AcknowledgeType.ACCEPT)
                    break
        assert seen_again, f'expired record {coords} was never re-delivered'
    finally:
        sc.close()


@pytest.mark.integration
@broker_required
def test_delivery_attempt_limit_archives_record():
    """After being released `group.share.delivery.attempt.limit` times
    (default 5), a record is permanently archived and never delivered again."""
    topic = f'poison_{_unique_id()}'
    group = f'g_{_unique_id()}'
    _create_topic(topic)

    sc = _consumer(group, mode='explicit')
    try:
        sc.subscribe([topic])
        _warmup(sc)
        _produce(topic, 1, prefix='poison')

        coords = None
        releases = 0
        deadline = time.monotonic() + 60.0
        # Default delivery limit is 5 — release that many times.
        while releases < 5 and time.monotonic() < deadline:
            for m in sc.poll(timeout=2.0):
                if m.error() is None:
                    if coords is None:
                        coords = (m.topic(), m.partition(), m.offset())
                    sc.acknowledge(m, AcknowledgeType.RELEASE)
                    releases += 1
                    if releases >= 5:
                        break
        assert releases >= 5, f'only saw {releases} releases, expected 5'
        assert coords is not None

        # Flush the final RELEASE.
        try:
            sc.poll(timeout=2.0)
        except KafkaException:
            pass

        # The record must NOT come back. Watch for ~10s.
        deadline = time.monotonic() + 10.0
        while time.monotonic() < deadline:
            for m in sc.poll(timeout=2.0):
                if (
                    m.error() is None
                    and (m.topic(), m.partition(), m.offset()) == coords
                ):
                    pytest.fail(
                        f'record {coords} was redelivered after hitting '
                        f'delivery.attempt.limit — broker should have archived it'
                    )
    finally:
        sc.close()



@pytest.mark.integration
@broker_required
def test_open_transaction_stalls_share_group():
    """In read_committed isolation, an open transaction blocks the share
    group from seeing records past the LSO. Once the transaction commits,
    the records become deliverable."""
    topic = f'txn_stall_{_unique_id()}'
    group = f'g_{_unique_id()}'
    _create_topic(topic)

    txn_producer = Producer({
        'bootstrap.servers': BOOTSTRAP_SERVERS,
        'transactional.id': f'txn_{_unique_id()}',
    })
    try:
        txn_producer.init_transactions(10)
    except KafkaException as e:
        pytest.skip(f'broker does not support transactions: {e}')

    try:
        _set_group_configs(group,
                           {'share.isolation.level': 'read_committed'})
    except KafkaException as e:
        pytest.skip(f'cannot set share.isolation.level on group: {e}')

    sc = _consumer(group, mode='explicit')
    try:
        sc.subscribe([topic])
        _warmup(sc)

        # Begin a transaction, produce, but DO NOT commit yet.
        txn_producer.begin_transaction()
        for i in range(3):
            txn_producer.produce(topic, value=f'txn-{i}'.encode())
        txn_producer.flush(5)

        # While the transaction is open, read_committed must NOT deliver.
        stalled = _collect(sc, 1, timeout=5.0)
        assert stalled == [], (
            f'open transaction did not stall the share group: '
            f'received {[m.value() for m in stalled]}'
        )

        # Commit the transaction — records should now flow.
        txn_producer.commit_transaction(10)

        msgs = _collect(sc, 3, POLL_TIMEOUT,
                        ack_type=AcknowledgeType.ACCEPT)
        assert len(msgs) == 3, f'expected 3 msgs after commit, got {len(msgs)}'
    finally:
        sc.close()
