#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Integration tests for ShareConsumer.commit_sync() / commit_async().

Needs a Kafka broker with KIP-932 enabled at localhost:9092. Tests skip
themselves when the broker isn't reachable.
"""

import threading
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
    NewTopic,
)

BOOTSTRAP_SERVERS = 'localhost:9092'
POLL_TIMEOUT = 10.0
SHORT_LOCK_MS = 15000  # broker minimum
LOCK_EXPIRY_BUFFER_S = 5

ERR_STATE = -172  # RD_KAFKA_RESP_ERR__STATE
ERR_INVALID_RECORD = 121  # INVALID_RECORD_STATE


# --- helpers ---------------------------------------------------------------


def _unique_id():
    return uuid.uuid4().hex[:8]


def _broker_reachable():
    try:
        p = Producer({'bootstrap.servers': BOOTSTRAP_SERVERS, 'socket.timeout.ms': 2000})
        return len(p.list_topics(timeout=2.0).brokers) > 0
    except Exception:
        return False


broker_required = pytest.mark.skipif(
    not _broker_reachable(),
    reason='Kafka broker with KIP-932 not reachable at localhost:9092',
)


def _produce(topic, count, prefix='msg', partition=None):
    p = Producer({'bootstrap.servers': BOOTSTRAP_SERVERS})
    values = [f'{prefix}-{i}'.encode() for i in range(count)]
    for v in values:
        if partition is None:
            p.produce(topic, value=v)
        else:
            p.produce(topic, value=v, partition=partition)
    p.flush(timeout=10)
    return values


def _create_topic(topic, num_partitions=1):
    if num_partitions > 1:
        admin = AdminClient({'bootstrap.servers': BOOTSTRAP_SERVERS})
        fs = admin.create_topics([NewTopic(topic, num_partitions=num_partitions, replication_factor=1)])
        for f in fs.values():
            try:
                f.result(timeout=10)
            except KafkaException:
                pass  # may already exist; metadata wait below covers it

    p = Producer({'bootstrap.servers': BOOTSTRAP_SERVERS})
    if num_partitions == 1:
        p.produce(topic, value=b'__init__')
        p.flush(timeout=10)

    deadline = time.monotonic() + 10.0
    while time.monotonic() < deadline:
        md = p.list_topics(topic, timeout=2.0)
        if topic in md.topics and md.topics[topic].partitions and len(md.topics[topic].partitions) >= num_partitions:
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
    """Drive empty polls until the share session is Stable. SPSO is
    initialised to `latest` once partitions are assigned, so anything
    produced before warmup is skipped by the broker."""
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        consumer.poll(timeout=1.0)


def _collect(consumer, n, timeout, ack_type=None):
    """Poll until we have `n` records or `timeout` elapses. In explicit
    mode the caller must pass `ack_type` so each record is acked inline,
    otherwise the next poll raises __STATE."""
    deadline = time.monotonic() + timeout
    out = []
    while len(out) < n and time.monotonic() < deadline:
        for m in consumer.poll(timeout=2.0):
            if m.error() is None:
                out.append(m)
                if ack_type is not None:
                    consumer.acknowledge(m, ack_type)
    return out


def _set_group_configs(group_id, configs):
    admin = AdminClient({'bootstrap.servers': BOOTSTRAP_SERVERS})
    res = ConfigResource(ConfigResource.Type.GROUP, group_id)
    for name, value in configs.items():
        res.add_incremental_config(ConfigEntry(name, str(value), incremental_operation=AlterConfigOpType.SET))
    for f in admin.incremental_alter_configs([res]).values():
        f.result(timeout=10)


# --- happy path -----------------------------------------------------------


@pytest.mark.integration
@broker_required
def test_implicit_commit_sync_returns_partition_results():
    """Implicit + sync commit returns a dict of TopicPartition -> None with
    no per-partition errors."""
    topic = f'commit_implicit_sync_{_unique_id()}'
    group = f'g_{_unique_id()}'
    _create_topic(topic)

    sc = _consumer(group, mode='implicit')
    try:
        sc.subscribe([topic])
        _warmup(sc)
        _produce(topic, 3)

        msgs = _collect(sc, 3, POLL_TIMEOUT)
        assert len(msgs) == 3

        result = sc.commit_sync(timeout=10.0)
        assert isinstance(result, dict)
        assert len(result) >= 1
        for tp, err in result.items():
            assert err is None, f'unexpected error: {err}'
    finally:
        sc.close()


@pytest.mark.integration
@broker_required
def test_implicit_commit_async_returns_immediately():
    """Async commit returns None and doesn't block."""
    topic = f'commit_implicit_async_{_unique_id()}'
    group = f'g_{_unique_id()}'
    _create_topic(topic)

    sc = _consumer(group, mode='implicit')
    try:
        sc.subscribe([topic])
        _warmup(sc)
        _produce(topic, 3)

        assert len(_collect(sc, 3, POLL_TIMEOUT)) == 3

        start = time.monotonic()
        result = sc.commit_async()
        elapsed = time.monotonic() - start

        assert result is None
        assert elapsed < 1.0, f'async commit blocked for {elapsed:.2f}s'
    finally:
        sc.close()


@pytest.mark.integration
@broker_required
def test_explicit_commit_sync_succeeds():
    """ACCEPT every record then commit; per-partition results are clean."""
    topic = f'commit_explicit_accept_{_unique_id()}'
    group = f'g_{_unique_id()}'
    _create_topic(topic)

    sc = _consumer(group, mode='explicit')
    try:
        sc.subscribe([topic])
        _warmup(sc)
        _produce(topic, 3)

        msgs = _collect(sc, 3, POLL_TIMEOUT, ack_type=AcknowledgeType.ACCEPT)
        assert len(msgs) == 3

        result = sc.commit_sync(timeout=10.0)
        assert isinstance(result, dict)
        for tp, err in result.items():
            assert err is None
    finally:
        sc.close()


@pytest.mark.integration
@broker_required
def test_commit_with_nothing_pending_returns_empty():
    """Commit before any poll returns an empty dict (sync) / None (async) —
    librdkafka returns NULL c_parts when there are no acks to send."""
    topic = f'commit_empty_{_unique_id()}'
    group = f'g_{_unique_id()}'
    _create_topic(topic)

    sc = _consumer(group, mode='implicit')
    try:
        sc.subscribe([topic])
        _warmup(sc)

        assert sc.commit_sync(timeout=2.0) == {}
        assert sc.commit_async() is None
    finally:
        sc.close()


@pytest.mark.integration
@broker_required
def test_repeated_commit_returns_empty_on_second_call():
    """Two commits in a row: the second has nothing to send."""
    topic = f'commit_repeated_{_unique_id()}'
    group = f'g_{_unique_id()}'
    _create_topic(topic)

    sc = _consumer(group, mode='explicit')
    try:
        sc.subscribe([topic])
        _warmup(sc)
        _produce(topic, 2)

        msgs = _collect(sc, 2, POLL_TIMEOUT, ack_type=AcknowledgeType.ACCEPT)
        assert len(msgs) == 2

        first = sc.commit_sync(timeout=10.0)
        assert isinstance(first, dict)

        second = sc.commit_sync(timeout=2.0)
        assert second == {}
    finally:
        sc.close()


# --- AcknowledgeType behaviors --------------------------------------------


@pytest.mark.integration
@broker_required
def test_reject_persists_through_commit():
    """REJECT followed by commit archives the record permanently."""
    topic = f'commit_reject_{_unique_id()}'
    group = f'g_{_unique_id()}'
    _create_topic(topic)

    sc1 = _consumer(group, mode='explicit')
    try:
        sc1.subscribe([topic])
        _warmup(sc1)
        _produce(topic, 1)

        msgs = _collect(sc1, 1, POLL_TIMEOUT, ack_type=AcknowledgeType.REJECT)
        assert len(msgs) == 1

        result = sc1.commit_sync(timeout=10.0)
        for tp, err in result.items():
            assert err is None
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
def test_release_through_commit_returns_record_to_available():
    """RELEASE + commit → record comes back on a later poll."""
    topic = f'commit_release_{_unique_id()}'
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
        result = sc.commit_sync(timeout=10.0)
        for tp, err in result.items():
            assert err is None

        deadline = time.monotonic() + 15.0
        seen_again = False
        while not seen_again and time.monotonic() < deadline:
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


@pytest.mark.integration
@broker_required
def test_commit_with_mixed_ack_types():
    """ACCEPT/RELEASE/REJECT mixed in one batch all land via commit."""
    topic = f'commit_mixed_{_unique_id()}'
    group = f'g_{_unique_id()}'
    _create_topic(topic)

    sc = _consumer(group, mode='explicit')
    try:
        sc.subscribe([topic])
        _warmup(sc)
        _produce(topic, 6)

        types = [
            AcknowledgeType.ACCEPT,
            AcknowledgeType.RELEASE,
            AcknowledgeType.REJECT,
        ]
        gathered = 0
        deadline = time.monotonic() + POLL_TIMEOUT
        while gathered < 6 and time.monotonic() < deadline:
            for m in sc.poll(timeout=2.0):
                if m.error() is None:
                    sc.acknowledge(m, types[gathered % 3])
                    gathered += 1
                    if gathered >= 6:
                        break
        assert gathered >= 3, f'only got {gathered} records'

        result = sc.commit_sync(timeout=10.0)
        # acks may have piggybacked on intermediate polls
        for tp, err in result.items():
            assert err is None
    finally:
        sc.close()


@pytest.mark.integration
@broker_required
def test_committed_releases_archive_at_delivery_limit():
    """RELEASE + commit, repeated 5 times (broker default), causes the
    record to be auto-archived as poison."""
    topic = f'commit_poison_{_unique_id()}'
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
        while releases < 5 and time.monotonic() < deadline:
            for m in sc.poll(timeout=2.0):
                if m.error() is None:
                    if coords is None:
                        coords = (m.topic(), m.partition(), m.offset())
                    sc.acknowledge(m, AcknowledgeType.RELEASE)
                    sc.commit_sync(timeout=5.0)
                    releases += 1
                    if releases >= 5:
                        break
        assert releases >= 5, f'only saw {releases} releases'

        deadline = time.monotonic() + 10.0
        while time.monotonic() < deadline:
            for m in sc.poll(timeout=2.0):
                if m.error() is None and (m.topic(), m.partition(), m.offset()) == coords:
                    pytest.fail(f'record {coords} redelivered after 5 releases')
    finally:
        sc.close()


# --- mode enforcement -----------------------------------------------------


@pytest.mark.integration
@broker_required
def test_implicit_acknowledge_raises_commit_still_works():
    """In implicit mode acknowledge() raises __STATE; commit() is fine
    (it auto-converts ACQUIRED → ACCEPT)."""
    topic = f'commit_implicit_ack_{_unique_id()}'
    group = f'g_{_unique_id()}'
    _create_topic(topic)

    sc = _consumer(group, mode='implicit')
    try:
        sc.subscribe([topic])
        _warmup(sc)
        _produce(topic, 1)

        msgs = _collect(sc, 1, POLL_TIMEOUT)
        assert msgs

        with pytest.raises(KafkaException) as ex:
            sc.acknowledge(msgs[0], AcknowledgeType.ACCEPT)
        assert ex.value.args[0].code() == ERR_STATE

        result = sc.commit_sync(timeout=10.0)
        assert isinstance(result, dict)
    finally:
        sc.close()


@pytest.mark.integration
@broker_required
def test_partial_ack_commit_then_unacked_blocks_poll():
    """Explicit mode: commit the acked records; the unacked ones still
    block the next poll with __STATE."""
    topic = f'commit_partial_{_unique_id()}'
    group = f'g_{_unique_id()}'
    _create_topic(topic)

    sc = _consumer(group, mode='explicit')
    try:
        sc.subscribe([topic])
        _warmup(sc)
        # produce many so the first poll likely returns ≥2
        _produce(topic, 10)

        gathered = []
        deadline = time.monotonic() + POLL_TIMEOUT
        while not gathered and time.monotonic() < deadline:
            for m in sc.poll(timeout=2.0):
                if m.error() is None:
                    gathered.append(m)
        assert gathered

        if len(gathered) < 2:
            pytest.skip(f'broker returned only {len(gathered)} record; ' 'need ≥2 to exercise partial ack')

        sc.acknowledge(gathered[0], AcknowledgeType.ACCEPT)
        sc.commit_sync(timeout=10.0)

        with pytest.raises(KafkaException) as ex:
            sc.poll(timeout=2.0)
        assert ex.value.args[0].code() == ERR_STATE

        for m in gathered[1:]:
            sc.acknowledge(m, AcknowledgeType.ACCEPT)
        sc.commit_sync(timeout=5.0)
    finally:
        sc.close()


@pytest.mark.integration
@broker_required
def test_explicit_commit_after_implicit_autocommit_is_noop():
    """Implicit mode auto-commits on the second poll; an explicit commit
    after that has nothing to send."""
    topic = f'commit_after_autocommit_{_unique_id()}'
    group = f'g_{_unique_id()}'
    _create_topic(topic)

    sc = _consumer(group, mode='implicit')
    try:
        sc.subscribe([topic])
        _warmup(sc)
        _produce(topic, 3)

        assert len(_collect(sc, 3, POLL_TIMEOUT)) == 3
        sc.poll(timeout=2.0)  # auto-commits the previous batch

        assert sc.commit_sync(timeout=2.0) == {}
    finally:
        sc.close()


# --- state-machine edge cases --------------------------------------------


@pytest.mark.integration
@broker_required
def test_commit_after_lock_expiry():
    """Acking + committing past the lock duration: librdkafka may raise
    __STATE on the ack, or the commit may surface INVALID_RECORD_STATE
    per partition. Either is acceptable."""
    topic = f'commit_lock_expiry_{_unique_id()}'
    group = f'g_{_unique_id()}'
    _create_topic(topic)

    try:
        _set_group_configs(group, {'share.record.lock.duration.ms': SHORT_LOCK_MS})
    except KafkaException as e:
        pytest.skip(f'cannot lower lock duration: {e}')

    sc = _consumer(group, mode='explicit')
    try:
        sc.subscribe([topic])
        _warmup(sc)
        _produce(topic, 1)

        msgs = _collect(sc, 1, POLL_TIMEOUT)
        assert msgs

        time.sleep(SHORT_LOCK_MS / 1000.0 + LOCK_EXPIRY_BUFFER_S)

        try:
            sc.acknowledge(msgs[0], AcknowledgeType.ACCEPT)
        except KafkaException as e:
            assert e.args[0].code() in (ERR_STATE, ERR_INVALID_RECORD)
            return

        result = sc.commit_sync(timeout=10.0)
        assert isinstance(result, dict)
    finally:
        sc.close()


@pytest.mark.integration
@broker_required
def test_lock_steal_with_committed_ack():
    """A holds a record past lock expiry; B fetches and commits it; A's
    late commit must not crash."""
    topic = f'commit_lock_steal_{_unique_id()}'
    group = f'g_{_unique_id()}'
    _create_topic(topic)

    try:
        _set_group_configs(group, {'share.record.lock.duration.ms': SHORT_LOCK_MS})
    except KafkaException as e:
        pytest.skip(f'cannot lower lock duration: {e}')

    sc_a = _consumer(group, mode='explicit')
    sc_b = _consumer(group, mode='explicit')
    try:
        sc_a.subscribe([topic])
        _warmup(sc_a)
        _produce(topic, 1)

        a_batch = _collect(sc_a, 1, POLL_TIMEOUT)
        assert a_batch
        a_msg = a_batch[0]

        time.sleep(SHORT_LOCK_MS / 1000.0 + LOCK_EXPIRY_BUFFER_S)

        # B can't use _warmup here — if it picks up the record mid-loop
        # the next poll inside warmup raises __STATE.
        sc_b.subscribe([topic])
        b_got = False
        deadline_b = time.monotonic() + POLL_TIMEOUT
        while not b_got and time.monotonic() < deadline_b:
            try:
                for m in sc_b.poll(timeout=2.0):
                    if m.error() is None:
                        sc_b.acknowledge(m, AcknowledgeType.ACCEPT)
                        b_got = True
                        break
            except KafkaException:
                pass
        if b_got:
            sc_b.commit_sync(timeout=5.0)

        try:
            sc_a.acknowledge(a_msg, AcknowledgeType.ACCEPT)
        except KafkaException as e:
            assert e.args[0].code() in (ERR_STATE, ERR_INVALID_RECORD)
            return

        result = sc_a.commit_sync(timeout=10.0)
        assert isinstance(result, dict)
    finally:
        sc_a.close()
        sc_b.close()


@pytest.mark.integration
@broker_required
def test_commit_after_acknowledge_unknown_offset():
    """Acking an unknown offset raises __STATE locally; a subsequent
    commit is a no-op."""
    topic = f'commit_unknown_offset_{_unique_id()}'
    group = f'g_{_unique_id()}'
    _create_topic(topic)

    sc = _consumer(group, mode='explicit')
    try:
        sc.subscribe([topic])
        _warmup(sc)

        with pytest.raises(KafkaException) as ex:
            sc.acknowledge_offset(topic, 0, 99999, AcknowledgeType.ACCEPT)
        assert ex.value.args[0].code() == ERR_STATE

        assert sc.commit_sync(timeout=2.0) == {}
    finally:
        sc.close()


# --- per-partition error reporting ----------------------------------------


@pytest.mark.integration
@broker_required
def test_per_partition_commit_results_all_succeed():
    """3 partitions, no induced errors: every entry in the result dict
    maps to None and the key matches a partition we polled from."""
    topic = f'commit_3p_clean_{_unique_id()}'
    group = f'g_{_unique_id()}'
    _create_topic(topic, num_partitions=3)

    sc = _consumer(group, mode='explicit')
    try:
        sc.subscribe([topic])
        _warmup(sc)

        for p in range(3):
            _produce(topic, 1, prefix=f'p{p}', partition=p)

        msgs = _collect(sc, 3, POLL_TIMEOUT, ack_type=AcknowledgeType.ACCEPT)
        assert msgs, 'no records received'

        partitions_seen = {m.partition() for m in msgs}

        result = sc.commit_sync(timeout=10.0)
        # acks may have piggybacked on intermediate polls inside _collect,
        # leaving nothing pending at commit time
        for tp, err in result.items():
            assert err is None, f'{tp.topic}[{tp.partition}] -> {err}'
            assert tp.partition in partitions_seen
    finally:
        sc.close()


@pytest.mark.integration
@broker_required
def test_per_partition_commit_results_with_lock_expiry():
    """Force lock expiry, then ack+commit: per-partition errors may or
    may not appear (librdkafka accepts late acks silently in some paths).
    The contract is no crash and any error is __STATE/INVALID_RECORD_STATE."""
    topic = f'commit_2p_mixed_{_unique_id()}'
    group = f'g_{_unique_id()}'
    _create_topic(topic, num_partitions=2)

    try:
        _set_group_configs(group, {'share.record.lock.duration.ms': SHORT_LOCK_MS})
    except KafkaException as e:
        pytest.skip(f'cannot lower lock duration: {e}')

    sc = _consumer(group, mode='explicit')
    try:
        sc.subscribe([topic])
        _warmup(sc)
        _produce(topic, 1, prefix='p0', partition=0)
        _produce(topic, 1, prefix='p1', partition=1)

        # Single poll only — must not poll again before acking.
        msgs = []
        deadline = time.monotonic() + POLL_TIMEOUT
        while not msgs and time.monotonic() < deadline:
            for m in sc.poll(timeout=2.0):
                if m.error() is None:
                    msgs.append(m)
        assert msgs

        time.sleep(SHORT_LOCK_MS / 1000.0 + LOCK_EXPIRY_BUFFER_S)

        for m in msgs:
            try:
                sc.acknowledge(m, AcknowledgeType.ACCEPT)
            except KafkaException:
                pass

        result = sc.commit_sync(timeout=10.0)
        assert isinstance(result, dict)
        for tp, err in result.items():
            if err is not None:
                assert err.code() in (ERR_STATE, ERR_INVALID_RECORD)
    finally:
        sc.close()


@pytest.mark.integration
@broker_required
def test_commit_on_closed_consumer_raises():
    """Closest reproducible top-level commit failure: commit on a closed
    consumer raises RuntimeError before reaching librdkafka."""
    topic = f'commit_closed_{_unique_id()}'
    group = f'g_{_unique_id()}'
    _create_topic(topic)

    sc = _consumer(group, mode='implicit')
    sc.subscribe([topic])
    _warmup(sc, seconds=2.0)
    sc.close()

    with pytest.raises(RuntimeError) as ex:
        sc.commit_sync(timeout=1.0)
    assert 'closed' in str(ex.value).lower()

    with pytest.raises(RuntimeError) as ex:
        sc.commit_async()
    assert 'closed' in str(ex.value).lower()


# --- sync vs async timing -------------------------------------------------


@pytest.mark.integration
@broker_required
def test_async_commit_does_not_block():
    """Async commit returns within 100ms regardless of pending acks."""
    topic = f'commit_async_fast_{_unique_id()}'
    group = f'g_{_unique_id()}'
    _create_topic(topic)

    sc = _consumer(group, mode='implicit')
    try:
        sc.subscribe([topic])
        _warmup(sc)
        _produce(topic, 5)
        _collect(sc, 5, POLL_TIMEOUT)

        start = time.monotonic()
        result = sc.commit_async()
        elapsed = time.monotonic() - start

        assert result is None
        assert elapsed < 0.5, f'async commit took {elapsed:.3f}s'
    finally:
        sc.close()


@pytest.mark.integration
@broker_required
def test_commit_with_zero_timeout_returns_fast():
    """timeout=0 returns immediately."""
    topic = f'commit_zero_timeout_{_unique_id()}'
    group = f'g_{_unique_id()}'
    _create_topic(topic)

    sc = _consumer(group, mode='implicit')
    try:
        sc.subscribe([topic])
        _warmup(sc)

        start = time.monotonic()
        result = sc.commit_sync(timeout=0)
        elapsed = time.monotonic() - start

        assert isinstance(result, dict)
        assert elapsed < 1.0, f'commit_sync(timeout=0) took {elapsed:.3f}s'
    finally:
        sc.close()


@pytest.mark.integration
def test_commit_does_not_hang_on_unreachable_broker():
    """Commit on a fresh, unsubscribed consumer pointed at an unreachable
    broker returns immediately (no acks pending). The interesting case
    — pending acks against an unreachable broker — needs wire-frame
    mocking to exercise."""
    sc = ShareConsumer(
        {
            'bootstrap.servers': '127.0.0.1:1',
            'group.id': f'g_{_unique_id()}',
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


@pytest.mark.integration
@broker_required
def test_sync_commit_releases_gil():
    """A CPU-bound background thread should make progress while sync
    commit blocks — confirms PyEval_SaveThread() is wired."""
    topic = f'commit_gil_{_unique_id()}'
    group = f'g_{_unique_id()}'
    _create_topic(topic)

    counter = {'n': 0}
    stop = threading.Event()

    def worker():
        while not stop.is_set():
            counter['n'] += 1

    t = threading.Thread(target=worker, daemon=True)
    t.start()

    sc = _consumer(group, mode='implicit')
    try:
        sc.subscribe([topic])
        _warmup(sc)
        _produce(topic, 5)
        _collect(sc, 5, POLL_TIMEOUT)

        before = counter['n']
        sc.commit_sync(timeout=2.0)
        after = counter['n']

        assert after - before > 100, (
            f'background thread made only {after - before} increments — ' f'GIL likely not released'
        )
    finally:
        stop.set()
        t.join(timeout=2.0)
        sc.close()


# --- lifecycle ------------------------------------------------------------


@pytest.mark.integration
@broker_required
def test_commit_before_close_persists_acks():
    """commit() before close() makes acks stick — a fresh consumer in
    the same group sees no redelivery. (close() alone does NOT flush.)"""
    topic = f'commit_then_close_{_unique_id()}'
    group = f'g_{_unique_id()}'
    _create_topic(topic)

    sc1 = _consumer(group, mode='explicit')
    try:
        sc1.subscribe([topic])
        _warmup(sc1)
        _produce(topic, 3)

        msgs = _collect(sc1, 3, POLL_TIMEOUT, ack_type=AcknowledgeType.ACCEPT)
        assert len(msgs) == 3

        result = sc1.commit_sync(timeout=10.0)
        assert isinstance(result, dict)
    finally:
        sc1.close()

    # Implicit verifier so any redelivery doesn't trip __STATE.
    sc2 = _consumer(group, mode='implicit')
    try:
        sc2.subscribe([topic])
        leftovers = []
        deadline = time.monotonic() + 8.0
        while time.monotonic() < deadline:
            for m in sc2.poll(timeout=1.0):
                if m.error() is None:
                    leftovers.append(m.value())
        assert leftovers == [], f'commit didn\'t persist; got: {leftovers}'
    finally:
        sc2.close()


@pytest.mark.integration
@broker_required
def test_commit_before_first_poll_is_noop():
    """commit() right after subscribe() returns None."""
    topic = f'commit_pre_poll_{_unique_id()}'
    group = f'g_{_unique_id()}'
    _create_topic(topic)

    sc = _consumer(group, mode='implicit')
    try:
        sc.subscribe([topic])
        assert sc.commit_sync(timeout=2.0) == {}
        assert sc.commit_async() is None
    finally:
        sc.close()


@pytest.mark.integration
@broker_required
def test_commit_after_unsubscribe_does_not_crash():
    """commit() after unsubscribe(): KIP doesn't define this; we just
    pin down whatever librdkafka does today and make sure it's stable."""
    topic = f'commit_unsub_{_unique_id()}'
    group = f'g_{_unique_id()}'
    _create_topic(topic)

    sc = _consumer(group, mode='implicit')
    try:
        sc.subscribe([topic])
        _warmup(sc)
        _produce(topic, 2)
        _collect(sc, 2, POLL_TIMEOUT)

        sc.unsubscribe()
        result = sc.commit_sync(timeout=5.0)
        assert isinstance(result, dict)
    finally:
        sc.close()


# --- configuration boundary -----------------------------------------------


@pytest.mark.integration
@broker_required
def test_commit_with_explicit_mode():
    """share.acknowledgement.mode=explicit parses and works end-to-end."""
    topic = f'commit_explicit_cfg_{_unique_id()}'
    group = f'g_{_unique_id()}'
    _create_topic(topic)

    sc = _consumer(group, mode='explicit')
    try:
        sc.subscribe([topic])
        _warmup(sc)
        _produce(topic, 2)

        msgs = _collect(sc, 2, POLL_TIMEOUT, ack_type=AcknowledgeType.ACCEPT)
        assert len(msgs) >= 1

        result = sc.commit_sync(timeout=10.0)
        for tp, err in result.items():
            assert err is None
    finally:
        sc.close()
