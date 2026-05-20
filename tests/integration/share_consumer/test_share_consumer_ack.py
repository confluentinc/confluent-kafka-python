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

"""Integration tests for ShareConsumer acknowledgement (KIP-932)."""

import time

import pytest

from confluent_kafka import AcknowledgeType, KafkaError, KafkaException, Producer
from confluent_kafka.admin import AlterConfigOpType, ConfigEntry, ConfigResource
from tests.common import drain_share_consumers, unique_id


def _set_group_config(kafka_cluster, group_id, name, value):
    """Set one dynamic share-group config."""
    res = ConfigResource(
        ConfigResource.Type.GROUP,
        group_id,
        incremental_configs=[
            ConfigEntry(name, str(value), incremental_operation=AlterConfigOpType.SET),
        ],
    )
    for f in kafka_cluster.admin().incremental_alter_configs([res]).values():
        f.result()


# TODO KIP-932: these tests verify ack success indirectly — by opening a
# second consumer in the same share group and asserting no redelivery.
# Once the per-record acknowledgement callback is exposed through the
# Python binding add direct success/failure assertions on the callback instead of relying
# on the no-redelivery side-channel.

# --- implicit mode ---------------------------------------------------------


def test_implicit_mode_acknowledge_raises(kafka_cluster):
    """acknowledge() in implicit mode must raise _STATE."""
    topic = kafka_cluster.create_topic_and_wait_propogation('test-share-consumer-ack-implicit-raises')

    sc = kafka_cluster.share_consumer({'share.acknowledgement.mode': 'implicit'})
    try:
        sc.subscribe([topic])

        producer = kafka_cluster.cimpl_producer()
        producer.produce(topic, value=b'msg-0')
        producer.flush(timeout=10.0)

        msgs = drain_share_consumers([sc], 1)[0]
        assert msgs, 'no messages received'

        with pytest.raises(KafkaException) as ex:
            sc.acknowledge(msgs[0], AcknowledgeType.ACCEPT)
        assert ex.value.args[0].code() == KafkaError._STATE
    finally:
        sc.close()


def test_implicit_mode_autocommits_on_next_poll(kafka_cluster):
    """Next poll() in implicit mode commits the previous batch."""
    topic = kafka_cluster.create_topic_and_wait_propogation('test-share-consumer-ack-implicit-autocommit')
    group_id = unique_id('test-share-consumer-ack-implicit-autocommit')
    n = 3

    sc1 = kafka_cluster.share_consumer({'group.id': group_id, 'share.acknowledgement.mode': 'implicit'})
    try:
        sc1.subscribe([topic])

        producer = kafka_cluster.cimpl_producer()
        produced = [f'msg-{i}'.encode() for i in range(n)]
        for v in produced:
            producer.produce(topic, value=v)
        producer.flush(timeout=10.0)

        first = drain_share_consumers([sc1], n)[0]
        assert len(first) == n

        sc1.poll(timeout=2.0)  # implicit-ack the prior batch
    finally:
        sc1.close()

    sc2 = kafka_cluster.share_consumer({'group.id': group_id, 'share.acknowledgement.mode': 'implicit'})
    try:
        sc2.subscribe([topic])
        leftovers = drain_share_consumers([sc2], 1, timeout_s=5.0)[0]
        assert leftovers == [], f'expected no redelivery, got {[m.value() for m in leftovers]} (produced {produced})'
    finally:
        sc2.close()


# --- explicit mode: ACCEPT / REJECT / RELEASE ------------------------------


def test_explicit_accept_prevents_redelivery(kafka_cluster):
    """ACCEPT means done — no redelivery."""
    topic = kafka_cluster.create_topic_and_wait_propogation('test-share-consumer-ack-explicit-accept')
    group_id = unique_id('test-share-consumer-ack-explicit-accept')
    n = 3

    sc1 = kafka_cluster.share_consumer({'group.id': group_id, 'share.acknowledgement.mode': 'explicit'})
    try:
        sc1.subscribe([topic])

        producer = kafka_cluster.cimpl_producer()
        for i in range(n):
            producer.produce(topic, value=f'msg-{i}'.encode())
        producer.flush(timeout=10.0)

        msgs = drain_share_consumers([sc1], n, ack_type=AcknowledgeType.ACCEPT)[0]
        assert len(msgs) == n

        sc1.poll(timeout=2.0)  # flush ACCEPTs
    finally:
        sc1.close()

    sc2 = kafka_cluster.share_consumer({'group.id': group_id, 'share.acknowledgement.mode': 'explicit'})
    try:
        sc2.subscribe([topic])
        leftovers = drain_share_consumers([sc2], 1, timeout_s=5.0, ack_type=AcknowledgeType.ACCEPT)[0]
        assert leftovers == [], 'ACCEPT should prevent redelivery'
    finally:
        sc2.close()


def test_explicit_reject_prevents_redelivery(kafka_cluster):
    """REJECT archives the record (poison-pill)."""
    topic = kafka_cluster.create_topic_and_wait_propogation('test-share-consumer-ack-explicit-reject')
    group_id = unique_id('test-share-consumer-ack-explicit-reject')
    n = 3

    sc1 = kafka_cluster.share_consumer({'group.id': group_id, 'share.acknowledgement.mode': 'explicit'})
    try:
        sc1.subscribe([topic])

        producer = kafka_cluster.cimpl_producer()
        for i in range(n):
            producer.produce(topic, value=f'msg-{i}'.encode())
        producer.flush(timeout=10.0)

        msgs = drain_share_consumers([sc1], n, ack_type=AcknowledgeType.REJECT)[0]
        assert len(msgs) == n

        sc1.poll(timeout=2.0)  # flush REJECTs
    finally:
        sc1.close()

    sc2 = kafka_cluster.share_consumer({'group.id': group_id, 'share.acknowledgement.mode': 'explicit'})
    try:
        sc2.subscribe([topic])
        leftovers = drain_share_consumers([sc2], 1, timeout_s=5.0, ack_type=AcknowledgeType.ACCEPT)[0]
        assert leftovers == [], 'REJECT should permanently archive'
    finally:
        sc2.close()


def test_explicit_release_causes_redelivery(kafka_cluster):
    """RELEASE puts the record back; same offset comes around again."""
    topic = kafka_cluster.create_topic_and_wait_propogation('test-share-consumer-ack-explicit-release')

    sc = kafka_cluster.share_consumer({'share.acknowledgement.mode': 'explicit'})
    try:
        sc.subscribe([topic])

        producer = kafka_cluster.cimpl_producer()
        producer.produce(topic, value=b'msg-0')
        producer.flush(timeout=10.0)

        first_batch = drain_share_consumers([sc], 1)[0]
        assert first_batch
        first = first_batch[0]
        coords = (first.topic(), first.partition(), first.offset())

        sc.acknowledge(first, AcknowledgeType.RELEASE)

        deadline = time.time() + 15.0
        seen_again = False
        while time.time() < deadline and not seen_again:
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


def test_unacked_records_block_next_poll(kafka_cluster):
    """Unacked records in explicit mode → next poll raises _STATE.
    Acking clears the block."""
    topic = kafka_cluster.create_topic_and_wait_propogation('test-share-consumer-ack-explicit-unacked')

    sc = kafka_cluster.share_consumer({'share.acknowledgement.mode': 'explicit'})
    try:
        sc.subscribe([topic])

        producer = kafka_cluster.cimpl_producer()
        producer.produce(topic, value=b'msg-0')
        producer.flush(timeout=10.0)

        # Get one record, don't ack it.
        first_batch = []
        deadline = time.time() + 20.0
        while not first_batch and time.time() < deadline:
            for m in sc.poll(timeout=2.0):
                if m.error() is None:
                    first_batch.append(m)
        assert first_batch, 'never received any messages'

        with pytest.raises(KafkaException) as ex:
            sc.poll(timeout=2.0)
        err = ex.value.args[0]
        assert err.code() == KafkaError._STATE
        assert 'not been acknowledged' in err.str()

        # Recover: ack then poll.
        for m in first_batch:
            sc.acknowledge(m, AcknowledgeType.ACCEPT)
        sc.poll(timeout=2.0)
    finally:
        sc.close()


# --- delivery limit, atomicity, transactions ------------------------------

# TODO KIP-932: Add a test once the per-record ack callback is exposed
# through the Python binding.


def test_delivery_attempt_limit_archives_record(kafka_cluster):
    """A record RELEASEd `delivery.attempt.limit` times (default 5) is archived."""
    topic = kafka_cluster.create_topic_and_wait_propogation('test-share-consumer-ack-poison')

    sc = kafka_cluster.share_consumer({'share.acknowledgement.mode': 'explicit'})
    try:
        sc.subscribe([topic])

        producer = kafka_cluster.cimpl_producer()
        producer.produce(topic, value=b'poison-0')
        producer.flush(timeout=10.0)

        coords = None
        releases = 0
        deadline = time.time() + 60.0
        while releases < 5 and time.time() < deadline:
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

        # Watch for ~10s — must not redeliver.
        deadline = time.time() + 10.0
        while time.time() < deadline:
            for m in sc.poll(timeout=2.0):
                if m.error() is None and (m.topic(), m.partition(), m.offset()) == coords:
                    pytest.fail(f'record {coords} redelivered past delivery.attempt.limit')
    finally:
        sc.close()


def test_open_transaction_stalls_share_group(kafka_cluster):
    """read_committed: open txn blocks delivery until commit."""
    topic = kafka_cluster.create_topic_and_wait_propogation('test-share-consumer-ack-txn-stall')
    group_id = unique_id('test-share-consumer-ack-txn-stall')

    txn_producer = Producer(kafka_cluster.client_conf({'transactional.id': unique_id('txn')}))
    try:
        txn_producer.init_transactions(10)
    except KafkaException as e:
        pytest.skip(f'broker does not support transactions: {e}')

    try:
        _set_group_config(kafka_cluster, group_id, 'share.isolation.level', 'read_committed')
    except KafkaException as e:
        pytest.skip(f'cannot set share.isolation.level on group: {e}')

    sc = kafka_cluster.share_consumer({'group.id': group_id, 'share.acknowledgement.mode': 'explicit'})
    try:
        sc.subscribe([topic])

        # Produce inside an uncommitted txn.
        txn_producer.begin_transaction()
        for i in range(3):
            txn_producer.produce(topic, value=f'txn-{i}'.encode())
        txn_producer.flush(5)

        # Open txn must stall delivery.
        stalled = drain_share_consumers([sc], 1, timeout_s=5.0)[0]
        assert stalled == [], f'open txn did not stall delivery: {[m.value() for m in stalled]}'

        txn_producer.commit_transaction(10)

        msgs = drain_share_consumers([sc], 3, ack_type=AcknowledgeType.ACCEPT)[0]
        assert len(msgs) == 3, f'expected 3 msgs after commit, got {len(msgs)}'
    finally:
        sc.close()


# --- acknowledge_offset() coverage ----------------------------------------


def test_acknowledge_offset_happy_path(kafka_cluster):
    """acknowledge_offset() advances state identically to acknowledge(msg)."""
    topic = kafka_cluster.create_topic_and_wait_propogation('test-share-consumer-ack-offset-happy')
    group_id = unique_id('test-share-consumer-ack-offset-happy')
    n = 3

    sc1 = kafka_cluster.share_consumer({'group.id': group_id, 'share.acknowledgement.mode': 'explicit'})
    try:
        sc1.subscribe([topic])

        producer = kafka_cluster.cimpl_producer()
        for i in range(n):
            producer.produce(topic, value=f'msg-{i}'.encode())
        producer.flush(timeout=10.0)

        seen = 0
        deadline = time.time() + 20.0
        while time.time() < deadline and seen < n:
            for m in sc1.poll(timeout=0.5):
                if m.error() is None:
                    sc1.acknowledge_offset(
                        m.topic(),
                        m.partition(),
                        m.offset(),
                        AcknowledgeType.ACCEPT,
                    )
                    seen += 1
        assert seen == n, f'expected to ack {n} records, acked {seen}'

        sc1.poll(timeout=2.0)  # flush ACCEPTs
    finally:
        sc1.close()

    sc2 = kafka_cluster.share_consumer({'group.id': group_id, 'share.acknowledgement.mode': 'explicit'})
    try:
        sc2.subscribe([topic])
        leftovers = drain_share_consumers([sc2], 1, timeout_s=5.0, ack_type=AcknowledgeType.ACCEPT)[0]
        assert leftovers == [], 'acknowledge_offset(ACCEPT) should prevent redelivery'
    finally:
        sc2.close()


def test_acknowledge_offset_invalid_coords_raise(kafka_cluster):
    """Bogus coords raise KafkaException; the consumer stays usable.

    There are two failure paths:
      - Structurally invalid args (NULL topic, negative partition/offset)
        are rejected by the front-door check in librdkafka with _INVALID_ARG.
      - Well-formed coords that the consumer never fetched (unsubscribed
        topic, out-of-range partition, out-of-range offset) fall through
        to the inflight-map lookup in librdkafka with _STATE.
    """
    topic = kafka_cluster.create_topic_and_wait_propogation('test-share-consumer-ack-offset-invalid')

    sc = kafka_cluster.share_consumer({'share.acknowledgement.mode': 'explicit'})
    try:
        sc.subscribe([topic])

        producer = kafka_cluster.cimpl_producer()
        producer.produce(topic, value=b'msg-0')
        producer.flush(timeout=10.0)

        msgs = []
        deadline = time.time() + 20.0
        while time.time() < deadline and not msgs:
            for m in sc.poll(timeout=0.5):
                if m.error() is None:
                    msgs.append(m)
            if msgs:
                break
        assert msgs, 'never received the produced record'
        m = msgs[0]

        with pytest.raises(KafkaException) as ex:
            sc.acknowledge_offset(
                'no-such-topic-' + unique_id(''),
                m.partition(),
                m.offset(),
                AcknowledgeType.ACCEPT,
            )
        assert ex.value.args[0].code() == KafkaError._STATE

        with pytest.raises(KafkaException) as ex:
            sc.acknowledge_offset(
                m.topic(),
                m.partition() + 99,
                m.offset(),
                AcknowledgeType.ACCEPT,
            )
        assert ex.value.args[0].code() == KafkaError._STATE

        with pytest.raises(KafkaException) as ex:
            sc.acknowledge_offset(
                m.topic(),
                m.partition(),
                m.offset() + 99_999,
                AcknowledgeType.ACCEPT,
            )
        assert ex.value.args[0].code() == KafkaError._STATE

        # Structurally invalid: negative partition hits the front-door
        # check before any inflight-map lookup, so _INVALID_ARG
        with pytest.raises(KafkaException) as ex:
            sc.acknowledge_offset(
                m.topic(),
                -1,
                m.offset(),
                AcknowledgeType.ACCEPT,
            )
        assert ex.value.args[0].code() == KafkaError._INVALID_ARG

        # Real record must still be ackable.
        sc.acknowledge(m, AcknowledgeType.ACCEPT)
    finally:
        sc.close()


# --- ack-type combinations and ordering -----------------------------------


def test_mixed_ack_types_in_single_batch(kafka_cluster):
    """ACCEPT + RELEASE + REJECT in one batch: only the RELEASEd one redelivers."""
    topic = kafka_cluster.create_topic_and_wait_propogation('test-share-consumer-ack-mixed')
    n = 3

    sc = kafka_cluster.share_consumer({'share.acknowledgement.mode': 'explicit'})
    try:
        sc.subscribe([topic])

        producer = kafka_cluster.cimpl_producer()
        for i in range(n):
            producer.produce(topic, value=f'msg-{i}'.encode())
        producer.flush(timeout=10.0)

        # Need all 3 in one Acquired set before acking — if librdkafka splits
        # the batch the len() assertion below trips.
        batch = []
        deadline = time.time() + 20.0
        while time.time() < deadline and len(batch) < n:
            for m in sc.poll(timeout=1.0):
                if m.error() is None:
                    batch.append(m)
            if len(batch) >= n:
                break
        assert len(batch) == n, f'expected {n} in a single Acquired set, got {len(batch)}'

        batch.sort(key=lambda x: (x.partition(), x.offset()))
        sc.acknowledge(batch[0], AcknowledgeType.ACCEPT)
        sc.acknowledge(batch[1], AcknowledgeType.RELEASE)
        sc.acknowledge(batch[2], AcknowledgeType.REJECT)
        released_coords = (batch[1].topic(), batch[1].partition(), batch[1].offset())

        seen = set()
        deadline = time.time() + 15.0
        while time.time() < deadline and not seen:
            for m in sc.poll(timeout=0.5):
                if m.error() is None:
                    seen.add((m.topic(), m.partition(), m.offset()))
                    sc.acknowledge(m, AcknowledgeType.ACCEPT)
        assert seen == {released_coords}, f'expected only {released_coords}, got {seen}'
    finally:
        sc.close()


def test_double_ack_same_record_is_lenient(kafka_cluster):
    """Double-acking the same record is a silent no-op; next poll must not raise _STATE."""
    topic = kafka_cluster.create_topic_and_wait_propogation('test-share-consumer-ack-double')

    sc = kafka_cluster.share_consumer({'share.acknowledgement.mode': 'explicit'})
    try:
        sc.subscribe([topic])

        producer = kafka_cluster.cimpl_producer()
        producer.produce(topic, value=b'msg-0')
        producer.flush(timeout=10.0)

        msgs = []
        deadline = time.time() + 20.0
        while time.time() < deadline and not msgs:
            for m in sc.poll(timeout=0.5):
                if m.error() is None:
                    msgs.append(m)
            if msgs:
                break
        assert msgs, 'never received the produced record'
        m = msgs[0]

        sc.acknowledge(m, AcknowledgeType.ACCEPT)
        sc.acknowledge(m, AcknowledgeType.ACCEPT)  # silent no-op
        sc.poll(timeout=1.0)  # must not raise _STATE
    finally:
        sc.close()


def test_ack_offset_from_prior_batch_raises(kafka_cluster):
    """Acking an offset from a prior, already-flushed batch raises _STATE."""
    topic = kafka_cluster.create_topic_and_wait_propogation('test-share-consumer-ack-prior-batch')
    n_first = 3

    sc = kafka_cluster.share_consumer({'share.acknowledgement.mode': 'explicit'})
    try:
        sc.subscribe([topic])

        producer = kafka_cluster.cimpl_producer()
        for i in range(n_first):
            producer.produce(topic, value=f'first-{i}'.encode())
        producer.flush(timeout=10.0)

        first_batch = []
        deadline = time.time() + 20.0
        while time.time() < deadline and len(first_batch) < n_first:
            for m in sc.poll(timeout=0.5):
                if m.error() is None:
                    first_batch.append(m)
                    sc.acknowledge(m, AcknowledgeType.ACCEPT)
            if len(first_batch) >= n_first:
                break
        assert len(first_batch) == n_first
        stale = first_batch[0]
        stale_coords = (stale.topic(), stale.partition(), stale.offset())

        # Move past that batch.
        producer.produce(topic, value=b'second-0')
        producer.flush(timeout=10.0)

        second_batch = []
        deadline = time.time() + 20.0
        while time.time() < deadline and not second_batch:
            for m in sc.poll(timeout=0.5):
                if m.error() is None:
                    second_batch.append(m)
            if second_batch:
                break
        assert second_batch, 'never received second-batch record'

        with pytest.raises(KafkaException) as ex:
            sc.acknowledge_offset(*stale_coords, AcknowledgeType.ACCEPT)
        assert ex.value.args[0].code() == KafkaError._STATE

        # Ack the live batch so close() doesn't trip.
        for m in second_batch:
            sc.acknowledge(m, AcknowledgeType.ACCEPT)
    finally:
        sc.close()


def test_out_of_order_acks_within_batch(kafka_cluster):
    """Acks within one batch may arrive in any order."""
    topic = kafka_cluster.create_topic_and_wait_propogation('test-share-consumer-ack-out-of-order')
    n = 5

    sc = kafka_cluster.share_consumer({'share.acknowledgement.mode': 'explicit'})
    try:
        sc.subscribe([topic])

        producer = kafka_cluster.cimpl_producer()
        for i in range(n):
            producer.produce(topic, value=f'msg-{i}'.encode())
        producer.flush(timeout=10.0)

        batch = []
        deadline = time.time() + 20.0
        while time.time() < deadline and len(batch) < n:
            for m in sc.poll(timeout=1.0):
                if m.error() is None:
                    batch.append(m)
            if len(batch) >= n:
                break
        assert len(batch) == n, f'expected {n} in one Acquired set, got {len(batch)}'

        # Highest offset first per partition.
        by_partition = {}
        for m in batch:
            by_partition.setdefault(m.partition(), []).append(m)
        for part_msgs in by_partition.values():
            part_msgs.sort(key=lambda x: x.offset(), reverse=True)
            for m in part_msgs:
                sc.acknowledge(m, AcknowledgeType.ACCEPT)

        sc.poll(timeout=2.0)  # would raise _STATE if any ack was dropped
    finally:
        sc.close()


# --- lifecycle and contract edges -----------------------------------------


def test_ack_after_unsubscribe_does_not_crash(kafka_cluster):
    """ack() after unsubscribe() must not crash; consumer must stay usable."""
    topic1 = kafka_cluster.create_topic_and_wait_propogation('test-share-consumer-ack-after-unsub-1')
    topic2 = kafka_cluster.create_topic_and_wait_propogation('test-share-consumer-ack-after-unsub-2')

    sc = kafka_cluster.share_consumer({'share.acknowledgement.mode': 'explicit'})
    try:
        sc.subscribe([topic1])

        producer = kafka_cluster.cimpl_producer()
        producer.produce(topic1, value=b'msg-pre-unsub')
        producer.flush(timeout=10.0)

        msgs = []
        deadline = time.time() + 20.0
        while time.time() < deadline and not msgs:
            for m in sc.poll(timeout=0.5):
                if m.error() is None:
                    msgs.append(m)
            if msgs:
                break
        assert msgs, 'never received the pre-unsubscribe record'

        sc.unsubscribe()

        # KafkaException or no-op both acceptable; segfault is not.
        try:
            sc.acknowledge(msgs[0], AcknowledgeType.ACCEPT)
        except KafkaException:
            pass

        sc.subscribe([topic2])
        producer.produce(topic2, value=b'msg-post-resub')
        producer.flush(timeout=10.0)

        seen = []
        deadline = time.time() + 20.0
        while time.time() < deadline and not seen:
            for m in sc.poll(timeout=0.5):
                if m.error() is None:
                    seen.append(m)
                    sc.acknowledge(m, AcknowledgeType.ACCEPT)
            if seen:
                break
        assert seen, 'consumer unusable after unsubscribe + stale ack'
    finally:
        sc.close()


def test_double_close_is_idempotent(kafka_cluster):
    """close() twice must be a no-op (__exit__ relies on this)."""
    sc = kafka_cluster.share_consumer()
    sc.close()
    sc.close()


def test_acknowledge_rejects_non_message_argument(kafka_cluster):
    """Non-Message arg raises TypeError at the binding (PyArg O!)."""
    sc = kafka_cluster.share_consumer({'share.acknowledgement.mode': 'explicit'})
    try:
        for bad in (None, 'not-a-message', 42, object()):
            with pytest.raises(TypeError):
                sc.acknowledge(bad, AcknowledgeType.ACCEPT)
    finally:
        sc.close()


def test_partial_ack_still_blocks_next_poll(kafka_cluster):
    """Acking a subset of an Acquired set still triggers _STATE on next poll."""
    topic = kafka_cluster.create_topic_and_wait_propogation('test-share-consumer-ack-partial')
    n = 5

    sc = kafka_cluster.share_consumer({'share.acknowledgement.mode': 'explicit'})
    try:
        sc.subscribe([topic])

        producer = kafka_cluster.cimpl_producer()
        for i in range(n):
            producer.produce(topic, value=f'msg-{i}'.encode())
        producer.flush(timeout=10.0)

        batch = []
        deadline = time.time() + 20.0
        while time.time() < deadline and len(batch) < n:
            for m in sc.poll(timeout=1.0):
                if m.error() is None:
                    batch.append(m)
            if len(batch) >= n:
                break
        assert len(batch) == n, f'expected {n} in one Acquired set, got {len(batch)}'

        for m in batch[:3]:
            sc.acknowledge(m, AcknowledgeType.ACCEPT)

        with pytest.raises(KafkaException) as ex:
            sc.poll(timeout=2.0)
        err = ex.value.args[0]
        assert err.code() == KafkaError._STATE
        assert 'not been acknowledged' in err.str()

        # Ack the rest; poll should be clean.
        for m in batch[3:]:
            sc.acknowledge(m, AcknowledgeType.ACCEPT)
        sc.poll(timeout=2.0)
    finally:
        sc.close()
