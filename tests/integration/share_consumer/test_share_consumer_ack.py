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

from confluent_kafka import AcknowledgeType, KafkaError, KafkaException
from tests.common import drain_share_consumers, poll_first_batch, unique_id

# TODO KIP-932: these tests verify ack success indirectly — by opening a
# second consumer in the same share group and asserting no redelivery.
# Once the per-record acknowledgement callback is exposed through the
# Python binding add direct success/failure assertions on the callback instead of relying
# on the no-redelivery side-channel.

# TODO KIP-932: add unit tests (alongside integration tests) that exercise
# type and value validation of acknowledge() / acknowledge_offset() input
# parameters — wrong arg types, out-of-range ack_type values, negative
# partition/offset, etc. Land these with the commit_sync / commit_async PR
# that introduces the equivalent input-validation surface.

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
    """Next poll() in implicit mode commits the previous batch.

    The implicit-ack chain works like this: the first poll() returns a batch
    that is held as 'inflight'; the next poll() acks that batch and may
    return more records; and so on. To verify the chain ack'd every record,
    we capture the first batch explicitly (before any ack), drain the
    remaining records, and assert that a second consumer joining the same
    group sees no redelivery.
    """
    topic = kafka_cluster.create_topic_and_wait_propogation('test-share-consumer-ack-implicit-autocommit')
    group_id = unique_id('test-share-consumer-ack-implicit-autocommit')
    num_messages = 3

    sc1 = kafka_cluster.share_consumer({'group.id': group_id, 'share.acknowledgement.mode': 'implicit'})
    try:
        sc1.subscribe([topic])

        producer = kafka_cluster.cimpl_producer()
        produced = [f'msg-{i}'.encode() for i in range(num_messages)]
        for value in produced:
            producer.produce(topic, value=value)
        producer.flush(timeout=10.0)

        # Capture the first batch (x records) before any ack happens.
        first_batch = poll_first_batch(sc1)
        assert first_batch, 'broker delivered nothing on first poll'

        # Subsequent polls implicit-ack the prior batch; drain the rest
        # (num_messages - x records) so the total adds up.
        remaining = drain_share_consumers([sc1], num_messages - len(first_batch), timeout_s=10.0)[0]

        received_values = {m.value() for m in first_batch + remaining}
        assert received_values == set(
            produced
        ), f'sc1 missed records: produced={set(produced)} received={received_values}'

        # Final poll acks the tail batch returned by drain.
        sc1.poll(timeout=2.0)
    finally:
        sc1.close()

    sc2 = kafka_cluster.share_consumer({'group.id': group_id, 'share.acknowledgement.mode': 'implicit'})
    try:
        sc2.subscribe([topic])
        leftovers = drain_share_consumers([sc2], 1, timeout_s=5.0)[0]
        assert leftovers == [], (
            f'sc1 ack-chain should have acked all {num_messages} records; '
            f'sc2 unexpectedly received {[m.value() for m in leftovers]} (produced {produced})'
        )
    finally:
        sc2.close()


# --- explicit mode: ACCEPT / REJECT / RELEASE ------------------------------


def test_explicit_accept_prevents_redelivery(kafka_cluster):
    """ACCEPT means done — no redelivery."""
    topic = kafka_cluster.create_topic_and_wait_propogation('test-share-consumer-ack-explicit-accept')
    group_id = unique_id('test-share-consumer-ack-explicit-accept')
    num_messages = 3

    sc1 = kafka_cluster.share_consumer({'group.id': group_id, 'share.acknowledgement.mode': 'explicit'})
    try:
        sc1.subscribe([topic])

        producer = kafka_cluster.cimpl_producer()
        produced = [f'msg-{i}'.encode() for i in range(num_messages)]
        for value in produced:
            producer.produce(topic, value=value)
        producer.flush(timeout=10.0)

        msgs = drain_share_consumers([sc1], num_messages, ack_type=AcknowledgeType.ACCEPT)[0]
        received_values = {m.value() for m in msgs}
        assert received_values == set(
            produced
        ), f'sc1 missed records: produced={set(produced)} received={received_values}'
    finally:
        # close() runs rd_kafka_share_consumer_close(), which sends pending
        # ack requests to the broker before destroying the consumer.
        sc1.close()

    sc2 = kafka_cluster.share_consumer({'group.id': group_id, 'share.acknowledgement.mode': 'explicit'})
    try:
        sc2.subscribe([topic])
        leftovers = drain_share_consumers([sc2], 1, timeout_s=5.0, ack_type=AcknowledgeType.ACCEPT)[0]
        assert leftovers == [], (
            f'ACCEPT should prevent redelivery; sc2 unexpectedly received '
            f'{[m.value() for m in leftovers]} (produced {produced})'
        )
    finally:
        sc2.close()


def test_explicit_reject_prevents_redelivery(kafka_cluster):
    """REJECT archives the record (poison-pill)."""
    topic = kafka_cluster.create_topic_and_wait_propogation('test-share-consumer-ack-explicit-reject')
    group_id = unique_id('test-share-consumer-ack-explicit-reject')
    num_messages = 3

    sc1 = kafka_cluster.share_consumer({'group.id': group_id, 'share.acknowledgement.mode': 'explicit'})
    try:
        sc1.subscribe([topic])

        producer = kafka_cluster.cimpl_producer()
        produced = [f'msg-{i}'.encode() for i in range(num_messages)]
        for value in produced:
            producer.produce(topic, value=value)
        producer.flush(timeout=10.0)

        msgs = drain_share_consumers([sc1], num_messages, ack_type=AcknowledgeType.REJECT)[0]
        received_values = {m.value() for m in msgs}
        assert received_values == set(
            produced
        ), f'sc1 missed records: produced={set(produced)} received={received_values}'
    finally:
        # close() flushes pending acks via rd_kafka_share_consumer_close().
        sc1.close()

    sc2 = kafka_cluster.share_consumer({'group.id': group_id, 'share.acknowledgement.mode': 'explicit'})
    try:
        sc2.subscribe([topic])
        leftovers = drain_share_consumers([sc2], 1, timeout_s=5.0, ack_type=AcknowledgeType.ACCEPT)[0]
        assert leftovers == [], (
            f'REJECT should permanently archive; sc2 unexpectedly received '
            f'{[m.value() for m in leftovers]} (produced {produced})'
        )
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
        released_coords = (first.topic(), first.partition(), first.offset())

        sc.acknowledge(first, AcknowledgeType.RELEASE)

        deadline = time.time() + 15.0
        seen_again = False
        while time.time() < deadline and not seen_again:
            for m in sc.poll(timeout=2.0):
                if m.error() is None:
                    if (m.topic(), m.partition(), m.offset()) == released_coords:
                        seen_again = True
                    sc.acknowledge(m, AcknowledgeType.ACCEPT)
                    if seen_again:
                        break
        assert seen_again, f'released record {released_coords} never came back'
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
        assert ex.value.args[0].code() == KafkaError._STATE

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

        poison_coords = None
        releases = 0
        deadline = time.time() + 60.0
        while releases < 5 and time.time() < deadline:
            for m in sc.poll(timeout=2.0):
                if m.error() is None:
                    if poison_coords is None:
                        poison_coords = (m.topic(), m.partition(), m.offset())
                    sc.acknowledge(m, AcknowledgeType.RELEASE)
                    releases += 1
                    if releases >= 5:
                        break
        assert releases >= 5, f'only saw {releases} releases, expected 5'
        assert poison_coords is not None

        # Flush the final RELEASE.
        try:
            sc.poll(timeout=2.0)
        except KafkaException:
            pass

        # The acquisition lock (group.share.record.lock.duration.ms=1000 in
        # test config) prevents same-consumer redelivery within ~1s of the
        # last release. Wait past that window before checking, so any
        # redelivery the broker would do has a chance to surface.
        time.sleep(2.0)

        # Collect everything received during the watch window and assert
        # explicitly that the poison record was not redelivered.
        seen_after_archive = []
        deadline = time.time() + 10.0
        while time.time() < deadline:
            for m in sc.poll(timeout=2.0):
                if m.error() is None:
                    seen_after_archive.append((m.topic(), m.partition(), m.offset()))
                    sc.acknowledge(m, AcknowledgeType.ACCEPT)
        assert poison_coords not in seen_after_archive, (
            f'record {poison_coords} redelivered past delivery.attempt.limit; '
            f'records seen during watch window: {seen_after_archive}'
        )
    finally:
        sc.close()


# --- acknowledge_offset() coverage ----------------------------------------


def test_acknowledge_offset_happy_path(kafka_cluster):
    """acknowledge_offset() advances state identically to acknowledge(msg)."""
    topic = kafka_cluster.create_topic_and_wait_propogation('test-share-consumer-ack-offset-happy')
    group_id = unique_id('test-share-consumer-ack-offset-happy')
    num_messages = 3

    sc1 = kafka_cluster.share_consumer({'group.id': group_id, 'share.acknowledgement.mode': 'explicit'})
    try:
        sc1.subscribe([topic])

        producer = kafka_cluster.cimpl_producer()
        produced = [f'msg-{i}'.encode() for i in range(num_messages)]
        for value in produced:
            producer.produce(topic, value=value)
        producer.flush(timeout=10.0)

        # Use acknowledge_offset() (not drain_share_consumers, which goes
        # through acknowledge(message, ...)) — that's the API under test here.
        received = []
        deadline = time.time() + 20.0
        while time.time() < deadline and len(received) < num_messages:
            for m in sc1.poll(timeout=0.5):
                if m.error() is None:
                    sc1.acknowledge_offset(
                        m.topic(),
                        m.partition(),
                        m.offset(),
                        AcknowledgeType.ACCEPT,
                    )
                    received.append(m)

        received_values = {m.value() for m in received}
        assert received_values == set(
            produced
        ), f'sc1 missed records: produced={set(produced)} received={received_values}'
    finally:
        # close() flushes pending acks via rd_kafka_share_consumer_close().
        sc1.close()

    sc2 = kafka_cluster.share_consumer({'group.id': group_id, 'share.acknowledgement.mode': 'explicit'})
    try:
        sc2.subscribe([topic])
        leftovers = drain_share_consumers([sc2], 1, timeout_s=5.0, ack_type=AcknowledgeType.ACCEPT)[0]
        assert leftovers == [], (
            f'acknowledge_offset(ACCEPT) should prevent redelivery; sc2 '
            f'unexpectedly received {[m.value() for m in leftovers]} (produced {produced})'
        )
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
    """ACCEPT + RELEASE + REJECT in one batch: only the RELEASEd one redelivers.

    This test specifically exercises mixed ack types WITHIN a single Acquired
    set. In explicit-ack mode, calling poll() again before acking the
    previous batch raises _STATE, so we cannot accumulate records across
    multiple polls. If the broker splits delivery into < num_messages on the
    first Acquired set, the in-batch property no longer applies and we skip.
    """
    topic = kafka_cluster.create_topic_and_wait_propogation('test-share-consumer-ack-mixed')
    num_messages = 3

    sc = kafka_cluster.share_consumer({'share.acknowledgement.mode': 'explicit'})
    try:
        sc.subscribe([topic])

        producer = kafka_cluster.cimpl_producer()
        for i in range(num_messages):
            producer.produce(topic, value=f'msg-{i}'.encode())
        producer.flush(timeout=10.0)

        # Single poll with a generous timeout — multiple polls would crash
        # with _STATE in explicit mode (prior batch unacked).
        batch = [m for m in sc.poll(timeout=10.0) if m.error() is None]
        if len(batch) != num_messages:
            for m in batch:
                sc.acknowledge(m, AcknowledgeType.ACCEPT)
            pytest.skip(
                f'broker did not deliver {num_messages} records in a single '
                f'Acquired set (got {len(batch)}); this test specifically '
                f'requires mixed ack types within one batch'
            )

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
    """Acking an offset from a prior batch that's no longer inflight raises _STATE."""
    topic = kafka_cluster.create_topic_and_wait_propogation('test-share-consumer-ack-prior-batch')
    num_first_messages = 3

    sc = kafka_cluster.share_consumer({'share.acknowledgement.mode': 'explicit'})
    try:
        sc.subscribe([topic])

        producer = kafka_cluster.cimpl_producer()
        for i in range(num_first_messages):
            producer.produce(topic, value=f'first-{i}'.encode())
        producer.flush(timeout=10.0)

        first_batch = []
        deadline = time.time() + 20.0
        while time.time() < deadline and len(first_batch) < num_first_messages:
            for m in sc.poll(timeout=0.5):
                if m.error() is None:
                    first_batch.append(m)
                    sc.acknowledge(m, AcknowledgeType.ACCEPT)
            if len(first_batch) >= num_first_messages:
                break
        assert len(first_batch) == num_first_messages
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

        # Ack the live batch so close() doesn't raise on unacked records.
        for m in second_batch:
            sc.acknowledge(m, AcknowledgeType.ACCEPT)
    finally:
        sc.close()


def test_out_of_order_acks_within_batch(kafka_cluster):
    """Acks within one batch may arrive in any order."""
    topic = kafka_cluster.create_topic_and_wait_propogation('test-share-consumer-ack-out-of-order')
    num_messages = 5

    sc = kafka_cluster.share_consumer({'share.acknowledgement.mode': 'explicit'})
    try:
        sc.subscribe([topic])

        producer = kafka_cluster.cimpl_producer()
        for i in range(num_messages):
            producer.produce(topic, value=f'msg-{i}'.encode())
        producer.flush(timeout=10.0)

        batch = []
        deadline = time.time() + 20.0
        while time.time() < deadline and len(batch) < num_messages:
            for m in sc.poll(timeout=1.0):
                if m.error() is None:
                    batch.append(m)
            if len(batch) >= num_messages:
                break
        assert len(batch) == num_messages, f'expected {num_messages} in one Acquired set, got {len(batch)}'

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
    num_messages = 5

    sc = kafka_cluster.share_consumer({'share.acknowledgement.mode': 'explicit'})
    try:
        sc.subscribe([topic])

        producer = kafka_cluster.cimpl_producer()
        for i in range(num_messages):
            producer.produce(topic, value=f'msg-{i}'.encode())
        producer.flush(timeout=10.0)

        batch = []
        deadline = time.time() + 20.0
        while time.time() < deadline and len(batch) < num_messages:
            for m in sc.poll(timeout=1.0):
                if m.error() is None:
                    batch.append(m)
            if len(batch) >= num_messages:
                break
        assert len(batch) == num_messages, f'expected {num_messages} in one Acquired set, got {len(batch)}'

        for m in batch[:3]:
            sc.acknowledge(m, AcknowledgeType.ACCEPT)

        with pytest.raises(KafkaException) as ex:
            sc.poll(timeout=2.0)
        assert ex.value.args[0].code() == KafkaError._STATE

        # Ack the rest; poll should be clean.
        for m in batch[3:]:
            sc.acknowledge(m, AcknowledgeType.ACCEPT)
        sc.poll(timeout=2.0)
    finally:
        sc.close()
