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

from confluent_kafka import AcknowledgeType, KafkaError, KafkaException, TopicPartition
from tests.common import drain_share_consumers, poll_first_batch, unique_id

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


# --- acknowledgement-commit callback --------------------------------------
#
# The cb fires once per partition, dispatched on the thread that calls poll().
# Tests that expect a cb invocation must poll() at least once after
# commit_async / commit_sync so the dispatch has a chance to run.


def _wait_for_callback(sc, invocations, expected_count, timeout_s=10.0):
    """Poll until at least expected_count cb invocations have been recorded
    or timeout_s elapses. Returns the (possibly partial) invocations list."""
    deadline = time.time() + timeout_s
    while len(invocations) < expected_count and time.time() < deadline:
        sc.poll(timeout=0.5)
    return invocations


def test_set_callback_rejects_non_callable(kafka_cluster):
    """Non-callable raises TypeError at the binding."""
    sc = kafka_cluster.share_consumer({'share.acknowledgement.mode': 'explicit'})
    try:
        for bad in (42, 'not-a-callable', object()):
            with pytest.raises(TypeError):
                sc.set_acknowledgement_commit_callback(bad)
    finally:
        sc.close()


def test_set_callback_accepts_none(kafka_cluster):
    """None clears the callback; setting/clearing repeatedly is safe.
    Both positional and ``callback=`` keyword forms work."""
    sc = kafka_cluster.share_consumer({'share.acknowledgement.mode': 'explicit'})
    try:
        sc.set_acknowledgement_commit_callback(None)
        sc.set_acknowledgement_commit_callback(lambda offsets, exc: None)
        sc.set_acknowledgement_commit_callback(None)
        sc.set_acknowledgement_commit_callback(callback=lambda offsets, exc: None)
        sc.set_acknowledgement_commit_callback(callback=None)
    finally:
        sc.close()


def test_set_callback_after_close_raises(kafka_cluster):
    """Calling the setter on a closed consumer raises RuntimeError."""
    sc = kafka_cluster.share_consumer({'share.acknowledgement.mode': 'explicit'})
    sc.close()
    with pytest.raises(RuntimeError):
        sc.set_acknowledgement_commit_callback(lambda offsets, exc: None)


def test_callback_fires_on_explicit_commit_async(kafka_cluster):
    """ack → commit_async → poll fires the cb with exception=None and
    a dict containing the acknowledged offsets."""
    topic = kafka_cluster.create_topic_and_wait_propogation(
        'test-share-consumer-cb-explicit')
    num_messages = 3

    sc = kafka_cluster.share_consumer({'share.acknowledgement.mode': 'explicit'})
    try:
        invocations = []
        sc.set_acknowledgement_commit_callback(
            lambda offsets, exc: invocations.append((offsets, exc)))

        sc.subscribe([topic])

        producer = kafka_cluster.cimpl_producer()
        produced = [f'msg-{i}'.encode() for i in range(num_messages)]
        for value in produced:
            producer.produce(topic, value=value)
        producer.flush(timeout=10.0)

        acked_offsets = set()
        batch = []
        deadline = time.time() + 20.0
        while time.time() < deadline and len(batch) < num_messages:
            for m in sc.poll(timeout=0.5):
                if m.error() is None:
                    batch.append(m)
                    sc.acknowledge(m, AcknowledgeType.ACCEPT)
                    acked_offsets.add(m.offset())
        assert len(batch) == num_messages, f'received {len(batch)} of {num_messages}'

        sc.commit_async()
        _wait_for_callback(sc, invocations, 1, timeout_s=10.0)

        assert invocations, 'callback never fired'
        cb_offsets = set()
        for offsets, exc in invocations:
            assert exc is None, f'unexpected exception in cb: {exc!r}'
            # librdkafka fires the cb once per partition; with a single
            # default-partition-count topic the dict has exactly one key.
            assert len(offsets) == 1, f'expected 1 partition key, got {list(offsets)}'
            tp = next(iter(offsets))
            assert isinstance(tp, TopicPartition)
            assert tp.topic == topic
            assert isinstance(offsets[tp], frozenset)
            cb_offsets |= offsets[tp]
        assert cb_offsets == acked_offsets, (
            f'cb saw {cb_offsets}, acked {acked_offsets}'
        )
    finally:
        sc.close()


def test_callback_fires_on_implicit_mode(kafka_cluster):
    """Implicit mode: next poll auto-acks prior batch and fires the cb."""
    topic = kafka_cluster.create_topic_and_wait_propogation(
        'test-share-consumer-cb-implicit')
    num_messages = 3

    sc = kafka_cluster.share_consumer({'share.acknowledgement.mode': 'implicit'})
    try:
        invocations = []
        sc.set_acknowledgement_commit_callback(
            lambda offsets, exc: invocations.append((offsets, exc)))

        sc.subscribe([topic])

        producer = kafka_cluster.cimpl_producer()
        for i in range(num_messages):
            producer.produce(topic, value=f'msg-{i}'.encode())
        producer.flush(timeout=10.0)

        # Drain all records — each subsequent poll() implicit-acks the prior
        # batch, which fires the cb.
        drain_share_consumers([sc], num_messages, timeout_s=20.0)

        # Final commit_async to flush the tail batch, plus polls to drain.
        sc.commit_async()
        _wait_for_callback(sc, invocations, 1, timeout_s=10.0)

        assert invocations, 'callback never fired'
        for offsets, exc in invocations:
            assert exc is None, f'unexpected exception in cb: {exc!r}'
            assert len(offsets) == 1
    finally:
        sc.close()


def test_callback_replacement_only_new_fires(kafka_cluster):
    """Re-registering replaces the prior callback — old one stops firing."""
    topic = kafka_cluster.create_topic_and_wait_propogation(
        'test-share-consumer-cb-replace')

    sc = kafka_cluster.share_consumer({'share.acknowledgement.mode': 'explicit'})
    try:
        cb1_calls = []
        cb2_calls = []

        # First cycle: cb1 active.
        sc.set_acknowledgement_commit_callback(
            lambda offsets, exc: cb1_calls.append((offsets, exc)))
        sc.subscribe([topic])
        producer = kafka_cluster.cimpl_producer()
        producer.produce(topic, value=b'msg-cb1')
        producer.flush(timeout=10.0)

        msgs = poll_first_batch(sc)
        assert msgs, 'cb1 cycle: no message received'
        for m in msgs:
            sc.acknowledge(m, AcknowledgeType.ACCEPT)
        sc.commit_async()
        _wait_for_callback(sc, cb1_calls, 1, timeout_s=10.0)
        assert cb1_calls, 'cb1 never fired'
        cb1_count_after_first_cycle = len(cb1_calls)

        # Swap to cb2.
        sc.set_acknowledgement_commit_callback(
            lambda offsets, exc: cb2_calls.append((offsets, exc)))

        producer.produce(topic, value=b'msg-cb2')
        producer.flush(timeout=10.0)
        msgs = poll_first_batch(sc)
        assert msgs, 'cb2 cycle: no message received'
        for m in msgs:
            sc.acknowledge(m, AcknowledgeType.ACCEPT)
        sc.commit_async()
        _wait_for_callback(sc, cb2_calls, 1, timeout_s=10.0)

        assert cb2_calls, 'cb2 never fired after replacement'
        assert len(cb1_calls) == cb1_count_after_first_cycle, (
            f'cb1 fired again after replacement: {cb1_calls[cb1_count_after_first_cycle:]}'
        )
    finally:
        sc.close()


def test_callback_clear_with_none_disables(kafka_cluster):
    """After clearing with None the cb must not fire on subsequent commits."""
    topic = kafka_cluster.create_topic_and_wait_propogation(
        'test-share-consumer-cb-clear')

    sc = kafka_cluster.share_consumer({'share.acknowledgement.mode': 'explicit'})
    try:
        invocations = []
        sc.set_acknowledgement_commit_callback(
            lambda offsets, exc: invocations.append((offsets, exc)))
        sc.subscribe([topic])

        producer = kafka_cluster.cimpl_producer()
        producer.produce(topic, value=b'msg-pre-clear')
        producer.flush(timeout=10.0)

        msgs = poll_first_batch(sc)
        assert msgs
        for m in msgs:
            sc.acknowledge(m, AcknowledgeType.ACCEPT)
        sc.commit_async()
        _wait_for_callback(sc, invocations, 1, timeout_s=10.0)
        assert invocations, 'cb did not fire before clear'
        count_before_clear = len(invocations)

        # Clear and do another cycle.
        sc.set_acknowledgement_commit_callback(None)
        producer.produce(topic, value=b'msg-post-clear')
        producer.flush(timeout=10.0)
        msgs = poll_first_batch(sc)
        assert msgs
        for m in msgs:
            sc.acknowledge(m, AcknowledgeType.ACCEPT)
        sc.commit_async()

        # Drain rk_rep so any (unwanted) cb dispatches get a chance.
        deadline = time.time() + 5.0
        while time.time() < deadline:
            sc.poll(timeout=0.5)

        assert len(invocations) == count_before_clear, (
            f'cb fired {len(invocations) - count_before_clear} times after clear'
        )
    finally:
        sc.close()


def test_callback_fires_on_commit_sync(kafka_cluster):
    """cb fires inside commit_sync itself — no extra poll needed."""
    topic = kafka_cluster.create_topic_and_wait_propogation(
        'test-share-consumer-cb-commit-sync')

    sc = kafka_cluster.share_consumer({'share.acknowledgement.mode': 'explicit'})
    try:
        invocations = []
        sc.set_acknowledgement_commit_callback(
            lambda offsets, exc: invocations.append((offsets, exc)))
        sc.subscribe([topic])

        producer = kafka_cluster.cimpl_producer()
        producer.produce(topic, value=b'msg-0')
        producer.flush(timeout=10.0)

        msgs = poll_first_batch(sc)
        assert msgs
        for m in msgs:
            sc.acknowledge(m, AcknowledgeType.ACCEPT)

        # commit_sync blocks until broker responds, and per-partition results
        # are also delivered to the cb via the post-commit rk_rep drain.
        result = sc.commit_sync(timeout=10.0)
        assert result, 'commit_sync returned no per-partition results'

        # The cb should have fired before commit_sync returned; no poll().
        assert invocations, 'cb did not fire during commit_sync'
        for offsets, exc in invocations:
            assert exc is None, f'unexpected exception in cb: {exc!r}'
            assert len(offsets) == 1
    finally:
        sc.close()


def test_callback_fires_during_close(kafka_cluster):
    """close() drains pending share-ack ops, so unacked-at-close work still
    surfaces through the cb."""
    topic = kafka_cluster.create_topic_and_wait_propogation(
        'test-share-consumer-cb-close')

    sc = kafka_cluster.share_consumer({'share.acknowledgement.mode': 'explicit'})
    invocations = []
    try:
        sc.set_acknowledgement_commit_callback(
            lambda offsets, exc: invocations.append((offsets, exc)))
        sc.subscribe([topic])

        producer = kafka_cluster.cimpl_producer()
        producer.produce(topic, value=b'msg-0')
        producer.flush(timeout=10.0)

        msgs = poll_first_batch(sc)
        assert msgs
        for m in msgs:
            sc.acknowledge(m, AcknowledgeType.ACCEPT)

        # Trigger the send but DO NOT poll — let close() drain the response.
        sc.commit_async()
    finally:
        sc.close()

    assert invocations, 'cb did not fire during close()'
    for offsets, exc in invocations:
        assert exc is None, f'unexpected exception in cb: {exc!r}'
        assert len(offsets) == 1


def test_callback_cardinality_multipartition(kafka_cluster):
    """With N partitions and records spread across them, the cb should fire
    N times, each with exactly one TopicPartition key."""
    num_partitions = 3
    num_messages_per_partition = 2
    topic = kafka_cluster.create_topic_and_wait_propogation(
        'test-share-consumer-cb-multipart',
        {'num_partitions': num_partitions})

    sc = kafka_cluster.share_consumer({'share.acknowledgement.mode': 'explicit'})
    try:
        invocations = []
        sc.set_acknowledgement_commit_callback(
            lambda offsets, exc: invocations.append((offsets, exc)))
        sc.subscribe([topic])

        producer = kafka_cluster.cimpl_producer()
        # Produce to specific partitions so all N see records.
        for partition in range(num_partitions):
            for i in range(num_messages_per_partition):
                producer.produce(
                    topic, value=f'p{partition}-msg-{i}'.encode(),
                    partition=partition)
        producer.flush(timeout=10.0)

        total_messages = num_partitions * num_messages_per_partition
        partitions_seen_in_acks = set()
        deadline = time.time() + 30.0
        received = 0
        while time.time() < deadline and received < total_messages:
            for m in sc.poll(timeout=0.5):
                if m.error() is None:
                    sc.acknowledge(m, AcknowledgeType.ACCEPT)
                    partitions_seen_in_acks.add(m.partition())
                    received += 1
        assert partitions_seen_in_acks == set(range(num_partitions)), (
            f'expected acks on partitions {set(range(num_partitions))}, '
            f'got {partitions_seen_in_acks}')

        sc.commit_async()
        deadline = time.time() + 10.0
        while time.time() < deadline and len(invocations) < num_partitions:
            sc.poll(timeout=0.5)

        # Every invocation: exactly one partition key.
        for offsets, exc in invocations:
            assert exc is None
            assert len(offsets) == 1, (
                f'cb invocation carried {len(offsets)} partitions; expected 1')

        # Aggregate: every partition we acked must show up across the
        # invocations.
        partitions_seen_in_cb = set()
        for offsets, _ in invocations:
            tp = next(iter(offsets))
            partitions_seen_in_cb.add(tp.partition)
        assert partitions_seen_in_cb == partitions_seen_in_acks, (
            f'cb saw partitions {partitions_seen_in_cb}, '
            f'expected {partitions_seen_in_acks}')
    finally:
        sc.close()


def test_callback_not_invoked_on_empty_commit(kafka_cluster):
    """commit_async/commit_sync with no pending acks short-circuits without
    a broker request — the cb must not fire."""
    topic = kafka_cluster.create_topic_and_wait_propogation(
        'test-share-consumer-cb-empty-commit')

    sc = kafka_cluster.share_consumer({'share.acknowledgement.mode': 'explicit'})
    try:
        invocations = []
        sc.set_acknowledgement_commit_callback(
            lambda offsets, exc: invocations.append((offsets, exc)))
        sc.subscribe([topic])

        # Empty commits — no records consumed, nothing to ack.
        sc.commit_async()
        result = sc.commit_sync(timeout=2.0)
        assert result == {}, f'expected empty result dict, got {result!r}'

        # Give librdkafka a chance to fire any (unwanted) cb dispatches.
        deadline = time.time() + 3.0
        while time.time() < deadline:
            sc.poll(timeout=0.5)

        assert invocations == [], (
            f'cb fired on empty commit: {invocations!r}'
        )
    finally:
        sc.close()


def test_callback_reentrancy_guard(kafka_cluster):
    """Calling share-consumer APIs from inside the cb fails with _STATE —
    librdkafka guards every entry point against reentrancy."""
    topic = kafka_cluster.create_topic_and_wait_propogation(
        'test-share-consumer-cb-reentrancy')

    sc = kafka_cluster.share_consumer({'share.acknowledgement.mode': 'explicit'})
    try:
        # Two separate guard checks: set_…cb itself, and a generic share-API
        # call (commit_async) that goes through the same reentrancy guard.
        captured_setter_err = []
        captured_commit_err = []

        def reentrant_cb(offsets, exc):
            try:
                sc.set_acknowledgement_commit_callback(
                    lambda o, e: None)
            except KafkaException as ex:
                captured_setter_err.append(ex)
            try:
                sc.commit_async()
            except KafkaException as ex:
                captured_commit_err.append(ex)

        sc.set_acknowledgement_commit_callback(reentrant_cb)
        sc.subscribe([topic])

        producer = kafka_cluster.cimpl_producer()
        producer.produce(topic, value=b'msg-0')
        producer.flush(timeout=10.0)

        msgs = poll_first_batch(sc)
        assert msgs
        for m in msgs:
            sc.acknowledge(m, AcknowledgeType.ACCEPT)

        sc.commit_async()
        deadline = time.time() + 10.0
        while time.time() < deadline and not captured_setter_err:
            sc.poll(timeout=0.5)

        assert captured_setter_err, 'cb did not raise on nested setter call'
        assert captured_setter_err[0].args[0].code() == KafkaError._STATE
        assert captured_commit_err, 'cb did not raise on nested commit_async'
        assert captured_commit_err[0].args[0].code() == KafkaError._STATE
    finally:
        # Replace the reentrant cb before close so close()'s drain doesn't
        # re-trip the guards.
        try:
            sc.set_acknowledgement_commit_callback(None)
        except Exception:
            pass
        sc.close()


def test_callback_exception_propagates_from_poll(kafka_cluster):
    """An exception raised inside the cb surfaces from the next poll() call."""
    topic = kafka_cluster.create_topic_and_wait_propogation(
        'test-share-consumer-cb-exception')

    sentinel = ValueError('sentinel from cb')

    def raising_cb(offsets, exc):
        raise sentinel

    sc = kafka_cluster.share_consumer({'share.acknowledgement.mode': 'explicit'})
    try:
        sc.set_acknowledgement_commit_callback(raising_cb)
        sc.subscribe([topic])

        producer = kafka_cluster.cimpl_producer()
        producer.produce(topic, value=b'msg-0')
        producer.flush(timeout=10.0)

        msgs = poll_first_batch(sc)
        assert msgs
        for m in msgs:
            sc.acknowledge(m, AcknowledgeType.ACCEPT)
        sc.commit_async()

        # The cb is dispatched on the next poll's rk_rep drain; that's when
        # the ValueError should resurface in this thread.
        deadline = time.time() + 10.0
        raised = None
        while time.time() < deadline and raised is None:
            try:
                sc.poll(timeout=0.5)
            except ValueError as e:
                raised = e
                break
        assert raised is sentinel, f'expected sentinel ValueError, got {raised!r}'
    finally:
        # Replace the raising cb before close so close() doesn't fire it.
        try:
            sc.set_acknowledgement_commit_callback(None)
        except Exception:
            pass
        sc.close()
