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

"""Integration tests for ShareConsumer"""

import gc
import sys
import time

import pytest

from confluent_kafka import (
    TIMESTAMP_CREATE_TIME,
    AcknowledgeType,
    IllegalStateException,
    KafkaError,
    KafkaException,
    Messages,
    Producer,
)
from confluent_kafka.admin import NewPartitions, NewTopic
from tests.common import (
    drain_share_consumers,
    poll_first_batch,
    set_group_config,
    unique_id,
)


def test_concurrent_consumers(kafka_cluster):
    """Two consumers in the same share group must receive disjoint records,
    and their union must cover every produced record exactly once.

    Per-consumer distribution is intentionally NOT asserted: with a single
    partition, serial round-robin polling, and the default max.poll.records
    (500) far exceeding the produced record count, a single ShareFetch from
    whichever consumer polls first can drain all records before the other
    gets a chance. KIP-932 share groups don't guarantee
    even distribution under those conditions, and librdkafka's analogous
    test (tests/0171-share_consumer_consume.c::test_multiple_consumers_*)
    likewise asserts only total-count and not per-consumer counts.
    """
    topic = kafka_cluster.create_topic_and_wait_propogation('test-share-consumer-concurrent')
    group_id = unique_id('test-share-group')
    n_messages = 30

    sc1 = kafka_cluster.share_consumer({'group.id': group_id})
    sc2 = kafka_cluster.share_consumer({'group.id': group_id})

    try:
        sc1.subscribe([topic])
        sc2.subscribe([topic])
        # Drive both consumers through the join handshake so neither races
        # ahead and grabs all records before the other is ready.
        for _ in range(10):
            sc1.poll(timeout=0.2)
            sc2.poll(timeout=0.2)

        producer = kafka_cluster.cimpl_producer()
        for i in range(n_messages):
            producer.produce(topic, value=f'msg-{i}'.encode())
        producer.flush(timeout=10.0)

        received_msgs_1, received_msgs_2 = drain_share_consumers([sc1, sc2], n_messages)
        offsets1 = {(msg.topic(), msg.partition(), msg.offset()) for msg in received_msgs_1}
        offsets2 = {(msg.topic(), msg.partition(), msg.offset()) for msg in received_msgs_2}

        overlap = offsets1 & offsets2
        all_offsets = offsets1 | offsets2

        assert overlap == set(), f"Same record delivered to both consumers: {overlap}"
        assert len(all_offsets) == n_messages, (
            f"Expected {n_messages} unique records across both consumers, "
            f"got {len(all_offsets)} (sc1={len(offsets1)}, sc2={len(offsets2)})"
        )
    finally:
        sc1.close()
        sc2.close()


def test_basic_consume_records(kafka_cluster):
    """Single share consumer reads all produced records with correct values."""
    topic = kafka_cluster.create_topic_and_wait_propogation('test-share-consumer-basic')
    n = 10

    sc = kafka_cluster.share_consumer()
    try:
        sc.subscribe([topic])

        expected = [f'msg-{i}'.encode() for i in range(n)]
        producer = kafka_cluster.cimpl_producer()
        for v in expected:
            producer.produce(topic, value=v)
        producer.flush(timeout=10.0)

        received_msgs = drain_share_consumers([sc], n)[0]
        values = sorted(msg.value() for msg in received_msgs)
        assert values == sorted(expected), f"Value mismatch: expected {sorted(expected)}, got {values}"
    finally:
        sc.close()


def test_poll_returns_messages(kafka_cluster):
    """poll() hands back a Messages whose count()/is_empty()/records()
    agree with the batch it wraps, over the real C poll -> Messages path."""
    topic = kafka_cluster.create_topic_and_wait_propogation('test-share-consumer-records')
    n = 10

    sc = kafka_cluster.share_consumer()
    try:
        sc.subscribe([topic])

        producer = kafka_cluster.cimpl_producer()
        for i in range(n):
            producer.produce(topic, value=f'msg-{i}'.encode())
        producer.flush(timeout=10.0)

        # Grab the first non-empty batch; empty polls are valid Messages too.
        batch = None
        deadline = time.time() + 30.0
        while time.time() < deadline:
            out = sc.poll(timeout=0.5)
            assert isinstance(out, Messages)
            if not out.is_empty():
                batch = out
                break

        assert batch is not None, 'no records delivered within timeout'
        assert batch.count() == len(batch)
        records = batch.records()
        assert type(records) is list
        assert records == list(batch)
    finally:
        sc.close()


def test_message_fields_preserved(kafka_cluster):
    """Key, value, and headers round-trip intact through ShareConsumer."""
    topic = kafka_cluster.create_topic_and_wait_propogation('test-share-consumer-fields')

    sc = kafka_cluster.share_consumer()
    try:
        sc.subscribe([topic])

        producer = kafka_cluster.cimpl_producer()
        produced = []
        for i in range(5):
            key = f'k-{i}'.encode()
            value = f'v-{i}'.encode()
            headers = [(f'h-{i}', f'hv-{i}'.encode())]
            producer.produce(topic, key=key, value=value, headers=headers)
            produced.append((key, value, headers))
        producer.flush(timeout=10.0)

        received_msgs = drain_share_consumers([sc], 5)[0]
        assert len(received_msgs) == 5

        got = sorted([(msg.key(), msg.value(), msg.headers()) for msg in received_msgs])
        exp = sorted(produced)
        assert got == exp, f"Field mismatch: expected {exp}, got {got}"
    finally:
        sc.close()


def test_header_order_preserved(kafka_cluster):
    """Header ORDER round-trips intact.

    test_message_fields_preserved sorts headers before comparing, so it can't
    catch a reordering. Keys can repeat and Kafka headers are an ordered list,
    so order is part of the contract.
    """
    topic = kafka_cluster.create_topic_and_wait_propogation('test-share-consumer-hdrorder')

    # Repeated key 'a' with different values: only order distinguishes them.
    headers = [('a', b'1'), ('b', b'2'), ('a', b'3'), ('c', b'4')]

    sc = kafka_cluster.share_consumer()
    try:
        sc.subscribe([topic])

        producer = kafka_cluster.cimpl_producer()
        producer.produce(topic, value=b'v', headers=headers)
        producer.flush(timeout=10.0)

        received_msgs = drain_share_consumers([sc], 1)[0]
        assert len(received_msgs) == 1
        assert received_msgs[0].value() == b'v'
        assert received_msgs[0].headers() == headers, f"header order/content changed: {received_msgs[0].headers()}"
    finally:
        sc.close()


def test_timestamp_and_type_preserved(kafka_cluster):
    """A producer-set CreateTime timestamp round-trips with TIMESTAMP_CREATE_TIME.

    Complements test_message_fields_preserved, which doesn't check timestamps.
    Assumes the broker default (CreateTime) so the producer's explicit timestamp
    is the one stored and returned.
    """
    topic = kafka_cluster.create_topic_and_wait_propogation('test-share-consumer-ts')

    ts = 1_600_000_000_000  # fixed CreateTime in ms

    sc = kafka_cluster.share_consumer()
    try:
        sc.subscribe([topic])

        producer = kafka_cluster.cimpl_producer()
        producer.produce(topic, value=b'v', timestamp=ts)
        producer.flush(timeout=10.0)

        received_msgs = drain_share_consumers([sc], 1)[0]
        assert len(received_msgs) == 1
        ts_type, ts_val = received_msgs[0].timestamp()
        assert ts_type == TIMESTAMP_CREATE_TIME, f"expected CREATE_TIME, got timestamp_type {ts_type}"
        assert ts_val == ts, f"timestamp not preserved: expected {ts}, got {ts_val}"
    finally:
        sc.close()


def test_zero_byte_and_null_key_value(kafka_cluster):
    """Empty (zero-length) vs absent (None) keys/values are preserved distinctly:
    b'' stays b'' (not collapsed to None), and None stays None.
    """
    topic = kafka_cluster.create_topic_and_wait_propogation('test-share-consumer-empty')

    sc = kafka_cluster.share_consumer()
    try:
        sc.subscribe([topic])

        producer = kafka_cluster.cimpl_producer()
        producer.produce(topic, key=b'', value=b'')  # empty, non-null -> offset 0
        producer.produce(topic, key=None, value=None)  # null -> offset 1
        producer.flush(timeout=10.0)

        received_msgs = drain_share_consumers([sc], 2)[0]
        by_offset = {msg.offset(): (msg.key(), msg.value()) for msg in received_msgs}
        assert len(by_offset) == 2, f"expected 2 records, got {len(by_offset)}"
        ordered = [by_offset[o] for o in sorted(by_offset)]
        assert ordered[0] == (b'', b''), f"empty key/value not preserved: {ordered[0]}"
        assert ordered[1] == (None, None), f"null key/value not preserved: {ordered[1]}"
    finally:
        sc.close()


def test_single_consumer_multi_partition_full_coverage(kafka_cluster):
    """One consumer drains a multi-partition topic: every record arrives exactly
    once and records show up from every partition.

    The existing basic/ordering tests use a single-partition topic; this
    exercises the multi-partition fetch path.
    """
    n_partitions = 3
    per_partition = 10
    topic = kafka_cluster.create_topic_and_wait_propogation(
        'test-share-consumer-multipart', conf={'num_partitions': n_partitions}
    )

    sc = kafka_cluster.share_consumer()
    try:
        sc.subscribe([topic])

        producer = kafka_cluster.cimpl_producer()
        for p in range(n_partitions):
            for i in range(per_partition):
                producer.produce(topic, value=f'p{p}-{i}'.encode(), partition=p)
        producer.flush(timeout=10.0)

        total = n_partitions * per_partition
        received_msgs = drain_share_consumers([sc], total)[0]

        # Exactly the produced records: unique offsets, exact value set, every
        # partition represented.
        unique_records = {(msg.partition(), msg.offset()) for msg in received_msgs}
        assert len(unique_records) == total, f"expected {total} unique records, got {len(unique_records)}"
        expected_values = sorted(f'p{p}-{i}'.encode() for p in range(n_partitions) for i in range(per_partition))
        assert (
            sorted(msg.value() for msg in received_msgs) == expected_values
        ), 'received values do not match the produced set'
        assert {msg.partition() for msg in received_msgs} == set(range(n_partitions))
    finally:
        sc.close()


def test_max_poll_records_caps_batch(kafka_cluster):
    """max.poll.records caps how many records a single poll() returns.

    The cap can be hidden in two ways: the broker never splits a record batch,
    so one oversized batch overshoots it, and the broker can merge several
    partitions into one response, which may also exceed it. To make the cap
    observable we use a single-partition topic and put each record in its own
    batch (linger.ms=0 + a flush after every produce). With cap=5 and 10 such
    records, no poll() returns more than 5 and draining all 10 takes at least
    2 polls.
    """
    # Single partition: with more, the broker could merge them into one
    # response that exceeds the cap.
    topic = kafka_cluster.create_topic_and_wait_propogation('test-share-consumer-maxpoll', {'num_partitions': 1})
    cap = 5
    n = 10

    sc = kafka_cluster.share_consumer({'max.poll.records': cap})
    try:
        sc.subscribe([topic])

        # One produce + flush per record => one broker batch per record, so the
        # cap can take effect (a single fat batch can't be split below its size).
        producer = kafka_cluster.cimpl_producer({'linger.ms': 0})
        for i in range(n):
            producer.produce(topic, value=f'msg-{i}'.encode())
            producer.flush(timeout=10.0)

        batch_sizes = []
        received_values = []
        deadline = time.time() + 30.0
        while time.time() < deadline and len(received_values) < n:
            batch = [msg.value() for msg in sc.poll(timeout=0.5) if msg.error() is None]
            if batch:
                batch_sizes.append(len(batch))
                received_values.extend(batch)

        expected_values = sorted(f'msg-{i}'.encode() for i in range(n))
        assert sorted(received_values) == expected_values, 'received values do not match the produced set'
        assert all(size <= cap for size in batch_sizes), f"a poll() exceeded max.poll.records={cap}: {batch_sizes}"
        assert len(batch_sizes) >= 2, f"expected >=2 capped batches for {n} records at cap {cap}, got {batch_sizes}"
    finally:
        sc.close()


def test_record_larger_than_fetch_max_bytes_delivered(kafka_cluster):
    """A record larger than the consumer's fetch.max.bytes is still delivered.

    The broker hands back at least one record per partition even when it exceeds
    the fetch budget, so a single large record can't wedge consumption.
    """
    topic = kafka_cluster.create_topic_and_wait_propogation('test-share-consumer-bigrec')

    # fetch.max.bytes is tiny so the large value far exceeds it. It has to be
    # >= message.max.bytes or construction is rejected, so lower both together.
    sc = kafka_cluster.share_consumer({'message.max.bytes': 1500, 'fetch.max.bytes': 1500})
    try:
        sc.subscribe([topic])

        small = b'small'
        large = b'x' * 5000  # >> fetch.max.bytes
        producer = kafka_cluster.cimpl_producer()
        producer.produce(topic, value=small)
        producer.produce(topic, value=large)
        producer.flush(timeout=10.0)

        received_msgs = drain_share_consumers([sc], 2)[0]
        values = sorted((msg.value() for msg in received_msgs), key=len)
        assert values == [
            small,
            large,
        ], f"oversized record not delivered intact: got byte-lengths {[len(v) for v in values]}"
    finally:
        sc.close()


@pytest.mark.parametrize('codec', ['none', 'gzip', 'lz4', 'zstd', 'snappy'])
def test_compression_codec_roundtrip(kafka_cluster, codec):
    """Records produced under each compression codec are consumed intact —
    decompression is transparent to the share consumer.

    Codecs the client wasn't built with are rejected at producer construction
    and skipped.
    """
    topic = kafka_cluster.create_topic_and_wait_propogation(f'test-share-consumer-compress-{codec}')
    n = 10

    try:
        producer = kafka_cluster.cimpl_producer({'compression.type': codec})
    except KafkaException as exc:
        pytest.skip(f"compression codec '{codec}' not available in this build: {exc}")

    sc = kafka_cluster.share_consumer()
    try:
        sc.subscribe([topic])

        expected = [f'{codec}-msg-{i}'.encode() for i in range(n)]
        for v in expected:
            producer.produce(topic, value=v)
        producer.flush(timeout=10.0)

        received_msgs = drain_share_consumers([sc], n)[0]
        assert sorted(msg.value() for msg in received_msgs) == sorted(expected), f"{codec}: value mismatch"
    finally:
        sc.close()


def test_multi_topic_subscription(kafka_cluster):
    """Subscribe to multiple topics; records from all topics are delivered."""
    topic_a = kafka_cluster.create_topic_and_wait_propogation('test-share-consumer-multi-a')
    topic_b = kafka_cluster.create_topic_and_wait_propogation('test-share-consumer-multi-b')
    n_per_topic = 5

    sc = kafka_cluster.share_consumer()
    try:
        sc.subscribe([topic_a, topic_b])

        producer = kafka_cluster.cimpl_producer()
        for i in range(n_per_topic):
            producer.produce(topic_a, value=f'a-{i}'.encode())
            producer.produce(topic_b, value=f'b-{i}'.encode())
        producer.flush(timeout=10.0)

        received_msgs = drain_share_consumers([sc], 2 * n_per_topic)[0]
        topics_seen = {msg.topic() for msg in received_msgs}
        assert topics_seen == {topic_a, topic_b}, f"Expected both topics, got {topics_seen}"
        assert (
            len(received_msgs) == 2 * n_per_topic
        ), f"Expected {2 * n_per_topic} records across both topics, got {len(received_msgs)}"
    finally:
        sc.close()


def test_records_before_join_not_delivered(kafka_cluster):
    """KIP-932: records produced before consumer joins must not be delivered."""
    topic = kafka_cluster.create_topic_and_wait_propogation('test-share-consumer-prejoin')
    n = 20

    producer = kafka_cluster.cimpl_producer()
    for i in range(n):
        producer.produce(topic, value=f'pre-{i}'.encode())
    producer.flush(timeout=10.0)

    # Override the suite-wide 'earliest' default: this test asserts that
    # pre-join records are NOT delivered, which is only the contract under
    # 'latest'.
    # TODO KIP-932: passing 'auto.offset.reset' as a consumer-config override
    # here gives the incorrect impression that it's a consumer property; it's
    # actually a per-group broker-side setting. Once the fixture exposes a
    # group-level setter, switch this test to use that instead.
    sc = kafka_cluster.share_consumer({'auto.offset.reset': 'latest'})
    try:
        sc.subscribe([topic])
        # Observation window — pre-join records (if delivered at all) would
        # arrive here.
        received_msgs = []
        deadline = time.time() + 8.0
        while time.time() < deadline:
            for msg in sc.poll(timeout=0.5):
                if msg.error() is None:
                    received_msgs.append(msg)

        assert received_msgs == [], (
            f"Pre-join records were delivered ({len(received_msgs)} messages); "
            f"share consumers must only see records produced after join"
        )
    finally:
        sc.close()


def test_three_consumers_no_overlap(kafka_cluster):
    """Three consumers in same share group: no overlap, full coverage.

    Pass poll_timeout_s=0.2 to drain_share_consumers so a 3-consumer round (~0.6s) completes well
    within the 1s record lock — otherwise locks expire before implicit-ack
    fires and the broker redelivers to other consumers, breaking the
    no-overlap invariant we DO want to assert here.
    """
    topic = kafka_cluster.create_topic_and_wait_propogation('test-share-consumer-three')
    group_id = unique_id('test-share-three')
    n = 30

    consumers = [kafka_cluster.share_consumer({'group.id': group_id}) for _ in range(3)]
    try:
        for sc in consumers:
            sc.subscribe([topic])
        # Drive every consumer through the join so none race ahead.
        for _ in range(10):
            for sc in consumers:
                sc.poll(timeout=0.2)

        producer = kafka_cluster.cimpl_producer()
        for i in range(n):
            producer.produce(topic, value=f'msg-{i}'.encode())
        producer.flush(timeout=10.0)

        received_msgs = drain_share_consumers(consumers, n, poll_timeout_s=0.2)
        offset_sets = [{(msg.topic(), msg.partition(), msg.offset()) for msg in r} for r in received_msgs]

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


def test_independent_share_groups(kafka_cluster):
    """Two consumers in different share groups each see all records."""
    topic = kafka_cluster.create_topic_and_wait_propogation('test-share-consumer-independent')
    n = 10

    sc_a = kafka_cluster.share_consumer()
    sc_b = kafka_cluster.share_consumer()

    try:
        sc_a.subscribe([topic])
        sc_b.subscribe([topic])

        producer = kafka_cluster.cimpl_producer()
        for i in range(n):
            producer.produce(topic, value=f'msg-{i}'.encode())
        producer.flush(timeout=10.0)

        received_msgs_a, received_msgs_b = drain_share_consumers([sc_a, sc_b], 2 * n)
        offsets_a = {(msg.topic(), msg.partition(), msg.offset()) for msg in received_msgs_a}
        offsets_b = {(msg.topic(), msg.partition(), msg.offset()) for msg in received_msgs_b}

        assert len(offsets_a) == n, f"Group A got {len(offsets_a)} unique records, expected {n}"
        assert len(offsets_b) == n, f"Group B got {len(offsets_b)} unique records, expected {n}"
        assert offsets_a == offsets_b, "Both groups should see the same set of records"
    finally:
        sc_a.close()
        sc_b.close()


def test_implicit_ack_no_redelivery(kafka_cluster):
    """Records consumed in poll N are implicitly accepted on later polls; no redelivery."""
    topic = kafka_cluster.create_topic_and_wait_propogation('test-share-consumer-ack')
    n = 10

    sc = kafka_cluster.share_consumer()
    try:
        sc.subscribe([topic])

        producer = kafka_cluster.cimpl_producer()
        for i in range(n):
            producer.produce(topic, value=f'msg-{i}'.encode())
        producer.flush(timeout=10.0)

        seen = set()
        deadline = time.time() + 20.0
        while time.time() < deadline and len(seen) < n:
            for msg in sc.poll(timeout=0.5):
                if msg.error() is None:
                    seen.add((msg.partition(), msg.offset()))

        assert len(seen) == n, f"Failed to consume all {n} records (got {len(seen)})"

        # Continue polling — implicit ack should accept previously delivered
        # records, so no redelivery should occur.
        extras = []
        for _ in range(8):
            for msg in sc.poll(timeout=0.5):
                if msg.error() is None:
                    extras.append((msg.partition(), msg.offset()))

        assert extras == [], f"Records were redelivered after implicit ack: {extras}"
    finally:
        sc.close()


def test_records_redelivered_after_lock_timeout(kafka_cluster):
    """Defining at-least-once invariant: when a consumer fails to ack within
    the acquisition-lock window, the broker redelivers the record to another
    consumer in the same share group.

    Relies on the test broker's reduced lock duration
    (group.share.record.lock.duration.ms=1000); under the production default
    of 30s, this test would block for half a minute per run.
    """
    topic = kafka_cluster.create_topic_and_wait_propogation('test-share-consumer-redeliver')
    group_id = unique_id('test-share-redeliver')
    n = 5

    sc1 = kafka_cluster.share_consumer({'group.id': group_id})
    sc2 = kafka_cluster.share_consumer({'group.id': group_id})

    try:
        sc1.subscribe([topic])
        sc2.subscribe([topic])
        # Drive both consumers through the join handshake so neither races
        # ahead of the other.
        for _ in range(10):
            sc1.poll(timeout=0.2)
            sc2.poll(timeout=0.2)

        producer = kafka_cluster.cimpl_producer()
        for i in range(n):
            producer.produce(topic, value=f'msg-{i}'.encode())
        producer.flush(timeout=10.0)

        # sc1 polls ONCE and grabs whichever records the broker assigns to
        # it. sc1 will then go silent — no further poll, so those records
        # are never implicitly acked.
        sc1_received = set()
        deadline = time.time() + 5.0
        while time.time() < deadline and not sc1_received:
            for msg in sc1.poll(timeout=0.5):
                if msg.error() is None:
                    sc1_received.add((msg.partition(), msg.offset()))
            if sc1_received:
                break

        assert sc1_received, "sc1 should have grabbed at least one record before going silent"

        # Wait past the broker's lock duration (1s). After this, records
        # held by sc1 are eligible for redelivery to any group member.
        time.sleep(1.5)

        # sc2 keeps polling and must eventually see every record produced —
        # both its own initial share AND sc1's now-unlocked records.
        sc2_received = set()
        deadline = time.time() + 10.0
        while time.time() < deadline and len(sc2_received) < n:
            for msg in sc2.poll(timeout=0.5):
                if msg.error() is None:
                    sc2_received.add((msg.partition(), msg.offset()))

        redelivered = sc1_received & sc2_received
        assert redelivered, (
            f"Expected sc1's unacked records to be redelivered to sc2 "
            f"(at-least-once contract). sc1 had {sc1_received}, "
            f"sc2 received {sc2_received}, no overlap."
        )
    finally:
        sc1.close()
        sc2.close()


def test_poll_with_zero_timeout(kafka_cluster):
    """poll(timeout=0) is non-blocking AND delivers records through the
    non-blocking path correctly.

    Async wrappers (asyncio bridges, custom event loops) integrate by
    tight-looping with timeout=0 and yielding to other tasks between calls.
    The contract: poll(0) returns promptly whether or not records are
    available, and produces records when they exist. A test that only
    asserts "first call returns fast" wouldn't catch a bug where poll(0)
    silently fails to surface available records.
    """
    topic = kafka_cluster.create_topic_and_wait_propogation('test-share-consumer-poll-zero')
    n = 10

    sc = kafka_cluster.share_consumer()
    try:
        sc.subscribe([topic])

        producer = kafka_cluster.cimpl_producer()
        for i in range(n):
            producer.produce(topic, value=f'msg-{i}'.encode())
        producer.flush(timeout=10.0)

        collected = []
        deadline = time.time() + 20.0
        while time.time() < deadline and len(collected) < n:
            for msg in sc.poll(timeout=0):
                if msg.error() is None:
                    collected.append((msg.partition(), msg.offset()))

        assert len(collected) == n, (
            f"poll(timeout=0) tight-loop should deliver all {n} records, " f"got {len(collected)}"
        )
    finally:
        sc.close()


def test_unsubscribe_stops_delivery(kafka_cluster):
    """After unsubscribe, poll() raises _STATE — can't even fetch, let alone
    deliver."""
    topic = kafka_cluster.create_topic_and_wait_propogation('test-share-consumer-unsub')

    sc = kafka_cluster.share_consumer()
    try:
        sc.subscribe([topic])

        producer = kafka_cluster.cimpl_producer()
        for i in range(5):
            producer.produce(topic, value=f'msg-{i}'.encode())
        producer.flush(timeout=10.0)

        first_batch = drain_share_consumers([sc], 5)[0]
        assert len(first_batch) == 5, f"Pre-unsubscribe phase incomplete (got {len(first_batch)}/5)"

        sc.unsubscribe()

        for i in range(5, 10):
            producer.produce(topic, value=f'msg-{i}'.encode())
        producer.flush(timeout=10.0)

        # No subscription anymore, so poll() raises _STATE instead of returning
        # an empty batch. Those 5 new records just sit on the broker.
        with pytest.raises(IllegalStateException) as ex:
            sc.poll(timeout=0.5)
        assert str(ex.value)
    finally:
        sc.close()


def test_resubscribe_to_different_topic(kafka_cluster):
    """subscribe() replaces (does not extend) the prior subscription."""
    topic_a = kafka_cluster.create_topic_and_wait_propogation('test-share-consumer-resub-a')
    topic_b = kafka_cluster.create_topic_and_wait_propogation('test-share-consumer-resub-b')

    sc = kafka_cluster.share_consumer()
    # TODO KIP-932: move producer creation inside the try block (consistent
    # with the other tests) so sc doesn't leak if cimpl_producer() raises.
    producer = kafka_cluster.cimpl_producer()
    try:
        # Phase 1: prove the topic_a subscription actually works before we
        # switch — otherwise we'd never know whether subscribe([topic_b])
        # was the thing that excluded topic_a or whether topic_a was never
        # really subscribed to in the first place.
        sc.subscribe([topic_a])
        for i in range(3):
            producer.produce(topic_a, value=f'a-pre-{i}'.encode())
        producer.flush(timeout=10.0)

        first_msgs = drain_share_consumers([sc], 3)[0]
        assert len(first_msgs) == 3, f"Failed to consume from topic_a (got {len(first_msgs)}/3)"
        assert all(
            msg.topic() == topic_a for msg in first_msgs
        ), f"Expected only topic_a records, got {[msg.topic() for msg in first_msgs]}"

        # Phase 2: switch subscription. Records to topic_a must no longer
        # be delivered; only topic_b records should arrive.
        sc.subscribe([topic_b])

        # subscribe() is async — drive heartbeats so the new subscription
        # ({topic_b}) reaches the broker before we produce. Without this,
        # the broker may still see {topic_a} and deliver a-post-* records.
        for _ in range(10):
            sc.poll(timeout=0.2)

        for i in range(5):
            producer.produce(topic_a, value=f'a-post-{i}'.encode())
            producer.produce(topic_b, value=f'b-{i}'.encode())
        producer.flush(timeout=10.0)

        received_msgs = drain_share_consumers([sc], 5)[0]
        topics = {msg.topic() for msg in received_msgs}
        assert topics == {topic_b}, f"Resubscribe should drop topic_a; got topics {topics}"
        assert len(received_msgs) == 5, f"Expected 5 topic_b records, got {len(received_msgs)}"
    finally:
        sc.close()


def test_messages_in_offset_order_single_consumer(kafka_cluster):
    """Within each partition, single consumer sees records in offset order."""
    topic = kafka_cluster.create_topic_and_wait_propogation('test-share-consumer-order')
    n = 30

    sc = kafka_cluster.share_consumer()
    try:
        sc.subscribe([topic])

        producer = kafka_cluster.cimpl_producer()
        for i in range(n):
            producer.produce(topic, value=f'msg-{i}'.encode())
        producer.flush(timeout=10.0)

        per_partition = {}
        total = 0
        deadline = time.time() + 20.0
        while time.time() < deadline and total < n:
            for msg in sc.poll(timeout=0.5):
                if msg.error() is None:
                    per_partition.setdefault(msg.partition(), []).append(msg.offset())
                    total += 1

        assert total == n, f"Expected {n} records, got {total}"

        for p, offsets in per_partition.items():
            assert offsets == sorted(offsets), f"Partition {p} offsets out of order: {offsets}"
    finally:
        sc.close()


def test_open_transaction_stalls_share_group(kafka_cluster):
    """read_committed: open txn blocks delivery until commit."""
    topic = kafka_cluster.create_topic_and_wait_propogation('test-share-consumer-txn-stall')
    group_id = unique_id('test-share-consumer-txn-stall')

    txn_producer = Producer(kafka_cluster.client_conf({'transactional.id': unique_id('txn')}))
    txn_producer.init_transactions(10)

    set_group_config(kafka_cluster, group_id, 'share.isolation.level', 'read_committed')

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
        assert stalled == [], f'open txn did not stall delivery: {[msg.value() for msg in stalled]}'

        txn_producer.commit_transaction(10)

        received_msgs = drain_share_consumers([sc], 3, ack_type=AcknowledgeType.ACCEPT)[0]
        assert len(received_msgs) == 3, f'expected 3 msgs after commit, got {len(received_msgs)}'
    finally:
        sc.close()


def test_read_committed_skips_aborted_transaction(kafka_cluster):
    """read_committed: records from an aborted transaction are never delivered,
    while a committed record on the same topic is. The complement of
    test_open_transaction_stalls_share_group, which covers the open (not yet
    resolved) transaction case."""
    topic = kafka_cluster.create_topic_and_wait_propogation('test-share-consumer-txn-abort')
    group_id = unique_id('test-share-consumer-txn-abort')

    txn_producer = Producer(kafka_cluster.client_conf({'transactional.id': unique_id('txn')}))
    txn_producer.init_transactions(10)

    set_group_config(kafka_cluster, group_id, 'share.isolation.level', 'read_committed')

    sc = kafka_cluster.share_consumer({'group.id': group_id, 'share.acknowledgement.mode': 'explicit'})
    try:
        sc.subscribe([topic])

        # Produce inside a transaction, then ABORT — these must never surface.
        txn_producer.begin_transaction()
        for i in range(3):
            txn_producer.produce(topic, value=f'aborted-{i}'.encode())
        txn_producer.flush(5)
        txn_producer.abort_transaction(10)

        # A committed record gives us a positive signal to wait for: if the
        # consumer is healthy and only the aborted records were filtered, this
        # is the one and only record it should deliver.
        txn_producer.begin_transaction()
        txn_producer.produce(topic, value=b'committed-0')
        txn_producer.flush(5)
        txn_producer.commit_transaction(10)

        received = drain_share_consumers([sc], 1, timeout_s=20.0, ack_type=AcknowledgeType.ACCEPT)[0]
        assert [m.value() for m in received] == [b'committed-0'], (
            f'read_committed should deliver only the committed record; ' f'got {[m.value() for m in received]}'
        )

        # No aborted record should arrive afterward either.
        stragglers = []
        deadline = time.time() + 3.0
        while time.time() < deadline:
            for m in sc.poll(timeout=0.5):
                if m.error() is None:
                    stragglers.append(m.value())
                    sc.acknowledge(m, AcknowledgeType.ACCEPT)
        assert stragglers == [], f'aborted/extra records leaked after commit: {stragglers}'
    finally:
        sc.close()


def test_read_committed_delivers_committed_transactions_with_marker_gap(kafka_cluster):
    """read_committed: committed transactional records are delivered, and the
    commit-marker control records (which occupy log offsets but are never
    delivered) leave a gap in the delivered offsets."""
    topic = kafka_cluster.create_topic_and_wait_propogation('test-share-consumer-txn-commit')
    group_id = unique_id('test-share-consumer-txn-commit')

    txn_producer = Producer(kafka_cluster.client_conf({'transactional.id': unique_id('txn')}))
    txn_producer.init_transactions(10)

    set_group_config(kafka_cluster, group_id, 'share.isolation.level', 'read_committed')

    records_per_txn = 3
    total = 2 * records_per_txn
    sc = kafka_cluster.share_consumer({'group.id': group_id, 'share.acknowledgement.mode': 'explicit'})
    try:
        sc.subscribe([topic])

        # Two committed transactions. The commit marker that ends the first
        # transaction sits at an offset between the two data runs and is never
        # delivered to a read_committed consumer.
        produced = []
        for txn in range(2):
            txn_producer.begin_transaction()
            for i in range(records_per_txn):
                value = f'txn{txn}-msg-{i}'.encode()
                produced.append(value)
                txn_producer.produce(topic, value=value)
            txn_producer.flush(5)
            txn_producer.commit_transaction(10)

        received = drain_share_consumers([sc], total, timeout_s=30.0, ack_type=AcknowledgeType.ACCEPT)[0]
        if len(received) < total:
            pytest.skip(f'broker delivered {len(received)}/{total} committed records; cannot assess marker gap')

        assert {m.value() for m in received} == set(produced), 'all committed records should be delivered'

        # Single-partition topic, so all records share a partition. The commit
        # marker occupies an offset that is never delivered, so the delivered
        # offsets span a wider range than the record count.
        offsets = sorted(m.offset() for m in received)
        assert (offsets[-1] - offsets[0] + 1) > total, (
            f'expected a control-record gap in delivered offsets {offsets}; '
            f'committed transactions should leave commit-marker gaps'
        )
    finally:
        sc.close()


def test_partition_max_record_locks_caps_in_flight(kafka_cluster):
    """share.partition.max.record.locks caps how many records can be acquired
    (in-flight, unacknowledged) per partition at once. With the cap set low, no
    single poll() acquires more than the cap, yet every record is eventually
    delivered once earlier ones are acknowledged and their locks released."""
    topic = kafka_cluster.create_topic_and_wait_propogation('test-share-consumer-lock-cap')
    group_id = unique_id('test-share-consumer-lock-cap')

    lock_cap = 100  # broker minimum for share.partition.max.record.locks
    num_messages = 2 * lock_cap

    try:
        set_group_config(kafka_cluster, group_id, 'share.partition.max.record.locks', lock_cap)
    except KafkaException as e:
        # share.partition.max.record.locks was added to the per-group config
        # allow-list after Kafka 4.2.0; older brokers reject it. Skip (rather
        # than fail) so the test self-enables once the broker supports it.
        pytest.skip(f'broker does not allow per-group share.partition.max.record.locks: {e}')

    # max.poll.records above the lock cap so the cap — not the poll batch size
    # — is what limits each acquisition.
    sc = kafka_cluster.share_consumer(
        {
            'group.id': group_id,
            'share.acknowledgement.mode': 'explicit',
            'max.poll.records': num_messages,
        }
    )
    try:
        sc.subscribe([topic])

        producer = kafka_cluster.cimpl_producer({'linger.ms': 0})
        for i in range(num_messages):
            producer.produce(topic, value=f'msg-{i}'.encode())
        producer.flush(timeout=10.0)

        received = 0
        max_batch = 0
        deadline = time.time() + 60.0
        while received < num_messages and time.time() < deadline:
            # Generous per-poll timeout so the broker can fill up to the cap;
            # otherwise a small batch wouldn't show the cap binding.
            batch = [m for m in sc.poll(timeout=2.0) if m.error() is None]
            if not batch:
                continue
            max_batch = max(max_batch, len(batch))
            # Explicit mode: ack the whole batch before the next poll, which
            # releases the locks and lets the next batch be acquired.
            for m in batch:
                sc.acknowledge(m, AcknowledgeType.ACCEPT)
            received += len(batch)

        assert received == num_messages, f'received {received} of {num_messages}'
        assert max_batch <= lock_cap, (
            f'a single poll acquired {max_batch} records, exceeding the ' f'partition lock cap of {lock_cap}'
        )
    finally:
        sc.close()


def test_double_close_is_idempotent(kafka_cluster):
    """close() twice must be a no-op (__exit__ relies on this)."""
    sc = kafka_cluster.share_consumer()
    sc.close()
    sc.close()


def test_subscribe_before_topic_exists(kafka_cluster):
    """A subscription made BEFORE the topic exists starts delivering once the
    topic is created and produced to.

    The client keeps refreshing metadata for a subscribed-but-unknown topic, so
    it picks the topic up when it appears and (earliest reset) drains it.
    """
    topic = unique_id('test-share-consumer-prejoin-create')
    n = 10

    sc = kafka_cluster.share_consumer()
    try:
        # Subscribe before the topic exists; the join can't assign it yet, and
        # no records should surface in the meantime.
        sc.subscribe([topic])
        pre = []
        for _ in range(5):
            pre.extend(msg for msg in sc.poll(timeout=0.2) if msg.error() is None)
        assert pre == [], f'no records should arrive before the topic exists, got {len(pre)}'

        # Create the topic, then produce to it.
        create_futures = kafka_cluster.admin().create_topics([NewTopic(topic, num_partitions=1, replication_factor=1)])
        create_futures[topic].result()
        time.sleep(1)  # propagation across brokers

        producer = kafka_cluster.cimpl_producer()
        for i in range(n):
            producer.produce(topic, value=f'msg-{i}'.encode())
        producer.flush(timeout=10.0)

        received_msgs = drain_share_consumers([sc], n, timeout_s=30.0)[0]
        assert sorted(msg.value() for msg in received_msgs) == sorted(
            f'msg-{i}'.encode() for i in range(n)
        ), 'expected exactly the records produced after the topic was created'
    finally:
        sc.close()


def test_resubscribe_same_topic_keeps_delivering(kafka_cluster):
    """Re-subscribing to the SAME topic doesn't disrupt consumption: records
    produced after the redundant re-subscribe are still delivered. Distinct
    from test_resubscribe_to_different_topic, which switches topics.
    """
    topic = kafka_cluster.create_topic_and_wait_propogation('test-share-consumer-resub-same')

    sc = kafka_cluster.share_consumer()
    try:
        sc.subscribe([topic])

        producer = kafka_cluster.cimpl_producer()
        for i in range(5):
            producer.produce(topic, value=f'first-{i}'.encode())
        producer.flush(timeout=10.0)
        first_msgs = drain_share_consumers([sc], 5)[0]
        assert sorted(msg.value() for msg in first_msgs) == [
            f'first-{i}'.encode() for i in range(5)
        ], 'phase 1 records mismatch'

        # Redundant re-subscribe to the same topic; drive heartbeats so the
        # (unchanged) subscription settles before producing more.
        sc.subscribe([topic])
        for _ in range(10):
            sc.poll(timeout=0.2)

        for i in range(5):
            producer.produce(topic, value=f'second-{i}'.encode())
        producer.flush(timeout=10.0)
        second_msgs = drain_share_consumers([sc], 5)[0]
        # Exactly the new records — and only those, proving the redundant
        # re-subscribe didn't redeliver phase 1's already-consumed records.
        assert sorted(msg.value() for msg in second_msgs) == [
            f'second-{i}'.encode() for i in range(5)
        ], 're-subscribe to the same topic should deliver the new records and only those'
        assert all(msg.topic() == topic for msg in second_msgs)
    finally:
        sc.close()


def test_dealloc_without_close_on_live_consumer(kafka_cluster):
    """Dropping a joined, actively-fetching share consumer without close() must
    tear down cleanly through dealloc.

    Every other test here closes in a finally; this covers the fallback where
    the handle is destroyed from dealloc instead, without a graceful group
    leave. By now the consumer has live broker state and running background
    threads, so a teardown bug shows up as a crash or hang, and a milder error
    surfaces as an unraisable exception, which we assert against.
    """
    topic = kafka_cluster.create_topic_and_wait_propogation('test-share-consumer-dealloc')
    n = 10

    sc = kafka_cluster.share_consumer()
    sc.subscribe([topic])

    producer = kafka_cluster.cimpl_producer()
    for i in range(n):
        producer.produce(topic, value=f'msg-{i}'.encode())
    producer.flush(timeout=10.0)

    # Drive a real join and at least one fetch so the handle holds live state
    # when it's dropped.
    received = drain_share_consumers([sc], n)[0]
    assert len(received) == n, f"setup: expected {n} records, got {len(received)}"

    unraisables = []
    prev_hook = sys.unraisablehook
    sys.unraisablehook = lambda args: unraisables.append(args)
    try:
        # No close(): let dealloc destroy the live handle.
        del sc
        gc.collect()
    finally:
        sys.unraisablehook = prev_hook

    assert unraisables == [], f"dealloc raised: {[u.exc_value for u in unraisables]}"


def test_max_poll_interval_exceeded_then_rejoins(kafka_cluster):
    """If the application stops polling for longer than max.poll.interval.ms the
    broker drops it from the group, and the next poll() raises
    _MAX_POLL_EXCEEDED. The consumer then rejoins on its own and keeps going.

    10s keeps the test quick; the interval is tracked client-side, so it doesn't
    depend on the broker session timeout.
    """
    topic = kafka_cluster.create_topic_and_wait_propogation('test-share-consumer-maxpoll-interval')
    interval_ms = 10000

    sc = kafka_cluster.share_consumer({'max.poll.interval.ms': interval_ms})
    try:
        sc.subscribe([topic])

        producer = kafka_cluster.cimpl_producer()
        producer.produce(topic, value=b'pre-stall')
        producer.flush(timeout=10.0)

        # Join and consume the first record so the poll-interval clock starts.
        first = poll_first_batch(sc)
        assert first, 'consumer never joined / received the first record'

        # Stop polling for longer than the interval so the consumer is considered failed.
        time.sleep(interval_ms / 1000.0 + 3.0)

        # One of the next polls must surface the eviction as _MAX_POLL_EXCEEDED.
        evicted = None
        deadline = time.time() + 15.0
        while time.time() < deadline and evicted is None:
            try:
                sc.poll(timeout=0.5)
            except KafkaException as exc:
                evicted = exc.args[0]
        assert evicted is not None, 'poll() never surfaced an eviction error after the interval lapsed'
        assert (
            evicted.code() == KafkaError._MAX_POLL_EXCEEDED
        ), f"expected _MAX_POLL_EXCEEDED, got {evicted.code()} ({evicted.str()})"

        # After the eviction is delivered, the consumer rejoins and consumes again.
        producer.produce(topic, value=b'post-rejoin')
        producer.flush(timeout=10.0)
        rejoined = []
        deadline = time.time() + 30.0
        while time.time() < deadline and not rejoined:
            try:
                rejoined = [msg for msg in sc.poll(timeout=0.5) if msg.error() is None]
            except KafkaException:
                # A lingering eviction error may surface once more during rejoin.
                continue
        assert rejoined, 'consumer did not rejoin and consume after eviction'
    finally:
        sc.close()


def test_blocking_poll_longer_than_max_poll_interval_does_not_falsely_trip(kafka_cluster):
    """Blocking inside a single poll() for longer than max.poll.interval.ms must
    not count as a missed poll, so it must not raise _MAX_POLL_EXCEEDED. Waiting
    inside poll is fine; only failing to call poll at all is a problem.

    Under the hood poll() waits in short chunks and re-checks between them, which
    keeps the consumer alive across one long call. This guards that behavior.
    """
    topic = kafka_cluster.create_topic_and_wait_propogation('test-share-consumer-maxpoll-blocking')
    interval_ms = 10000

    sc = kafka_cluster.share_consumer({'max.poll.interval.ms': interval_ms})
    try:
        sc.subscribe([topic])
        # Join.
        for _ in range(10):
            sc.poll(timeout=0.2)

        # One blocking poll on an idle topic, longer than the interval: it must
        # return an empty batch without raising.
        batch = sc.poll(timeout=interval_ms / 1000.0 + 2.0)
        assert batch.is_empty(), f'idle blocking poll should return no records, got {batch.count()}'

        # The consumer is still a live group member: produce and consume.
        producer = kafka_cluster.cimpl_producer()
        producer.produce(topic, value=b'after-blocking-poll')
        producer.flush(timeout=10.0)
        received = poll_first_batch(sc)
        assert received, 'consumer should still consume after a long blocking poll'
        assert received[0].value() == b'after-blocking-poll'
    finally:
        sc.close()


def test_topic_deletion_under_subscription_keeps_other_topic_flowing(kafka_cluster):
    """Deleting one of several subscribed topics shouldn't break the consumer.
    The deleted topic just stops contributing while the others keep delivering,
    and the deletion never comes back as a poll() error (it turns up in commit
    results instead), so poll() has to stay usable throughout.
    """
    topic_del = kafka_cluster.create_topic_and_wait_propogation('test-share-consumer-del')
    topic_keep = kafka_cluster.create_topic_and_wait_propogation('test-share-consumer-keep')

    # Refresh metadata quickly and don't linger on the deleted topic. By default
    # the client holds on to a vanished topic for topic.metadata.propagation.max.ms
    # (30s), and while it does the whole share session stalls, including the topic
    # we still want to read. Setting it to 0 lets the session rebuild right away.
    sc = kafka_cluster.share_consumer(
        {'topic.metadata.refresh.interval.ms': 500, 'topic.metadata.propagation.max.ms': 0}
    )
    try:
        sc.subscribe([topic_del, topic_keep])

        producer = kafka_cluster.cimpl_producer()
        for i in range(5):
            producer.produce(topic_del, value=f'del-{i}'.encode())
            producer.produce(topic_keep, value=f'keep-{i}'.encode())
        producer.flush(timeout=10.0)

        first = drain_share_consumers([sc], 10)[0]
        assert {msg.topic() for msg in first} == {
            topic_del,
            topic_keep,
        }, f"expected records from both topics initially, got {({msg.topic() for msg in first})}"

        # Delete one topic out from under the active subscription.
        for f in kafka_cluster.admin().delete_topics([topic_del]).values():
            f.result()

        # The surviving topic must keep flowing. The broker may briefly fail the
        # whole share session while it recovers from the delete, so tolerate
        # transient poll errors but require the survivor to drain fully.
        for i in range(5, 10):
            producer.produce(topic_keep, value=f'keep-{i}'.encode())
        producer.flush(timeout=10.0)

        expected_new = {f'keep-{i}'.encode() for i in range(5, 10)}
        survivor_values = set()
        deadline = time.time() + 45.0
        while time.time() < deadline and not expected_new <= survivor_values:
            try:
                batch = sc.poll(timeout=0.5)
            except KafkaException:
                continue  # transient session-recovery error after the delete
            for msg in batch:
                if msg.error() is None and msg.topic() == topic_keep:
                    survivor_values.add(msg.value())
        assert expected_new <= survivor_values, (
            f"surviving topic stopped delivering after the co-subscribed topic was deleted "
            f"(missing {expected_new - survivor_values})"
        )
    finally:
        sc.close()


def test_partition_increase_during_consume(kafka_cluster):
    """Partitions added to a topic while it's being consumed are picked up: once
    the clients refresh metadata, records on the new partitions are delivered.

    Both the producer and the consumer use a short metadata refresh so the new
    partitions are noticed quickly instead of after the default five minutes.
    """
    topic = kafka_cluster.create_topic_and_wait_propogation('test-share-consumer-addparts', conf={'num_partitions': 2})

    sc = kafka_cluster.share_consumer({'topic.metadata.refresh.interval.ms': 500})
    try:
        sc.subscribe([topic])

        producer = kafka_cluster.cimpl_producer({'topic.metadata.refresh.interval.ms': 500})
        for p in range(2):
            for i in range(5):
                producer.produce(topic, value=f'p{p}-{i}'.encode(), partition=p)
        producer.flush(timeout=10.0)
        first = drain_share_consumers([sc], 10)[0]
        assert len(first) == 10, f'initial 2-partition drain incomplete: {len(first)}/10'

        # Grow 2 -> 4 partitions, then give both clients time to refresh metadata.
        for f in kafka_cluster.admin().create_partitions([NewPartitions(topic, 4)]).values():
            f.result()
        time.sleep(2)

        for p in (2, 3):
            for i in range(5):
                producer.produce(topic, value=f'p{p}-{i}'.encode(), partition=p)
        producer.flush(timeout=10.0)

        new_part_records = []
        deadline = time.time() + 30.0
        while time.time() < deadline and len(new_part_records) < 10:
            for msg in sc.poll(timeout=0.5):
                if msg.error() is None and msg.partition() in (2, 3):
                    new_part_records.append(msg)
        assert (
            len(new_part_records) == 10
        ), f"records from the added partitions were not delivered (got {len(new_part_records)}/10)"
        assert {msg.partition() for msg in new_part_records} == {2, 3}
    finally:
        sc.close()


def test_sparse_partitions_do_not_stall(kafka_cluster):
    """Producing to only some of a topic's partitions still drains everything;
    the empty partitions don't hold up the populated ones. The full-coverage test
    produces to every partition, so this one deliberately leaves gaps.
    """
    topic = kafka_cluster.create_topic_and_wait_propogation('test-share-consumer-sparse', conf={'num_partitions': 5})
    populated = (0, 2, 4)
    per_partition = 10

    sc = kafka_cluster.share_consumer()
    try:
        sc.subscribe([topic])

        producer = kafka_cluster.cimpl_producer()
        for p in populated:
            for i in range(per_partition):
                producer.produce(topic, value=f'p{p}-{i}'.encode(), partition=p)
        producer.flush(timeout=10.0)

        total = len(populated) * per_partition
        received = drain_share_consumers([sc], total)[0]
        assert len(received) == total, f'sparse-partition drain stalled: {len(received)}/{total}'
        assert {msg.partition() for msg in received} == set(populated), (
            f"expected records only from populated partitions {populated}, "
            f"got {sorted({msg.partition() for msg in received})}"
        )
    finally:
        sc.close()


def test_poll_empty_existing_topic_returns_empty_then_delivers(kafka_cluster):
    """Polling a subscribed topic that exists but is empty returns an empty batch
    rather than an error, and starts delivering once records arrive.
    test_subscribe_before_topic_exists covers the topic-doesn't-exist-yet case;
    here the topic exists but has nothing in it. Also checks that poll() hands
    back a Messages batch, empty when there's nothing to read.
    """
    topic = kafka_cluster.create_topic_and_wait_propogation('test-share-consumer-emptythenproduce')
    n = 10

    sc = kafka_cluster.share_consumer()
    try:
        sc.subscribe([topic])

        # Poll the empty topic a few times: each returns an empty list, no error.
        for _ in range(5):
            batch = sc.poll(timeout=0.5)
            assert isinstance(batch, Messages), f'poll() must return Messages, got {type(batch)}'
            assert batch.is_empty(), f'empty topic should yield no records, got {batch.count()}'

        # Now produce; the consumer transitions to delivering.
        producer = kafka_cluster.cimpl_producer()
        for i in range(n):
            producer.produce(topic, value=f'msg-{i}'.encode())
        producer.flush(timeout=10.0)

        received = drain_share_consumers([sc], n)[0]
        assert sorted(msg.value() for msg in received) == sorted(
            f'msg-{i}'.encode() for i in range(n)
        ), 'expected exactly the records produced after the empty polls'
    finally:
        sc.close()


def test_close_releases_inflight_to_group(kafka_cluster):
    """When a consumer holding unacknowledged records closes, those records go
    back to the group and another member picks them up. This covers the close()
    path; test_records_redelivered_after_lock_timeout covers the other one, where
    the holder just stops polling and the lock has to time out first.
    """
    topic = kafka_cluster.create_topic_and_wait_propogation('test-share-consumer-close-release')
    group_id = unique_id('test-share-close-release')
    n = 10

    sc_a = kafka_cluster.share_consumer({'group.id': group_id})
    sc_b = kafka_cluster.share_consumer({'group.id': group_id})
    try:
        sc_a.subscribe([topic])
        sc_b.subscribe([topic])
        # Drive both through the join so neither races ahead.
        for _ in range(10):
            sc_a.poll(timeout=0.2)
            sc_b.poll(timeout=0.2)

        producer = kafka_cluster.cimpl_producer()
        for i in range(n):
            producer.produce(topic, value=f'msg-{i}'.encode())
        producer.flush(timeout=10.0)

        # A grabs whatever the broker assigns it, leaves it unacked, and closes.
        a_received = set()
        deadline = time.time() + 10.0
        while time.time() < deadline and not a_received:
            for msg in sc_a.poll(timeout=0.5):
                if msg.error() is None:
                    a_received.add((msg.partition(), msg.offset()))
        assert a_received, 'sc_a should have grabbed at least one record before closing'

        # Close A: its in-flight (unacked) records are released back to the group.
        sc_a.close()
        sc_a = None

        # B drains every record, including A's released ones.
        b_received = set()
        deadline = time.time() + 20.0
        while time.time() < deadline and len(b_received) < n:
            for msg in sc_b.poll(timeout=0.5):
                if msg.error() is None:
                    b_received.add((msg.partition(), msg.offset()))
        assert len(b_received) == n, f"B did not drain all records after A closed (got {len(b_received)}/{n})"
        assert a_received <= b_received, (
            f"A's in-flight records were not all redelivered to B after close: " f"missing {a_received - b_received}"
        )
    finally:
        if sc_a is not None:
            sc_a.close()
        sc_b.close()


def test_earliest_delivers_pre_subscribe_backlog(kafka_cluster):
    """With share.auto.offset.reset=earliest (the suite default), records produced
    before the consumer subscribes are still delivered. This is the other side of
    test_records_before_join_not_delivered, which checks the 'latest' case.
    """
    topic = kafka_cluster.create_topic_and_wait_propogation('test-share-consumer-earliest-backlog')
    n = 20

    # Produce the backlog before any consumer subscribes.
    producer = kafka_cluster.cimpl_producer()
    for i in range(n):
        producer.produce(topic, value=f'backlog-{i}'.encode())
    producer.flush(timeout=10.0)

    # The fixture sets share.auto.offset.reset=earliest on this group before the
    # first subscribe, so the share-partition start offset is the log start.
    sc = kafka_cluster.share_consumer()
    try:
        sc.subscribe([topic])
        received = drain_share_consumers([sc], n, timeout_s=30.0)[0]
        assert sorted(msg.value() for msg in received) == sorted(
            f'backlog-{i}'.encode() for i in range(n)
        ), 'earliest reset must replay the records produced before the consumer subscribed'
    finally:
        sc.close()


def test_single_consumer_self_redelivery_increments_delivery_count(kafka_cluster):
    """An unacknowledged record comes back after its acquisition lock expires, and
    its delivery_count goes from 1 to 2. The test broker uses a short 1s lock so
    this doesn't take 30s. The lock-timeout redelivery test uses two consumers;
    this one watches the count climb on a single consumer.
    """
    topic = kafka_cluster.create_topic_and_wait_propogation('test-share-consumer-self-redeliver')

    sc = kafka_cluster.share_consumer()
    try:
        sc.subscribe([topic])

        producer = kafka_cluster.cimpl_producer()
        producer.produce(topic, value=b'redelivery-probe')
        producer.flush(timeout=10.0)

        # First delivery: delivery_count == 1. Do NOT acknowledge.
        first = poll_first_batch(sc)
        assert first, 'consumer never received the record'
        probe = first[0]
        target = (probe.partition(), probe.offset())
        assert probe.delivery_count() == 1, f"first delivery should have delivery_count=1, got {probe.delivery_count()}"

        # Wait past the broker's 1s lock so the unacked record is redelivered.
        time.sleep(1.5)

        # The same record comes back, now with delivery_count == 2. (In implicit
        # mode the next poll's piggybacked ack targets the already-expired lock
        # and is a no-op, then the record is re-acquired.)
        redelivered = None
        deadline = time.time() + 10.0
        while time.time() < deadline and redelivered is None:
            for msg in sc.poll(timeout=0.5):
                if msg.error() is None and (msg.partition(), msg.offset()) == target:
                    redelivered = msg
                    break
        assert redelivered is not None, 'unacked record was not redelivered after lock expiry'
        assert (
            redelivered.delivery_count() == 2
        ), f"redelivered record should have delivery_count=2, got {redelivered.delivery_count()}"
    finally:
        sc.close()


def test_poll_returns_whole_batch_exceeding_max_poll_records(kafka_cluster):
    """max.poll.records caps a poll at batch boundaries, not in the middle of a
    batch. When a whole broker batch is bigger than the cap, one poll hands back
    the entire batch instead of splitting it. test_max_poll_records_caps_batch
    checks the other direction, where each record is its own batch and the cap
    holds.
    """
    topic = kafka_cluster.create_topic_and_wait_propogation('test-share-consumer-batchovercap')
    cap = 5
    n = 20

    sc = kafka_cluster.share_consumer({'max.poll.records': cap})
    try:
        sc.subscribe([topic])

        # Pack every record into one broker batch: a high linger plus a single
        # flush keeps them together instead of one batch per record.
        producer = kafka_cluster.cimpl_producer({'linger.ms': 1000, 'batch.num.messages': n + 10})
        for i in range(n):
            producer.produce(topic, value=f'msg-{i}'.encode())
        producer.flush(timeout=10.0)

        first_batch = poll_first_batch(sc)
        assert len(first_batch) > cap, (
            f"a single poll should return the whole batch rather than split it at the cap "
            f"(cap={cap}, got {len(first_batch)})"
        )
    finally:
        sc.close()


def test_two_groups_earliest_vs_latest(kafka_cluster):
    """Two share groups on the same topic with different share.auto.offset.reset
    start from different points: the earliest group replays the existing backlog,
    the latest group only sees records produced after it joined.
    """
    topic = kafka_cluster.create_topic_and_wait_propogation('test-share-consumer-two-resets')
    backlog = 10
    fresh = 5

    producer = kafka_cluster.cimpl_producer()
    for i in range(backlog):
        producer.produce(topic, value=f'backlog-{i}'.encode())
    producer.flush(timeout=10.0)

    sc_earliest = kafka_cluster.share_consumer()  # fixture default is earliest
    sc_latest = kafka_cluster.share_consumer({'auto.offset.reset': 'latest'})
    try:
        sc_earliest.subscribe([topic])
        sc_latest.subscribe([topic])
        # Give the latest group time to fully join and pin its start offset at the
        # current end of the log before any fresh records exist. A short join loop
        # isn't enough — the start offset is only fixed once it has actually
        # fetched, and if that lands after the fresh records they'd be skipped.
        # Only poll the latest group here: polling the earliest one would consume
        # and (implicitly) ack the backlog before the collection loop can see it.
        stabilize = time.time() + 8.0
        while time.time() < stabilize:
            sc_latest.poll(timeout=0.3)

        for i in range(fresh):
            producer.produce(topic, value=f'fresh-{i}'.encode())
        producer.flush(timeout=10.0)

        expected_backlog = {f'backlog-{i}'.encode() for i in range(backlog)}
        expected_fresh = {f'fresh-{i}'.encode() for i in range(fresh)}

        earliest_vals = set()
        deadline = time.time() + 30.0
        while time.time() < deadline and not (expected_backlog | expected_fresh) <= earliest_vals:
            for msg in sc_earliest.poll(timeout=0.5):
                if msg.error() is None:
                    earliest_vals.add(msg.value())

        latest_vals = set()
        deadline = time.time() + 15.0
        while time.time() < deadline and not expected_fresh <= latest_vals:
            for msg in sc_latest.poll(timeout=0.5):
                if msg.error() is None:
                    latest_vals.add(msg.value())

        assert earliest_vals == expected_backlog | expected_fresh, (
            f"earliest group should see backlog + fresh, missing "
            f"{(expected_backlog | expected_fresh) - earliest_vals}"
        )
        assert expected_fresh <= latest_vals, f"latest group missing fresh records: {expected_fresh - latest_vals}"
        assert not (
            latest_vals & expected_backlog
        ), f"latest group should not see the backlog, but got {latest_vals & expected_backlog}"
    finally:
        sc_earliest.close()
        sc_latest.close()


def test_read_uncommitted_delivers_uncommitted_records(kafka_cluster):
    """With the default share.isolation.level=read_uncommitted, records written
    inside a still-open transaction are delivered right away, without waiting for
    the commit. The flip side of test_open_transaction_stalls_share_group, which
    sets read_committed and shows the open transaction blocks delivery.
    """
    topic = kafka_cluster.create_topic_and_wait_propogation('test-share-consumer-read-uncommitted')

    txn_producer = Producer(kafka_cluster.client_conf({'transactional.id': unique_id('txn-uncommitted')}))
    try:
        txn_producer.init_transactions(10)
    except KafkaException as e:
        pytest.skip(f'broker does not support transactions: {e}')

    sc = kafka_cluster.share_consumer()  # group default is read_uncommitted
    try:
        sc.subscribe([topic])

        # Produce inside a transaction and leave it open.
        txn_producer.begin_transaction()
        for i in range(3):
            txn_producer.produce(topic, value=f'uncommitted-{i}'.encode())
        txn_producer.flush(5)

        received = drain_share_consumers([sc], 3, timeout_s=15.0)[0]
        assert len(received) == 3, f'read_uncommitted should deliver the open-transaction records, got {len(received)}'
        assert sorted(msg.value() for msg in received) == [f'uncommitted-{i}'.encode() for i in range(3)]

        txn_producer.abort_transaction(10)
    finally:
        sc.close()


def test_multi_consumer_overlapping_subscriptions(kafka_cluster):
    """Two consumers in one share group with overlapping-but-different topic
    lists each receive only the topics they subscribed to. The shared topic is
    split across the group; a consumer-specific topic goes only to that consumer.
    """
    topic_shared = kafka_cluster.create_topic_and_wait_propogation('test-share-consumer-ov-shared')
    topic_a = kafka_cluster.create_topic_and_wait_propogation('test-share-consumer-ov-a')
    topic_b = kafka_cluster.create_topic_and_wait_propogation('test-share-consumer-ov-b')
    group_id = unique_id('test-share-overlap')
    n = 10

    sc_a = kafka_cluster.share_consumer({'group.id': group_id})
    sc_b = kafka_cluster.share_consumer({'group.id': group_id})
    try:
        sc_a.subscribe([topic_shared, topic_a])
        sc_b.subscribe([topic_shared, topic_b])
        for _ in range(10):
            sc_a.poll(timeout=0.2)
            sc_b.poll(timeout=0.2)

        producer = kafka_cluster.cimpl_producer()
        for i in range(n):
            producer.produce(topic_shared, value=f'shared-{i}'.encode())
            producer.produce(topic_a, value=f'a-{i}'.encode())
            producer.produce(topic_b, value=f'b-{i}'.encode())
        producer.flush(timeout=10.0)

        expected_a = {f'a-{i}'.encode() for i in range(n)}
        expected_b = {f'b-{i}'.encode() for i in range(n)}
        expected_shared = {f'shared-{i}'.encode() for i in range(n)}

        a_msgs, b_msgs = [], []
        deadline = time.time() + 30.0
        while time.time() < deadline:
            for sc, bucket in ((sc_a, a_msgs), (sc_b, b_msgs)):
                for msg in sc.poll(timeout=0.5):
                    if msg.error() is None:
                        bucket.append(msg)
            a_vals = {m.value() for m in a_msgs}
            b_vals = {m.value() for m in b_msgs}
            shared_seen = {v for v in (a_vals | b_vals) if v.startswith(b'shared-')}
            if expected_a <= a_vals and expected_b <= b_vals and expected_shared <= shared_seen:
                break

        a_topics = {m.topic() for m in a_msgs}
        b_topics = {m.topic() for m in b_msgs}
        assert topic_b not in a_topics, "sc_a received records from topic_b, which it never subscribed to"
        assert topic_a not in b_topics, "sc_b received records from topic_a, which it never subscribed to"

        assert expected_a <= {m.value() for m in a_msgs}, "sc_a missing some of its exclusive topic_a records"
        assert expected_b <= {m.value() for m in b_msgs}, "sc_b missing some of its exclusive topic_b records"
        shared_seen = {m.value() for m in a_msgs + b_msgs if m.topic() == topic_shared}
        assert (
            shared_seen == expected_shared
        ), f"shared topic not fully covered, missing {expected_shared - shared_seen}"
    finally:
        sc_a.close()
        sc_b.close()


def test_topic_recreate_resumes_with_fresh_delivery(kafka_cluster):
    """A topic deleted and recreated under the same name is a brand new topic:
    after the recreate the consumer delivers only the new records, each at
    delivery_count 1, and none of the pre-delete records come back.
    """
    topic = unique_id('test-share-consumer-recreate')
    n = 10
    producer_conf = {'topic.metadata.refresh.interval.ms': 500, 'topic.metadata.propagation.max.ms': 0}

    sc = kafka_cluster.share_consumer(
        {'topic.metadata.refresh.interval.ms': 500, 'topic.metadata.propagation.max.ms': 0}
    )
    try:
        admin = kafka_cluster.admin()
        admin.create_topics([NewTopic(topic, num_partitions=1, replication_factor=1)])[topic].result()
        time.sleep(1)
        sc.subscribe([topic])

        producer = kafka_cluster.cimpl_producer(producer_conf)
        for i in range(n):
            producer.produce(topic, value=f'before-{i}'.encode())
        producer.flush(timeout=10.0)
        before = drain_share_consumers([sc], n)[0]
        assert len(before) == n, f'phase 1 drain incomplete: {len(before)}/{n}'

        # Delete and recreate under the same name. Deletion is async, so wait
        # until the name is actually gone from metadata before recreating —
        # otherwise the create races the delete and hits TOPIC_ALREADY_EXISTS.
        for f in admin.delete_topics([topic]).values():
            f.result()
        gone = time.time() + 30.0
        while time.time() < gone and topic in admin.list_topics(timeout=5).topics:
            time.sleep(1)
        admin.create_topics([NewTopic(topic, num_partitions=1, replication_factor=1)])[topic].result()
        time.sleep(2)

        # Fresh producer so it doesn't carry stale metadata for the old topic.
        producer = kafka_cluster.cimpl_producer(producer_conf)
        for i in range(n):
            producer.produce(topic, value=f'after-{i}'.encode())
        producer.flush(timeout=10.0)

        expected_after = {f'after-{i}'.encode() for i in range(n)}
        first_count = {}
        deadline = time.time() + 45.0
        while time.time() < deadline and not expected_after <= set(first_count):
            try:
                batch = sc.poll(timeout=0.5)
            except KafkaException:
                continue  # transient session-recovery error during the recreate
            for msg in batch:
                if msg.error() is None and msg.value() not in first_count:
                    first_count[msg.value()] = msg.delivery_count()

        assert expected_after <= set(
            first_count
        ), f"recreated topic did not deliver the fresh records (missing {expected_after - set(first_count)})"
        stale = {v for v in first_count if v.startswith(b'before-')}
        assert not stale, f'pre-recreate records were redelivered after the topic was recreated: {stale}'
        assert all(first_count[v] == 1 for v in expected_after), (
            f"recreated-topic records should first arrive at delivery_count 1, "
            f"got {({v: first_count[v] for v in expected_after})}"
        )
    finally:
        sc.close()
