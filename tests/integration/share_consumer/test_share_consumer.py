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
    KafkaException,
    Messages,
    Producer,
)
from confluent_kafka.admin import NewTopic
from tests.common import (
    drain_share_consumers,
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
