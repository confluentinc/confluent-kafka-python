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

import time

from tests.common import (
    drain_share_consumers,
    unique_id,
)


def test_concurrent_consumers(kafka_cluster):
    """Two consumers in the same share group must receive disjoint records,
    and their union must cover every produced record exactly once.

    Per-consumer distribution is intentionally NOT asserted: with a single
    partition, serial round-robin polling, and batch_size=10005, a single
    ShareFetch from whichever consumer polls first can drain all records
    before the other gets a chance. KIP-932 share groups don't guarantee
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

        received_1, received_2 = drain_share_consumers([sc1, sc2], n_messages)
        offsets1 = {(m.topic(), m.partition(), m.offset()) for m in received_1}
        offsets2 = {(m.topic(), m.partition(), m.offset()) for m in received_2}

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

        received = drain_share_consumers([sc], n)[0]
        values = sorted(m.value() for m in received)
        assert values == sorted(expected), f"Value mismatch: expected {sorted(expected)}, got {values}"
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

        received = drain_share_consumers([sc], 5)[0]
        assert len(received) == 5

        got = sorted([(m.key(), m.value(), m.headers()) for m in received])
        exp = sorted(produced)
        assert got == exp, f"Field mismatch: expected {exp}, got {got}"
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

        received = drain_share_consumers([sc], 2 * n_per_topic)[0]
        topics_seen = {m.topic() for m in received}
        assert topics_seen == {topic_a, topic_b}, f"Expected both topics, got {topics_seen}"
        assert (
            len(received) == 2 * n_per_topic
        ), f"Expected {2 * n_per_topic} records across both topics, got {len(received)}"
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

        received = drain_share_consumers(consumers, n, poll_timeout_s=0.2)
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

        received_a, received_b = drain_share_consumers([sc_a, sc_b], 2 * n)
        offsets_a = {(m.topic(), m.partition(), m.offset()) for m in received_a}
        offsets_b = {(m.topic(), m.partition(), m.offset()) for m in received_b}

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
            for m in sc1.poll(timeout=0.5):
                if m.error() is None:
                    sc1_received.add((m.partition(), m.offset()))
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
            for m in sc2.poll(timeout=0.5):
                if m.error() is None:
                    sc2_received.add((m.partition(), m.offset()))

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
            for m in sc.poll(timeout=0):
                if m.error() is None:
                    collected.append((m.partition(), m.offset()))

        assert len(collected) == n, (
            f"poll(timeout=0) tight-loop should deliver all {n} records, " f"got {len(collected)}"
        )
    finally:
        sc.close()


def test_unsubscribe_stops_delivery(kafka_cluster):
    """After unsubscribe, future polls return no records even when broker has new ones."""
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

        post = []
        deadline = time.time() + 5.0
        while time.time() < deadline:
            for m in sc.poll(timeout=0.5):
                if m.error() is None:
                    post.append(m)

        assert post == [], f"Records delivered after unsubscribe: {len(post)} messages"
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

        first = drain_share_consumers([sc], 3)[0]
        assert len(first) == 3, f"Failed to consume from topic_a (got {len(first)}/3)"
        assert all(
            m.topic() == topic_a for m in first
        ), f"Expected only topic_a records, got {[m.topic() for m in first]}"

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

        received = drain_share_consumers([sc], 5)[0]
        topics = {m.topic() for m in received}
        assert topics == {topic_b}, f"Resubscribe should drop topic_a; got topics {topics}"
        assert len(received) == 5, f"Expected 5 topic_b records, got {len(received)}"
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
            for m in sc.poll(timeout=0.5):
                if m.error() is None:
                    per_partition.setdefault(m.partition(), []).append(m.offset())
                    total += 1

        assert total == n, f"Expected {n} records, got {total}"

        for p, offsets in per_partition.items():
            assert offsets == sorted(offsets), f"Partition {p} offsets out of order: {offsets}"
    finally:
        sc.close()
