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

"""Integration tests for ShareConsumer.commit_sync() / commit_async() (KIP-932)."""

import threading
import time

import pytest

from confluent_kafka import AcknowledgeType, IllegalStateException, KafkaError, KafkaException
from confluent_kafka.admin import AlterConfigOpType, ConfigEntry, ConfigResource, ResourceType
from tests.common import drain_share_consumers, poll_ack_commit_loop, poll_first_batch, unique_id

# --- happy path -----------------------------------------------------------


def test_implicit_commit_sync_returns_partition_results(kafka_cluster):
    """Implicit acks + per-batch commit_sync: every commit returns a
    non-empty {TopicPartition: None} dict."""
    topic = kafka_cluster.create_topic_and_wait_propogation('test-share-consumer-commit-implicit-sync')
    num_messages = 3

    sc = kafka_cluster.share_consumer({'share.acknowledgement.mode': 'implicit'})
    try:
        sc.subscribe([topic])

        producer = kafka_cluster.cimpl_producer()
        produced = [f'msg-{i}'.encode() for i in range(num_messages)]
        for value in produced:
            producer.produce(topic, value=value)
        producer.flush(timeout=10.0)

        msgs, commit_results = poll_ack_commit_loop(sc, num_messages)
        received_values = {msg.value() for msg in msgs}
        assert received_values == set(
            produced
        ), f'sc missed records: produced={set(produced)} received={received_values}'

        assert commit_results, 'no commit_sync results recorded'
        for result in commit_results:
            assert result, 'commit_sync returned an empty result; expected per-partition entries'
            for tp, err in result.items():
                assert err is None, f'unexpected error: {err}'
    finally:
        sc.close()


def test_implicit_commit_async_returns_immediately(kafka_cluster):
    """Async commit returns None and doesn't block."""
    topic = kafka_cluster.create_topic_and_wait_propogation('test-share-consumer-commit-implicit-async')
    num_messages = 3

    sc = kafka_cluster.share_consumer({'share.acknowledgement.mode': 'implicit'})
    try:
        sc.subscribe([topic])

        producer = kafka_cluster.cimpl_producer()
        produced = [f'msg-{i}'.encode() for i in range(num_messages)]
        for value in produced:
            producer.produce(topic, value=value)
        producer.flush(timeout=10.0)

        msgs = drain_share_consumers([sc], num_messages)[0]
        received_values = {msg.value() for msg in msgs}
        assert received_values == set(
            produced
        ), f'sc missed records: produced={set(produced)} received={received_values}'

        start = time.monotonic()
        result = sc.commit_async()
        elapsed = time.monotonic() - start

        assert result is None
        assert elapsed < 1.0, f'async commit blocked for {elapsed:.2f}s'
    finally:
        sc.close()


def test_explicit_commit_sync_succeeds(kafka_cluster):
    """ACCEPT each record then commit_sync per batch. Every batch's
    result should be non-empty with no per-partition errors."""
    topic = kafka_cluster.create_topic_and_wait_propogation('test-share-consumer-commit-explicit-accept')
    num_messages = 3

    sc = kafka_cluster.share_consumer({'share.acknowledgement.mode': 'explicit'})
    try:
        sc.subscribe([topic])

        producer = kafka_cluster.cimpl_producer()
        produced = [f'msg-{i}'.encode() for i in range(num_messages)]
        for value in produced:
            producer.produce(topic, value=value)
        producer.flush(timeout=10.0)

        msgs, commit_results = poll_ack_commit_loop(sc, num_messages, ack_type=AcknowledgeType.ACCEPT)
        received_values = {msg.value() for msg in msgs}
        assert received_values == set(
            produced
        ), f'sc missed records: produced={set(produced)} received={received_values}'

        assert commit_results, 'no commit_sync results recorded'
        for result in commit_results:
            assert result, 'commit_sync returned an empty result; acks piggybacked on the next poll'
            for tp, err in result.items():
                assert err is None
    finally:
        sc.close()


def test_commit_with_nothing_pending_returns_empty(kafka_cluster):
    """Commit after a few empty polls returns an empty dict (sync) /
    None (async) when there are no acks to send."""
    topic = kafka_cluster.create_topic_and_wait_propogation('test-share-consumer-commit-empty')

    sc = kafka_cluster.share_consumer({'share.acknowledgement.mode': 'implicit'})
    try:
        sc.subscribe([topic])
        # Drive a few empty polls so the share session is past the join
        # handshake before the commit calls.
        for _ in range(5):
            sc.poll(timeout=0.2)

        assert sc.commit_sync(timeout=2.0) == {}
        assert sc.commit_async() is None
    finally:
        sc.close()


def test_repeated_commit_returns_empty_on_second_call(kafka_cluster):
    """First commits should carry real per-partition results. A
    back-to-back commit_sync right after has nothing pending, so it
    should return {}."""
    topic = kafka_cluster.create_topic_and_wait_propogation('test-share-consumer-commit-repeated')
    num_messages = 2

    sc = kafka_cluster.share_consumer({'share.acknowledgement.mode': 'explicit'})
    try:
        sc.subscribe([topic])

        producer = kafka_cluster.cimpl_producer()
        produced = [f'msg-{i}'.encode() for i in range(num_messages)]
        for value in produced:
            producer.produce(topic, value=value)
        producer.flush(timeout=10.0)

        msgs, commit_results = poll_ack_commit_loop(sc, num_messages, ack_type=AcknowledgeType.ACCEPT)
        received_values = {msg.value() for msg in msgs}
        assert received_values == set(
            produced
        ), f'sc missed records: produced={set(produced)} received={received_values}'

        assert commit_results, 'no commit_sync results recorded'
        assert commit_results[0], 'first commit_sync returned empty; expected real per-partition results'

        second = sc.commit_sync(timeout=2.0)
        assert second == {}
    finally:
        sc.close()


def test_commit_with_large_batch(kafka_cluster):
    """10k records committed across many small batches. Exercises the
    ack-batch build, partition-list conversion, and broker round-trip
    at scale, and checks every batch's commit_sync result, not just
    the last one."""
    topic = kafka_cluster.create_topic_and_wait_propogation('test-share-consumer-commit-large-batch')
    num_messages = 10000

    sc = kafka_cluster.share_consumer({'share.acknowledgement.mode': 'implicit'})
    try:
        sc.subscribe([topic])

        producer = kafka_cluster.cimpl_producer()
        for i in range(num_messages):
            producer.produce(topic, value=f'msg-{i}'.encode())
        producer.flush(timeout=60.0)

        msgs, commit_results = poll_ack_commit_loop(sc, num_messages, deadline_s=60.0, commit_timeout_s=30.0)
        assert len(msgs) >= num_messages // 2, f'only drained {len(msgs)}/{num_messages}'

        # However the broker batches the ShareFetch response, every
        # batch we committed must come back with a non-empty
        # per-partition result and no errors. (Batch count is
        # broker-side non-deterministic so we don't pin it down.)
        assert commit_results, 'no commit_sync results recorded'
        for result in commit_results:
            assert result, 'commit_sync returned an empty result; expected per-partition entries'
            for tp, err in result.items():
                assert err is None, f'{tp.topic}[{tp.partition}] -> {err}'
    finally:
        sc.close()


# --- AcknowledgeType behaviors --------------------------------------------


def test_reject_persists_through_commit(kafka_cluster):
    """REJECT followed by commit archives the record permanently."""
    topic = kafka_cluster.create_topic_and_wait_propogation('test-share-consumer-commit-reject')
    group_id = unique_id('test-share-consumer-commit-reject')

    sc1 = kafka_cluster.share_consumer({'group.id': group_id, 'share.acknowledgement.mode': 'explicit'})
    try:
        sc1.subscribe([topic])

        producer = kafka_cluster.cimpl_producer()
        producer.produce(topic, value=b'msg-0')
        producer.flush(timeout=10.0)

        msgs, commit_results = poll_ack_commit_loop(sc1, 1, ack_type=AcknowledgeType.REJECT)
        assert len(msgs) == 1

        assert commit_results, 'no commit_sync results recorded'
        for result in commit_results:
            for tp, err in result.items():
                assert err is None
    finally:
        sc1.close()

    sc2 = kafka_cluster.share_consumer({'group.id': group_id, 'share.acknowledgement.mode': 'explicit'})
    try:
        sc2.subscribe([topic])
        leftovers = drain_share_consumers([sc2], 1, timeout_s=5.0, ack_type=AcknowledgeType.ACCEPT)[0]
        assert leftovers == [], 'REJECT should permanently archive'
    finally:
        sc2.close()


def test_release_through_commit_returns_record_to_available(kafka_cluster):
    """RELEASE + commit: the record comes back on a later poll, and
    the broker bumps delivery_count to 2 to flag it as a redelivery."""
    topic = kafka_cluster.create_topic_and_wait_propogation('test-share-consumer-commit-release')

    sc = kafka_cluster.share_consumer({'share.acknowledgement.mode': 'explicit'})
    try:
        sc.subscribe([topic])

        producer = kafka_cluster.cimpl_producer()
        producer.produce(topic, value=b'msg-0')
        producer.flush(timeout=10.0)

        first_batch = poll_first_batch(sc)
        assert first_batch
        first_msg = first_batch[0]
        assert (
            first_msg.delivery_count() == 1
        ), f'first delivery should have delivery_count=1, got {first_msg.delivery_count()}'

        sc.acknowledge(first_msg, AcknowledgeType.RELEASE)
        result = sc.commit_sync(timeout=10.0)
        for tp, err in result.items():
            assert err is None

        deadline = time.time() + 15.0
        redelivered = None
        while redelivered is None and time.time() < deadline:
            for msg in sc.poll(timeout=2.0):
                if msg.error() is None:
                    if msg.delivery_count() >= 2:
                        redelivered = msg
                    sc.acknowledge(msg, AcknowledgeType.ACCEPT)
                    if redelivered is not None:
                        break
        assert redelivered is not None, 'released record never came back as redelivery'
        assert (
            redelivered.delivery_count() == 2
        ), f'redelivery should have delivery_count=2, got {redelivered.delivery_count()}'
    finally:
        sc.close()


def test_commit_with_mixed_ack_types(kafka_cluster):
    """Mix ACCEPT/RELEASE/REJECT in one batch and commit. All three
    should land cleanly, and any RELEASE'd records should come back on
    a later poll with delivery_count == 2."""
    topic = kafka_cluster.create_topic_and_wait_propogation('test-share-consumer-commit-mixed')
    num_messages = 6

    sc = kafka_cluster.share_consumer({'share.acknowledgement.mode': 'explicit'})
    try:
        sc.subscribe([topic])

        producer = kafka_cluster.cimpl_producer()
        for i in range(num_messages):
            producer.produce(topic, value=f'msg-{i}'.encode())
        producer.flush(timeout=10.0)

        ack_types = [
            AcknowledgeType.ACCEPT,
            AcknowledgeType.RELEASE,
            AcknowledgeType.REJECT,
        ]
        released_offsets = set()
        acked_count = 0
        deadline = time.time() + 10.0
        while acked_count < num_messages and time.time() < deadline:
            for msg in sc.poll(timeout=2.0):
                if msg.error() is None:
                    ack_type = ack_types[acked_count % 3]
                    if ack_type is AcknowledgeType.RELEASE:
                        released_offsets.add((msg.partition(), msg.offset()))
                    sc.acknowledge(msg, ack_type)
                    acked_count += 1
                    if acked_count >= num_messages:
                        break
        assert acked_count >= 3, f'only got {acked_count} records'
        assert released_offsets, 'no RELEASE acks issued, so we cannot check redelivery'

        result = sc.commit_sync(timeout=10.0)
        for tp, err in result.items():
            assert err is None

        # Every record we RELEASE'd should come back with
        # delivery_count == 2, at the offset we actually released.
        redelivered_offsets = set()
        deadline = time.time() + 15.0
        while redelivered_offsets != released_offsets and time.time() < deadline:
            for msg in sc.poll(timeout=2.0):
                if msg.error() is None:
                    coords = (msg.partition(), msg.offset())
                    assert msg.delivery_count() >= 2, (
                        f'unexpected first-delivery record {coords} during redelivery window '
                        f'(delivery_count={msg.delivery_count()})'
                    )
                    assert (
                        coords in released_offsets
                    ), f'redelivered record {coords} was not in released set {released_offsets}'
                    redelivered_offsets.add(coords)
                    sc.acknowledge(msg, AcknowledgeType.ACCEPT)
        assert (
            redelivered_offsets == released_offsets
        ), f'released {released_offsets} but only saw redelivery of {redelivered_offsets}'
    finally:
        sc.close()


def test_committed_releases_archive_at_delivery_limit(kafka_cluster):
    """RELEASE + commit five times in a row (the broker default) should
    get the record auto-archived as poison. We also check that
    delivery_count goes 1, 2, 3, 4, 5 so we'd catch a broker that
    re-delivers without bumping the counter."""
    topic = kafka_cluster.create_topic_and_wait_propogation('test-share-consumer-commit-poison')

    sc = kafka_cluster.share_consumer({'share.acknowledgement.mode': 'explicit'})
    try:
        sc.subscribe([topic])

        producer = kafka_cluster.cimpl_producer()
        producer.produce(topic, value=b'poison-0')
        producer.flush(timeout=10.0)

        poison_coords = None
        delivery_counts_seen = []
        deadline = time.time() + 60.0
        while len(delivery_counts_seen) < 5 and time.time() < deadline:
            for msg in sc.poll(timeout=2.0):
                if msg.error() is None:
                    if poison_coords is None:
                        poison_coords = (msg.topic(), msg.partition(), msg.offset())
                    else:
                        # We only produced one record, so every later
                        # delivery has to be the same one.
                        assert (msg.topic(), msg.partition(), msg.offset()) == poison_coords
                    delivery_counts_seen.append(msg.delivery_count())
                    sc.acknowledge(msg, AcknowledgeType.RELEASE)
                    sc.commit_sync(timeout=5.0)
                    if len(delivery_counts_seen) >= 5:
                        break
        assert delivery_counts_seen == [
            1,
            2,
            3,
            4,
            5,
        ], f'expected delivery_count progression [1,2,3,4,5], got {delivery_counts_seen}'

        deadline = time.time() + 10.0
        while time.time() < deadline:
            for msg in sc.poll(timeout=2.0):
                if msg.error() is None and (msg.topic(), msg.partition(), msg.offset()) == poison_coords:
                    pytest.fail(f'record {poison_coords} redelivered after 5 releases')
    finally:
        sc.close()


# --- mode enforcement -----------------------------------------------------


def test_implicit_acknowledge_raises_commit_still_works(kafka_cluster):
    """In implicit mode acknowledge() raises _STATE; commit() is fine
    (it auto-converts ACQUIRED → ACCEPT)."""
    topic = kafka_cluster.create_topic_and_wait_propogation('test-share-consumer-commit-implicit-ack')

    sc = kafka_cluster.share_consumer({'share.acknowledgement.mode': 'implicit'})
    try:
        sc.subscribe([topic])

        producer = kafka_cluster.cimpl_producer()
        producer.produce(topic, value=b'msg-0')
        producer.flush(timeout=10.0)

        msgs = drain_share_consumers([sc], 1)[0]
        assert msgs

        with pytest.raises(IllegalStateException) as ex:
            sc.acknowledge(msgs[0], AcknowledgeType.ACCEPT)
        assert str(ex.value)

        result = sc.commit_sync(timeout=10.0)
        assert isinstance(result, dict)
    finally:
        sc.close()


def test_partial_ack_commit_then_unacked_blocks_poll(kafka_cluster):
    """Explicit mode: commit the acked records; the unacked ones still
    block the next poll with _STATE."""
    topic = kafka_cluster.create_topic_and_wait_propogation('test-share-consumer-commit-partial')

    sc = kafka_cluster.share_consumer({'share.acknowledgement.mode': 'explicit'})
    try:
        sc.subscribe([topic])

        # Produce many so the first poll likely returns >=2.
        producer = kafka_cluster.cimpl_producer()
        for i in range(10):
            producer.produce(topic, value=f'msg-{i}'.encode())
        producer.flush(timeout=10.0)

        gathered_msgs = []
        deadline = time.time() + 10.0
        while not gathered_msgs and time.time() < deadline:
            for msg in sc.poll(timeout=2.0):
                if msg.error() is None:
                    gathered_msgs.append(msg)
        assert gathered_msgs

        if len(gathered_msgs) < 2:
            pytest.skip(f'broker returned only {len(gathered_msgs)} record; need >=2 to exercise partial ack')

        sc.acknowledge(gathered_msgs[0], AcknowledgeType.ACCEPT)
        sc.commit_sync(timeout=10.0)

        with pytest.raises(IllegalStateException) as ex:
            sc.poll(timeout=2.0)
        assert str(ex.value)

        for msg in gathered_msgs[1:]:
            sc.acknowledge(msg, AcknowledgeType.ACCEPT)
        sc.commit_sync(timeout=5.0)
    finally:
        sc.close()


def test_explicit_commit_after_implicit_autocommit_is_noop(kafka_cluster):
    """Implicit mode auto-commits on the second poll; an explicit commit
    after that has nothing to send."""
    topic = kafka_cluster.create_topic_and_wait_propogation('test-share-consumer-commit-after-autocommit')
    num_messages = 3

    sc = kafka_cluster.share_consumer({'share.acknowledgement.mode': 'implicit'})
    try:
        sc.subscribe([topic])

        producer = kafka_cluster.cimpl_producer()
        produced = [f'msg-{i}'.encode() for i in range(num_messages)]
        for value in produced:
            producer.produce(topic, value=value)
        producer.flush(timeout=10.0)

        msgs = drain_share_consumers([sc], num_messages)[0]
        received_values = {msg.value() for msg in msgs}
        assert received_values == set(
            produced
        ), f'sc missed records: produced={set(produced)} received={received_values}'
        sc.poll(timeout=2.0)  # auto-commits the previous batch

        assert sc.commit_sync(timeout=2.0) == {}
    finally:
        sc.close()


def test_explicit_commit_with_no_acks_is_noop(kafka_cluster):
    """Explicit mode + zero acknowledge() calls: commit_sync is an empty
    no-op (mirrors the implicit-mode nothing-pending case)."""
    topic = kafka_cluster.create_topic_and_wait_propogation('test-share-consumer-commit-explicit-no-acks')

    sc = kafka_cluster.share_consumer({'share.acknowledgement.mode': 'explicit'})
    try:
        sc.subscribe([topic])
        for _ in range(5):
            sc.poll(timeout=0.2)
        assert sc.commit_sync(timeout=2.0) == {}
    finally:
        sc.close()


# --- state-machine edge cases --------------------------------------------


def test_commit_after_lock_expiry(kafka_cluster):
    """After the broker's lock duration expires, the local acknowledge()
    call still succeeds — there is no client-side lock check; lock
    state is broker-side. The broker rejects at commit time, surfacing
    INVALID_RECORD_STATE in the per-partition commit results.

    Relies on the suite-wide 1s lock duration from broker_conf().
    """
    topic = kafka_cluster.create_topic_and_wait_propogation('test-share-consumer-commit-lock-expiry')

    sc = kafka_cluster.share_consumer({'share.acknowledgement.mode': 'explicit'})
    try:
        sc.subscribe([topic])

        producer = kafka_cluster.cimpl_producer()
        producer.produce(topic, value=b'msg-0')
        producer.flush(timeout=10.0)

        msgs = poll_first_batch(sc)
        assert msgs

        # Wait past the broker's 1s lock duration. The batch transitions
        # ACQUIRED -> AVAILABLE on the broker; the local in-flight table
        # is unaffected.
        time.sleep(1.5)

        # Local ack succeeds — there is no client-side lock-expiry check.
        sc.acknowledge(msgs[0], AcknowledgeType.ACCEPT)

        # Broker rejects the stale ack with INVALID_RECORD_STATE.
        result = sc.commit_sync(timeout=10.0)
        assert any(
            err is not None and err.code() == KafkaError.INVALID_RECORD_STATE for err in result.values()
        ), f'expected INVALID_RECORD_STATE in {result}'
    finally:
        sc.close()


def test_lock_steal_with_committed_ack(kafka_cluster):
    """A holds a record past lock expiry; B fetches and commits it; A's
    late commit must not crash. Uses the suite-wide 1s lock duration."""
    topic = kafka_cluster.create_topic_and_wait_propogation('test-share-consumer-commit-lock-steal')
    group_id = unique_id('test-share-consumer-commit-lock-steal')

    sc_a = kafka_cluster.share_consumer({'group.id': group_id, 'share.acknowledgement.mode': 'explicit'})
    sc_b = kafka_cluster.share_consumer({'group.id': group_id, 'share.acknowledgement.mode': 'explicit'})
    try:
        sc_a.subscribe([topic])

        producer = kafka_cluster.cimpl_producer()
        producer.produce(topic, value=b'msg-0')
        producer.flush(timeout=10.0)

        a_batch = poll_first_batch(sc_a)
        assert a_batch
        a_msg = a_batch[0]

        # Wait past the broker's 1s lock duration so sc_b can steal.
        time.sleep(1.5)

        sc_b.subscribe([topic])
        b_got = False
        deadline_b = time.time() + 10.0
        while not b_got and time.time() < deadline_b:
            try:
                for msg in sc_b.poll(timeout=2.0):
                    if msg.error() is None:
                        sc_b.acknowledge(msg, AcknowledgeType.ACCEPT)
                        b_got = True
                        break
            except KafkaException:
                pass
        if b_got:
            sc_b.commit_sync(timeout=5.0)

        try:
            sc_a.acknowledge(a_msg, AcknowledgeType.ACCEPT)
        except KafkaException as e:
            assert e.args[0].code() in (KafkaError._STATE, KafkaError.INVALID_RECORD_STATE)
            return

        result = sc_a.commit_sync(timeout=10.0)
        assert isinstance(result, dict)
    finally:
        sc_a.close()
        sc_b.close()


def test_commit_after_acknowledge_unknown_offset(kafka_cluster):
    """Acking an unknown offset raises _STATE locally; a subsequent
    commit is a no-op."""
    topic = kafka_cluster.create_topic_and_wait_propogation('test-share-consumer-commit-unknown-offset')

    sc = kafka_cluster.share_consumer({'share.acknowledgement.mode': 'explicit'})
    try:
        sc.subscribe([topic])

        with pytest.raises(IllegalStateException) as ex:
            sc.acknowledge_offset(topic, 0, 99999, AcknowledgeType.ACCEPT)
        assert str(ex.value)

        assert sc.commit_sync(timeout=2.0) == {}
    finally:
        sc.close()


# --- per-partition error reporting ----------------------------------------


def test_per_partition_commit_results_all_succeed(kafka_cluster):
    """3 partitions, no induced errors. Each batch's commit_sync should
    map every touched partition to None, and taken together the
    committed partitions should match the ones we actually polled from."""
    topic = kafka_cluster.create_topic_and_wait_propogation(
        'test-share-consumer-commit-3p-clean',
        conf={'num_partitions': 3},
    )

    sc = kafka_cluster.share_consumer({'share.acknowledgement.mode': 'explicit'})
    try:
        sc.subscribe([topic])

        producer = kafka_cluster.cimpl_producer()
        for partition in range(3):
            producer.produce(topic, value=f'p{partition}-0'.encode(), partition=partition)
        producer.flush(timeout=10.0)

        msgs, commit_results = poll_ack_commit_loop(sc, 3, ack_type=AcknowledgeType.ACCEPT)
        assert msgs, 'no records received'

        partitions_polled = {msg.partition() for msg in msgs}

        assert commit_results, 'no commit_sync results recorded'
        partitions_committed = set()
        for result in commit_results:
            assert result, 'commit_sync returned an empty result; expected per-partition entries'
            for tp, err in result.items():
                assert err is None, f'{tp.topic}[{tp.partition}] -> {err}'
                partitions_committed.add(tp.partition)

        assert (
            partitions_committed == partitions_polled
        ), f'commit results cover {partitions_committed} but polled from {partitions_polled}'
    finally:
        sc.close()


def test_per_partition_commit_results_with_lock_expiry(kafka_cluster):
    """Force lock expiry, then ack+commit: per-partition errors may or
    may not appear (late acks may be accepted silently in some paths).
    The contract is no crash and any error is _STATE/INVALID_RECORD_STATE.
    Uses the suite-wide 1s lock duration."""
    topic = kafka_cluster.create_topic_and_wait_propogation(
        'test-share-consumer-commit-2p-mixed',
        conf={'num_partitions': 2},
    )

    sc = kafka_cluster.share_consumer({'share.acknowledgement.mode': 'explicit'})
    try:
        sc.subscribe([topic])

        producer = kafka_cluster.cimpl_producer()
        producer.produce(topic, value=b'p0-0', partition=0)
        producer.produce(topic, value=b'p1-0', partition=1)
        producer.flush(timeout=10.0)

        # Single poll only — must not poll again before acking.
        msgs = []
        deadline = time.time() + 10.0
        while not msgs and time.time() < deadline:
            for msg in sc.poll(timeout=2.0):
                if msg.error() is None:
                    msgs.append(msg)
        assert msgs

        time.sleep(1.5)

        for msg in msgs:
            try:
                sc.acknowledge(msg, AcknowledgeType.ACCEPT)
            except KafkaException:
                pass

        result = sc.commit_sync(timeout=10.0)
        assert isinstance(result, dict)
        for tp, err in result.items():
            if err is not None:
                assert err.code() in (KafkaError._STATE, KafkaError.INVALID_RECORD_STATE)
    finally:
        sc.close()


def test_commit_spans_multiple_topics(kafka_cluster):
    """Subscribe to two topics, produce one record to each, then run
    the poll/ack/commit_sync loop. Each batch's result should only
    mention the subscribed topics and no errors, and across all
    batches we should see both topics we polled from."""
    topic_a = kafka_cluster.create_topic_and_wait_propogation('test-share-consumer-commit-multi-topic-a')
    topic_b = kafka_cluster.create_topic_and_wait_propogation('test-share-consumer-commit-multi-topic-b')

    sc = kafka_cluster.share_consumer({'share.acknowledgement.mode': 'explicit'})
    try:
        sc.subscribe([topic_a, topic_b])

        producer = kafka_cluster.cimpl_producer()
        producer.produce(topic_a, value=b'a-0')
        producer.produce(topic_b, value=b'b-0')
        producer.flush(timeout=10.0)

        msgs, commit_results = poll_ack_commit_loop(sc, 2, ack_type=AcknowledgeType.ACCEPT)
        topics_polled = {msg.topic() for msg in msgs}
        assert topics_polled, 'no records received'

        assert commit_results, 'no commit_sync results recorded'
        topics_committed = set()
        for result in commit_results:
            assert result, 'commit_sync returned an empty result; expected per-partition entries'
            for tp, err in result.items():
                assert err is None, f'{tp.topic}[{tp.partition}] -> {err}'
                assert tp.topic in (topic_a, topic_b)
                topics_committed.add(tp.topic)

        assert (
            topics_committed == topics_polled
        ), f'commit results cover {topics_committed} but polled from {topics_polled}'
    finally:
        sc.close()


def test_commit_on_closed_consumer_raises(kafka_cluster):
    """commit() on a closed consumer raises RuntimeError before reaching
    the C layer."""
    topic = kafka_cluster.create_topic_and_wait_propogation('test-share-consumer-commit-closed')

    sc = kafka_cluster.share_consumer({'share.acknowledgement.mode': 'implicit'})
    sc.subscribe([topic])
    sc.close()

    with pytest.raises(RuntimeError) as ex:
        sc.commit_sync(timeout=1.0)
    assert 'closed' in str(ex.value).lower()

    with pytest.raises(RuntimeError) as ex:
        sc.commit_async()
    assert 'closed' in str(ex.value).lower()


# --- sync vs async timing -------------------------------------------------


def test_async_commit_does_not_block(kafka_cluster):
    """Async commit returns within 500ms regardless of pending acks."""
    topic = kafka_cluster.create_topic_and_wait_propogation('test-share-consumer-commit-async-fast')

    sc = kafka_cluster.share_consumer({'share.acknowledgement.mode': 'implicit'})
    try:
        sc.subscribe([topic])

        producer = kafka_cluster.cimpl_producer()
        for i in range(5):
            producer.produce(topic, value=f'msg-{i}'.encode())
        producer.flush(timeout=10.0)
        drain_share_consumers([sc], 5)

        start = time.monotonic()
        result = sc.commit_async()
        elapsed = time.monotonic() - start

        assert result is None
        assert elapsed < 0.5, f'async commit took {elapsed:.3f}s'
    finally:
        sc.close()


def test_commit_with_zero_timeout_returns_fast(kafka_cluster):
    """timeout=0 returns immediately."""
    topic = kafka_cluster.create_topic_and_wait_propogation('test-share-consumer-commit-zero-timeout')

    sc = kafka_cluster.share_consumer({'share.acknowledgement.mode': 'implicit'})
    try:
        sc.subscribe([topic])

        start = time.monotonic()
        result = sc.commit_sync(timeout=0)
        elapsed = time.monotonic() - start

        assert isinstance(result, dict)
        assert elapsed < 1.0, f'commit_sync(timeout=0) took {elapsed:.3f}s'
    finally:
        sc.close()


def test_sync_commit_releases_gil(kafka_cluster):
    """A CPU-bound background thread should make progress while sync
    commit blocks — confirms PyEval_SaveThread() is wired."""
    topic = kafka_cluster.create_topic_and_wait_propogation('test-share-consumer-commit-gil')

    counter = {'count': 0}
    stop_event = threading.Event()

    def worker():
        while not stop_event.is_set():
            counter['count'] += 1

    worker_thread = threading.Thread(target=worker, daemon=True)
    worker_thread.start()

    sc = kafka_cluster.share_consumer({'share.acknowledgement.mode': 'implicit'})
    try:
        sc.subscribe([topic])

        producer = kafka_cluster.cimpl_producer()
        for i in range(5):
            producer.produce(topic, value=f'msg-{i}'.encode())
        producer.flush(timeout=10.0)
        drain_share_consumers([sc], 5)

        before = counter['count']
        sc.commit_sync(timeout=2.0)
        after = counter['count']

        assert (
            after - before > 100
        ), f'background thread made only {after - before} increments — GIL likely not released'
    finally:
        stop_event.set()
        worker_thread.join(timeout=2.0)
        sc.close()


def test_commit_with_short_timeout_does_not_crash(kafka_cluster):
    """A very small but nonzero timeout returns quickly. The commit may
    race through to a clean dict or surface a timeout per-partition;
    either is acceptable. The contract here is no crash and bounded
    wall-clock."""
    topic = kafka_cluster.create_topic_and_wait_propogation('test-share-consumer-commit-short-timeout')
    num_messages = 3

    sc = kafka_cluster.share_consumer({'share.acknowledgement.mode': 'implicit'})
    try:
        sc.subscribe([topic])

        producer = kafka_cluster.cimpl_producer()
        for i in range(num_messages):
            producer.produce(topic, value=f'msg-{i}'.encode())
        producer.flush(timeout=10.0)
        drain_share_consumers([sc], num_messages)

        start = time.monotonic()
        try:
            result = sc.commit_sync(timeout=0.001)
            assert isinstance(result, dict)
        except KafkaException:
            pass
        elapsed = time.monotonic() - start
        assert elapsed < 2.0, f'commit_sync(timeout=0.001) took {elapsed:.3f}s'
    finally:
        sc.close()


def test_commit_with_infinite_timeout_returns_when_idle(kafka_cluster):
    """commit_sync(timeout=-1) is the RD_POLL_INFINITE sentinel. With
    nothing pending it must still return promptly (no broker round trip
    needed)."""
    topic = kafka_cluster.create_topic_and_wait_propogation('test-share-consumer-commit-infinite-timeout')

    sc = kafka_cluster.share_consumer({'share.acknowledgement.mode': 'implicit'})
    try:
        sc.subscribe([topic])

        start = time.monotonic()
        result = sc.commit_sync(timeout=-1)
        elapsed = time.monotonic() - start

        assert isinstance(result, dict)
        assert elapsed < 5.0, f'idle commit_sync(timeout=-1) blocked {elapsed:.3f}s'
    finally:
        sc.close()


def test_commit_sync_accepts_float_seconds_timeout(kafka_cluster):
    """commit_sync(timeout=<float>) parses; float seconds is the
    convention shared with the regular Consumer."""
    topic = kafka_cluster.create_topic_and_wait_propogation('test-share-consumer-commit-float-timeout')

    sc = kafka_cluster.share_consumer({'share.acknowledgement.mode': 'implicit'})
    try:
        sc.subscribe([topic])
        result = sc.commit_sync(timeout=2.5)
        assert isinstance(result, dict)
    finally:
        sc.close()


def test_commit_sync_rejects_non_numeric_timeout(kafka_cluster):
    """Non-numeric timeout is rejected by PyArg_ParseTuple before
    reaching the C layer."""
    topic = kafka_cluster.create_topic_and_wait_propogation('test-share-consumer-commit-bad-timeout')

    sc = kafka_cluster.share_consumer({'share.acknowledgement.mode': 'implicit'})
    try:
        sc.subscribe([topic])
        with pytest.raises(TypeError):
            sc.commit_sync(timeout='abc')
        with pytest.raises(TypeError):
            sc.commit_sync(timeout=None)
    finally:
        sc.close()


# --- lifecycle ------------------------------------------------------------


def test_commit_before_close_persists_acks(kafka_cluster):
    """commit() before close() makes acks stick — a fresh consumer in
    the same group sees no redelivery. (close() alone does NOT flush.)"""
    topic = kafka_cluster.create_topic_and_wait_propogation('test-share-consumer-commit-before-close')
    group_id = unique_id('test-share-consumer-commit-before-close')
    num_messages = 3

    sc1 = kafka_cluster.share_consumer({'group.id': group_id, 'share.acknowledgement.mode': 'explicit'})
    try:
        sc1.subscribe([topic])

        producer = kafka_cluster.cimpl_producer()
        produced = [f'msg-{i}'.encode() for i in range(num_messages)]
        for value in produced:
            producer.produce(topic, value=value)
        producer.flush(timeout=10.0)

        msgs, commit_results = poll_ack_commit_loop(sc1, num_messages, ack_type=AcknowledgeType.ACCEPT)
        received_values = {msg.value() for msg in msgs}
        assert received_values == set(
            produced
        ), f'sc1 missed records: produced={set(produced)} received={received_values}'
        assert commit_results, 'no commit_sync results recorded'
    finally:
        sc1.close()

    # Implicit verifier so any redelivery doesn't raise _STATE.
    sc2 = kafka_cluster.share_consumer({'group.id': group_id, 'share.acknowledgement.mode': 'implicit'})
    try:
        sc2.subscribe([topic])
        leftovers = []
        deadline = time.time() + 8.0
        while time.time() < deadline:
            for msg in sc2.poll(timeout=1.0):
                if msg.error() is None:
                    leftovers.append(msg.value())
        assert leftovers == [], f'commit did not persist; got: {leftovers}'
    finally:
        sc2.close()


def test_commit_before_first_poll_is_noop(kafka_cluster):
    """commit() right after subscribe() returns an empty dict (sync) /
    None (async)."""
    topic = kafka_cluster.create_topic_and_wait_propogation('test-share-consumer-commit-pre-poll')

    sc = kafka_cluster.share_consumer({'share.acknowledgement.mode': 'implicit'})
    try:
        sc.subscribe([topic])
        assert sc.commit_sync(timeout=2.0) == {}
        assert sc.commit_async() is None
    finally:
        sc.close()


def test_commit_before_subscribe_is_noop(kafka_cluster):
    """commit() on a brand-new consumer that never subscribed: KIP
    doesn't define this; pin down the current behavior (a clean no-op
    is the stable answer)."""
    sc = kafka_cluster.share_consumer({'share.acknowledgement.mode': 'implicit'})
    try:
        assert sc.commit_sync(timeout=2.0) == {}
        assert sc.commit_async() is None
    finally:
        sc.close()


def test_async_then_sync_commit_both_complete(kafka_cluster):
    """commit_async() immediately followed by commit_sync(): both
    resolve cleanly without one blocking or duplicating the other."""
    topic = kafka_cluster.create_topic_and_wait_propogation('test-share-consumer-commit-async-then-sync')
    num_messages = 3

    sc = kafka_cluster.share_consumer({'share.acknowledgement.mode': 'implicit'})
    try:
        sc.subscribe([topic])

        producer = kafka_cluster.cimpl_producer()
        for i in range(num_messages):
            producer.produce(topic, value=f'msg-{i}'.encode())
        producer.flush(timeout=10.0)
        drain_share_consumers([sc], num_messages)

        assert sc.commit_async() is None
        result = sc.commit_sync(timeout=10.0)
        assert isinstance(result, dict)
        for tp, err in result.items():
            assert err is None
    finally:
        sc.close()


def test_commit_after_unsubscribe_does_not_crash(kafka_cluster):
    """commit() after unsubscribe(): KIP doesn't define this; we just
    pin down the current behavior and make sure it's stable."""
    topic = kafka_cluster.create_topic_and_wait_propogation('test-share-consumer-commit-unsub')
    num_messages = 2

    sc = kafka_cluster.share_consumer({'share.acknowledgement.mode': 'implicit'})
    try:
        sc.subscribe([topic])

        producer = kafka_cluster.cimpl_producer()
        for i in range(num_messages):
            producer.produce(topic, value=f'msg-{i}'.encode())
        producer.flush(timeout=10.0)
        drain_share_consumers([sc], num_messages)

        sc.unsubscribe()
        result = sc.commit_sync(timeout=5.0)
        assert isinstance(result, dict)
    finally:
        sc.close()


# --- configuration boundary -----------------------------------------------


def test_commit_with_explicit_mode(kafka_cluster):
    """share.acknowledgement.mode=explicit parses and works end-to-end
    in the poll/ack/commit_sync loop."""
    topic = kafka_cluster.create_topic_and_wait_propogation('test-share-consumer-commit-explicit-cfg')
    num_messages = 2

    sc = kafka_cluster.share_consumer({'share.acknowledgement.mode': 'explicit'})
    try:
        sc.subscribe([topic])

        producer = kafka_cluster.cimpl_producer()
        for i in range(num_messages):
            producer.produce(topic, value=f'msg-{i}'.encode())
        producer.flush(timeout=10.0)

        msgs, commit_results = poll_ack_commit_loop(sc, num_messages, ack_type=AcknowledgeType.ACCEPT)
        assert len(msgs) >= 1

        assert commit_results, 'no commit_sync results recorded'
        for result in commit_results:
            assert result, 'commit_sync returned an empty result; acks piggybacked on the next poll'
            for tp, err in result.items():
                assert err is None
    finally:
        sc.close()


# --- commit_async --------------------------------------------------------
#
# commit_async returns immediately and surfaces broker results via the
# share_acknowledgement_commit_cb (not yet exposed in this binding).
# Durability tests verify end-to-end persistence by spinning up a
# second consumer in the same group and checking for absence of
# redelivery — the only signal currently observable from Python.


def test_implicit_commit_async_persists(kafka_cluster):
    """Implicit mode + commit_async + poll to drain in-flight: acks
    durably land. Fresh consumer in same group sees no redelivery."""
    topic = kafka_cluster.create_topic_and_wait_propogation('test-share-consumer-commit-async-implicit-durable')
    group_id = unique_id('test-share-consumer-commit-async-implicit-durable')
    num_messages = 3

    sc1 = kafka_cluster.share_consumer({'group.id': group_id, 'share.acknowledgement.mode': 'implicit'})
    try:
        sc1.subscribe([topic])

        producer = kafka_cluster.cimpl_producer()
        produced = [f'msg-{i}'.encode() for i in range(num_messages)]
        for value in produced:
            producer.produce(topic, value=value)
        producer.flush(timeout=10.0)

        msgs = drain_share_consumers([sc1], num_messages)[0]
        received_values = {msg.value() for msg in msgs}
        assert received_values == set(
            produced
        ), f'sc1 missed records: produced={set(produced)} received={received_values}'
        assert sc1.commit_async() is None
        for _ in range(5):
            sc1.poll(timeout=0.5)
    finally:
        sc1.close()

    sc2 = kafka_cluster.share_consumer({'group.id': group_id, 'share.acknowledgement.mode': 'implicit'})
    try:
        sc2.subscribe([topic])
        leftovers = []
        deadline = time.time() + 8.0
        while time.time() < deadline:
            for msg in sc2.poll(timeout=1.0):
                if msg.error() is None:
                    leftovers.append(msg.value())
        assert leftovers == [], f'async commit did not persist; got: {leftovers}'
    finally:
        sc2.close()


def test_explicit_commit_async_accept_persists(kafka_cluster):
    """Explicit ACCEPT + commit_async: acks persist; fresh consumer in
    same group sees no redelivery."""
    topic = kafka_cluster.create_topic_and_wait_propogation('test-share-consumer-commit-async-accept')
    group_id = unique_id('test-share-consumer-commit-async-accept')
    num_messages = 3

    sc1 = kafka_cluster.share_consumer({'group.id': group_id, 'share.acknowledgement.mode': 'explicit'})
    try:
        sc1.subscribe([topic])

        producer = kafka_cluster.cimpl_producer()
        produced = [f'msg-{i}'.encode() for i in range(num_messages)]
        for value in produced:
            producer.produce(topic, value=value)
        producer.flush(timeout=10.0)

        msgs, commit_results = poll_ack_commit_loop(
            sc1,
            num_messages,
            ack_type=AcknowledgeType.ACCEPT,
            async_commit=True,
        )
        received_values = {msg.value() for msg in msgs}
        assert received_values == set(
            produced
        ), f'sc1 missed records: produced={set(produced)} received={received_values}'
        assert commit_results and all(
            r is None for r in commit_results
        ), f'expected commit_async to return None per batch; got {commit_results}'
        # Flush any in-flight async acks before close.
        for _ in range(5):
            sc1.poll(timeout=0.5)
    finally:
        sc1.close()

    sc2 = kafka_cluster.share_consumer({'group.id': group_id, 'share.acknowledgement.mode': 'implicit'})
    try:
        sc2.subscribe([topic])
        leftovers = []
        deadline = time.time() + 8.0
        while time.time() < deadline:
            for msg in sc2.poll(timeout=1.0):
                if msg.error() is None:
                    leftovers.append(msg.value())
        assert leftovers == [], f'async ACCEPT did not persist; got: {leftovers}'
    finally:
        sc2.close()


def test_explicit_commit_async_release_redelivers(kafka_cluster):
    """RELEASE + commit_async: the released record comes back on a
    later poll with delivery_count == 2, same as the sync RELEASE
    path."""
    topic = kafka_cluster.create_topic_and_wait_propogation('test-share-consumer-commit-async-release')

    sc = kafka_cluster.share_consumer({'share.acknowledgement.mode': 'explicit'})
    try:
        sc.subscribe([topic])

        producer = kafka_cluster.cimpl_producer()
        producer.produce(topic, value=b'msg-0')
        producer.flush(timeout=10.0)

        first_batch = poll_first_batch(sc)
        assert first_batch
        first_msg = first_batch[0]
        assert (
            first_msg.delivery_count() == 1
        ), f'first delivery should have delivery_count=1, got {first_msg.delivery_count()}'

        sc.acknowledge(first_msg, AcknowledgeType.RELEASE)
        assert sc.commit_async() is None

        deadline = time.time() + 15.0
        redelivered = None
        while redelivered is None and time.time() < deadline:
            for msg in sc.poll(timeout=2.0):
                if msg.error() is None:
                    if msg.delivery_count() >= 2:
                        redelivered = msg
                    sc.acknowledge(msg, AcknowledgeType.ACCEPT)
                    if redelivered is not None:
                        break
        assert redelivered is not None, 'released record never came back via async commit'
        assert (
            redelivered.delivery_count() == 2
        ), f'redelivery should have delivery_count=2, got {redelivered.delivery_count()}'
    finally:
        sc.close()


def test_explicit_commit_async_reject_archives(kafka_cluster):
    """REJECT + commit_async: archived record does not come back to
    any consumer in the same group."""
    topic = kafka_cluster.create_topic_and_wait_propogation('test-share-consumer-commit-async-reject')
    group_id = unique_id('test-share-consumer-commit-async-reject')

    sc1 = kafka_cluster.share_consumer({'group.id': group_id, 'share.acknowledgement.mode': 'explicit'})
    try:
        sc1.subscribe([topic])

        producer = kafka_cluster.cimpl_producer()
        producer.produce(topic, value=b'msg-0')
        producer.flush(timeout=10.0)

        msgs = poll_first_batch(sc1)
        assert len(msgs) == 1
        for msg in msgs:
            sc1.acknowledge(msg, AcknowledgeType.REJECT)

        assert sc1.commit_async() is None
        for _ in range(5):
            sc1.poll(timeout=0.5)
    finally:
        sc1.close()

    sc2 = kafka_cluster.share_consumer({'group.id': group_id, 'share.acknowledgement.mode': 'explicit'})
    try:
        sc2.subscribe([topic])
        leftovers = drain_share_consumers([sc2], 1, timeout_s=5.0, ack_type=AcknowledgeType.ACCEPT)[0]
        assert leftovers == [], 'async REJECT should permanently archive'
    finally:
        sc2.close()


def test_commit_async_with_mixed_ack_types(kafka_cluster):
    """Mix ACCEPT/RELEASE/REJECT in one batch and commit_async. The
    RELEASE'd records should redeliver on a later poll with
    delivery_count == 2, same as the sync mixed-ack test."""
    topic = kafka_cluster.create_topic_and_wait_propogation('test-share-consumer-commit-async-mixed')
    num_messages = 6

    sc = kafka_cluster.share_consumer({'share.acknowledgement.mode': 'explicit'})
    try:
        sc.subscribe([topic])

        producer = kafka_cluster.cimpl_producer()
        for i in range(num_messages):
            producer.produce(topic, value=f'msg-{i}'.encode())
        producer.flush(timeout=10.0)

        ack_types = [
            AcknowledgeType.ACCEPT,
            AcknowledgeType.RELEASE,
            AcknowledgeType.REJECT,
        ]
        released_offsets = set()
        acked_count = 0
        deadline = time.time() + 10.0
        while acked_count < num_messages and time.time() < deadline:
            for msg in sc.poll(timeout=2.0):
                if msg.error() is None:
                    ack_type = ack_types[acked_count % 3]
                    if ack_type is AcknowledgeType.RELEASE:
                        released_offsets.add((msg.partition(), msg.offset()))
                    sc.acknowledge(msg, ack_type)
                    acked_count += 1
                    if acked_count >= num_messages:
                        break
        assert acked_count >= 3, f'only got {acked_count} records'
        assert released_offsets, 'no RELEASE acks issued, so we cannot check redelivery'

        assert sc.commit_async() is None

        # Every RELEASE'd record should come back as a redelivery with
        # delivery_count == 2; same contract as the sync path.
        redelivered_offsets = set()
        deadline = time.time() + 15.0
        while redelivered_offsets != released_offsets and time.time() < deadline:
            for msg in sc.poll(timeout=2.0):
                if msg.error() is None:
                    coords = (msg.partition(), msg.offset())
                    assert msg.delivery_count() >= 2, (
                        f'unexpected first-delivery record {coords} during redelivery window '
                        f'(delivery_count={msg.delivery_count()})'
                    )
                    assert (
                        coords in released_offsets
                    ), f'redelivered record {coords} was not in released set {released_offsets}'
                    redelivered_offsets.add(coords)
                    sc.acknowledge(msg, AcknowledgeType.ACCEPT)
        assert (
            redelivered_offsets == released_offsets
        ), f'released {released_offsets} but only saw redelivery of {redelivered_offsets}'
    finally:
        sc.close()


def test_explicit_commit_async_with_no_acks_is_noop(kafka_cluster):
    """Explicit mode + zero acknowledge() calls: commit_async is a
    clean no-op."""
    topic = kafka_cluster.create_topic_and_wait_propogation('test-share-consumer-commit-async-explicit-no-acks')

    sc = kafka_cluster.share_consumer({'share.acknowledgement.mode': 'explicit'})
    try:
        sc.subscribe([topic])
        for _ in range(5):
            sc.poll(timeout=0.2)
        assert sc.commit_async() is None
    finally:
        sc.close()


def test_commit_async_tight_loop_stays_fast(kafka_cluster):
    """50 commit_async calls back-to-back complete well under 2s — no
    accumulating latency, no implicit blocking."""
    topic = kafka_cluster.create_topic_and_wait_propogation('test-share-consumer-commit-async-tight-loop')

    sc = kafka_cluster.share_consumer({'share.acknowledgement.mode': 'implicit'})
    try:
        sc.subscribe([topic])

        n_calls = 50
        start = time.monotonic()
        for _ in range(n_calls):
            assert sc.commit_async() is None
        elapsed = time.monotonic() - start

        assert elapsed < 2.0, f'{n_calls} commit_async calls took {elapsed:.3f}s'
    finally:
        sc.close()


def test_commit_async_return_value_is_always_none(kafka_cluster):
    """commit_async never returns a dict — pure side-effect, type-stable
    across empty and non-empty states."""
    topic = kafka_cluster.create_topic_and_wait_propogation('test-share-consumer-commit-async-return-type')
    num_messages = 3

    sc = kafka_cluster.share_consumer({'share.acknowledgement.mode': 'implicit'})
    try:
        sc.subscribe([topic])
        assert sc.commit_async() is None  # nothing pending

        producer = kafka_cluster.cimpl_producer()
        for i in range(num_messages):
            producer.produce(topic, value=f'msg-{i}'.encode())
        producer.flush(timeout=10.0)
        drain_share_consumers([sc], num_messages)

        assert sc.commit_async() is None  # with pending acks
    finally:
        sc.close()


def test_commit_async_before_subscribe_is_noop(kafka_cluster):
    """commit_async on a brand-new consumer (no subscribe yet): no
    crash, returns None. KIP doesn't define this; pin down behavior."""
    sc = kafka_cluster.share_consumer({'share.acknowledgement.mode': 'implicit'})
    try:
        assert sc.commit_async() is None
    finally:
        sc.close()


def test_commit_async_inside_with_block(kafka_cluster):
    """ShareConsumer used as a context manager: commit_async before
    __exit__ persists, mirroring the explicit-close path."""
    topic = kafka_cluster.create_topic_and_wait_propogation('test-share-consumer-commit-async-with-block')
    group_id = unique_id('test-share-consumer-commit-async-with-block')
    num_messages = 2

    with kafka_cluster.share_consumer({'group.id': group_id, 'share.acknowledgement.mode': 'implicit'}) as sc:
        sc.subscribe([topic])

        producer = kafka_cluster.cimpl_producer()
        for i in range(num_messages):
            producer.produce(topic, value=f'msg-{i}'.encode())
        producer.flush(timeout=10.0)
        drain_share_consumers([sc], num_messages)
        assert sc.commit_async() is None
        for _ in range(5):
            sc.poll(timeout=0.5)

    with kafka_cluster.share_consumer({'group.id': group_id, 'share.acknowledgement.mode': 'implicit'}) as sc2:
        sc2.subscribe([topic])
        leftovers = []
        deadline = time.time() + 5.0
        while time.time() < deadline:
            for msg in sc2.poll(timeout=1.0):
                if msg.error() is None:
                    leftovers.append(msg.value())
        assert leftovers == [], f'commit_async inside with-block did not persist; got: {leftovers}'


def test_multiple_commit_async_then_sync_drains_all(kafka_cluster):
    """Three commit_async calls followed by commit_sync — sync resolves
    cleanly with no per-partition errors."""
    topic = kafka_cluster.create_topic_and_wait_propogation('test-share-consumer-commit-multi-async-then-sync')
    num_messages = 5

    sc = kafka_cluster.share_consumer({'share.acknowledgement.mode': 'implicit'})
    try:
        sc.subscribe([topic])

        producer = kafka_cluster.cimpl_producer()
        for i in range(num_messages):
            producer.produce(topic, value=f'msg-{i}'.encode())
        producer.flush(timeout=10.0)
        drain_share_consumers([sc], num_messages)

        assert sc.commit_async() is None
        assert sc.commit_async() is None
        assert sc.commit_async() is None
        result = sc.commit_sync(timeout=10.0)
        assert isinstance(result, dict)
        for tp, err in result.items():
            assert err is None
    finally:
        sc.close()


def test_commit_async_then_immediate_close_persists(kafka_cluster):
    """commit_async followed immediately by close (no intervening poll)
    — KIP doesn't define close-flush behavior; we pin down the current
    behavior. Most likely close drains the in-flight."""
    topic = kafka_cluster.create_topic_and_wait_propogation('test-share-consumer-commit-async-then-close')
    group_id = unique_id('test-share-consumer-commit-async-then-close')
    num_messages = 3

    sc1 = kafka_cluster.share_consumer({'group.id': group_id, 'share.acknowledgement.mode': 'explicit'})
    try:
        sc1.subscribe([topic])

        producer = kafka_cluster.cimpl_producer()
        produced = [f'msg-{i}'.encode() for i in range(num_messages)]
        for value in produced:
            producer.produce(topic, value=value)
        producer.flush(timeout=10.0)

        msgs, commit_results = poll_ack_commit_loop(
            sc1,
            num_messages,
            ack_type=AcknowledgeType.ACCEPT,
            async_commit=True,
        )
        received_values = {msg.value() for msg in msgs}
        assert received_values == set(
            produced
        ), f'sc1 missed records: produced={set(produced)} received={received_values}'
        assert commit_results and all(
            r is None for r in commit_results
        ), f'expected commit_async to return None per batch; got {commit_results}'
        # No intervening poll — close immediately after async submit.
    finally:
        sc1.close()

    sc2 = kafka_cluster.share_consumer({'group.id': group_id, 'share.acknowledgement.mode': 'implicit'})
    try:
        sc2.subscribe([topic])
        leftovers = []
        deadline = time.time() + 8.0
        while time.time() < deadline:
            for msg in sc2.poll(timeout=1.0):
                if msg.error() is None:
                    leftovers.append(msg.value())
        assert leftovers == [], f'close did not flush in-flight async commit; got: {leftovers}'
    finally:
        sc2.close()


def test_commit_async_spans_multiple_topics(kafka_cluster):
    """commit_async over records from two topics — both topics'
    acknowledgements persist across a consumer restart."""
    topic_a = kafka_cluster.create_topic_and_wait_propogation('test-share-consumer-commit-async-multi-topic-a')
    topic_b = kafka_cluster.create_topic_and_wait_propogation('test-share-consumer-commit-async-multi-topic-b')
    group_id = unique_id('test-share-consumer-commit-async-multi-topic')

    sc1 = kafka_cluster.share_consumer({'group.id': group_id, 'share.acknowledgement.mode': 'explicit'})
    try:
        sc1.subscribe([topic_a, topic_b])

        producer = kafka_cluster.cimpl_producer()
        producer.produce(topic_a, value=b'a-0')
        producer.produce(topic_b, value=b'b-0')
        producer.flush(timeout=10.0)

        msgs, commit_results = poll_ack_commit_loop(
            sc1,
            2,
            ack_type=AcknowledgeType.ACCEPT,
            async_commit=True,
        )
        topics_seen = {msg.topic() for msg in msgs}
        if len(topics_seen) < 2:
            pytest.skip(f'broker only delivered {topics_seen}; need both topics')
        assert commit_results and all(
            r is None for r in commit_results
        ), f'expected commit_async to return None per batch; got {commit_results}'
        # Flush any in-flight async acks before close.
        for _ in range(5):
            sc1.poll(timeout=0.5)
    finally:
        sc1.close()

    sc2 = kafka_cluster.share_consumer({'group.id': group_id, 'share.acknowledgement.mode': 'implicit'})
    try:
        sc2.subscribe([topic_a, topic_b])
        leftovers = []
        deadline = time.time() + 8.0
        while time.time() < deadline:
            for msg in sc2.poll(timeout=1.0):
                if msg.error() is None:
                    leftovers.append(msg.value())
        assert leftovers == [], f'async multi-topic commit did not persist; got: {leftovers}'
    finally:
        sc2.close()


def test_commit_async_rejects_arguments(kafka_cluster):
    """commit_async is METH_NOARGS — passing any argument raises
    TypeError before reaching the C layer."""
    topic = kafka_cluster.create_topic_and_wait_propogation('test-share-consumer-commit-async-bad-args')

    sc = kafka_cluster.share_consumer({'share.acknowledgement.mode': 'implicit'})
    try:
        sc.subscribe([topic])
        with pytest.raises(TypeError):
            sc.commit_async(timeout=1.0)
        with pytest.raises(TypeError):
            sc.commit_async(1.0)
    finally:
        sc.close()


# --- multi-consumer scenarios ---------------------------------------------


def test_commit_handles_high_volume_across_two_consumers(kafka_cluster):
    """10k records to a partition topic, two consumers in one share group,
    every record delivered exactly once across the group
    """
    group_id = unique_id('test-share-consumer-commit-high-volume')

    res = ConfigResource(
        ResourceType.GROUP,
        group_id,
        incremental_configs=[
            ConfigEntry(
                'share.record.lock.duration.ms',
                '30000',
                incremental_operation=AlterConfigOpType.SET,
            ),
        ],
    )
    for f in kafka_cluster.admin().incremental_alter_configs([res]).values():
        f.result()

    topic = kafka_cluster.create_topic_and_wait_propogation(
        'test-share-consumer-commit-high-volume',
        conf={'num_partitions': 6},
    )
    num_messages = 10_000

    producer = kafka_cluster.cimpl_producer()
    for i in range(num_messages):
        producer.produce(topic, value=f'msg-{i}'.encode())
    producer.flush(timeout=30.0)

    sc1 = kafka_cluster.share_consumer({'group.id': group_id})
    sc2 = kafka_cluster.share_consumer({'group.id': group_id})
    try:
        sc1.subscribe([topic])
        sc2.subscribe([topic])

        per_consumer_msgs, per_consumer_commits = poll_ack_commit_loop(
            [sc1, sc2],
            num_messages,
            poll_timeout_s=3.0,
            deadline_s=60.0,
        )

        # Each batch committed by either consumer should have a clean
        # per-partition result. An empty result here would mean the
        # acks piggybacked on the next poll, which is fine for drain
        # but defeats the point of this test.
        for sc_label, commits in (('sc1', per_consumer_commits[0]), ('sc2', per_consumer_commits[1])):
            for result in commits:
                assert result, f'{sc_label}: commit_sync returned empty result'
                for tp, err in result.items():
                    assert err is None, f'{sc_label}: {tp.topic}[{tp.partition}] -> {err}'

        sc1_keys = {(msg.partition(), msg.offset()) for msg in per_consumer_msgs[0]}
        sc2_keys = {(msg.partition(), msg.offset()) for msg in per_consumer_msgs[1]}

        assert len(sc1_keys) == len(per_consumer_msgs[0]), 'sc1 saw duplicate (partition, offset)'
        assert len(sc2_keys) == len(per_consumer_msgs[1]), 'sc2 saw duplicate (partition, offset)'
        assert sc1_keys.isdisjoint(sc2_keys), f'overlap of {len(sc1_keys & sc2_keys)} records across consumers'
        assert len(sc1_keys | sc2_keys) == num_messages, (
            f'expected {num_messages} unique records, got {len(sc1_keys | sc2_keys)} '
            f'(sc1={len(sc1_keys)}, sc2={len(sc2_keys)})'
        )
    finally:
        sc1.close()
        sc2.close()


def test_redelivery_increments_delivery_count_and_commits(kafka_cluster):
    """A record re-acquired after lock expiry has its delivery_count
    incremented by exactly 1, and commit_sync handles the redelivered
    record cleanly.
    """
    group_id = unique_id('test-share-consumer-commit-delivery-count')

    res = ConfigResource(
        ResourceType.GROUP,
        group_id,
        incremental_configs=[
            ConfigEntry(
                'share.record.lock.duration.ms',
                '5000',
                incremental_operation=AlterConfigOpType.SET,
            ),
        ],
    )
    for f in kafka_cluster.admin().incremental_alter_configs([res]).values():
        f.result()

    topic = kafka_cluster.create_topic_and_wait_propogation('test-share-consumer-commit-delivery-count')

    sc_a = kafka_cluster.share_consumer({'group.id': group_id, 'share.acknowledgement.mode': 'explicit'})
    sc_b = kafka_cluster.share_consumer({'group.id': group_id, 'share.acknowledgement.mode': 'explicit'})
    try:
        # Subscribe only A first so the single record lands unambiguously
        # on sc_a — no race with sc_b for the first delivery.
        sc_a.subscribe([topic])

        producer = kafka_cluster.cimpl_producer()
        producer.produce(topic, value=b'redelivery-probe')
        producer.flush(timeout=10.0)

        a_batch = poll_first_batch(sc_a)
        assert a_batch, 'sc_a never received the produced record'
        a_first = a_batch[0]

        # First delivery: count==1. Do NOT acknowledge — leave A's 5s
        # acquisition lock to expire.
        assert a_first.delivery_count() == 1, (
            f'first delivery should have delivery_count=1, ' f'got {a_first.delivery_count()}'
        )
        target = (a_first.partition(), a_first.offset())

        # Before lock expiry, sc_b must NOT see the record — confirms the
        # broker is honouring A's still-valid acquisition lock.
        sc_b.subscribe([topic])
        for msg in sc_b.poll(timeout=0.5):
            if msg.error() is None:
                assert (msg.partition(), msg.offset()) != target, 'sc_b saw the record before sc_a lost its lock'

        # Wait past this group's 5s lock duration so sc_b can steal.
        time.sleep(5.5)

        b_batch = poll_first_batch(sc_b)
        b_seen = next(
            (msg for msg in b_batch if (msg.partition(), msg.offset()) == target),
            None,
        )
        assert b_seen is not None, f'sc_b never received redelivery of {target} after lock expiry'
        # Redelivery is exactly the second delivery — incremented by 1.
        assert b_seen.delivery_count() == 2, (
            f'redelivery should have delivery_count=2, ' f'got {b_seen.delivery_count()}'
        )

        sc_b.acknowledge(b_seen, AcknowledgeType.ACCEPT)
        result = sc_b.commit_sync(timeout=10.0)
        assert isinstance(result, dict)
        for tp, err in result.items():
            assert err is None, f'{tp.topic}[{tp.partition}] -> {err}'
    finally:
        sc_a.close()
        sc_b.close()
