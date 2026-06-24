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

"""Integration tests for ShareConsumer close()/destroy.

Covers what close() does with acquired records (release vs. commit) in each
ack mode, the garbage-collection teardown path end to end, reentrancy from the
ack callback, and a couple of lifecycle edges — all against a real broker.
"""

import gc
import time
import weakref

from confluent_kafka import AcknowledgeType, KafkaError, KafkaException
from tests.common import (
    drain_share_consumers,
    poll_first_batch,
    unique_id,
)


def test_implicit_close_releases_unacked_redelivers(kafka_cluster):
    """Implicit mode: close() releases the last poll's records instead of
    committing them, so a second consumer in the group picks them up.

    The contrast is test_implicit_mode_autocommits_on_next_poll, which polls
    once more to ack the tail before closing. Here we skip that, so close() is
    the only thing that touches the held record.
    """
    topic = kafka_cluster.create_topic_and_wait_propogation('test-share-consumer-close-implicit-release')
    group_id = unique_id('test-share-consumer-close-implicit-release')

    sc1 = kafka_cluster.share_consumer({'group.id': group_id, 'share.acknowledgement.mode': 'implicit'})
    try:
        sc1.subscribe([topic])

        producer = kafka_cluster.cimpl_producer()
        producer.produce(topic, value=b'msg-0')
        producer.flush(timeout=10.0)

        # Take the record but don't poll again (no implicit ack) or commit, so
        # it's still acquired-but-unacked when we close.
        batch = poll_first_batch(sc1)
        assert batch, 'sc1 received nothing'
        held = {(m.topic(), m.partition(), m.offset()) for m in batch}
    finally:
        sc1.close()  # releases the held record, doesn't ack it

    sc2 = kafka_cluster.share_consumer({'group.id': group_id, 'share.acknowledgement.mode': 'implicit'})
    try:
        sc2.subscribe([topic])
        redelivered = poll_first_batch(sc2, timeout_s=10.0)
        seen = {(m.topic(), m.partition(), m.offset()) for m in redelivered}
        assert held <= seen, f'released record should be redelivered; held {held}, sc2 saw {seen}'
    finally:
        sc2.close()


def test_explicit_close_partial_acks_releases_remainder(kafka_cluster):
    """Explicit mode: close() commits what was acked and releases the rest.
    Ack half a batch, close, and the other half comes back to a second
    consumer.
    """
    topic = kafka_cluster.create_topic_and_wait_propogation('test-share-consumer-close-partial-ack')
    group_id = unique_id('test-share-consumer-close-partial-ack')
    num_messages = 4

    sc1 = kafka_cluster.share_consumer({'group.id': group_id, 'share.acknowledgement.mode': 'explicit'})
    try:
        sc1.subscribe([topic])

        producer = kafka_cluster.cimpl_producer()
        for i in range(num_messages):
            producer.produce(topic, value=f'msg-{i}'.encode())
        producer.flush(timeout=10.0)

        # One flush of small records arrives as a single batch, so we can split
        # it (same assumption as test_mixed_ack_types_in_single_batch).
        batch = poll_first_batch(sc1)
        assert len(batch) >= 2, f'need a multi-record batch to split; got {len(batch)}'

        half = len(batch) // 2
        for m in batch[:half]:
            sc1.acknowledge(m, AcknowledgeType.ACCEPT)
        accepted = {(m.topic(), m.partition(), m.offset()) for m in batch[:half]}
        released = {(m.topic(), m.partition(), m.offset()) for m in batch[half:]}
    finally:
        sc1.close()  # commit the acked half, release the rest

    sc2 = kafka_cluster.share_consumer({'group.id': group_id, 'share.acknowledgement.mode': 'explicit'})
    try:
        sc2.subscribe([topic])
        seen = set()
        deadline = time.time() + 10.0
        while time.time() < deadline and not released <= seen:
            for m in sc2.poll(timeout=0.5):
                if m.error() is None:
                    seen.add((m.topic(), m.partition(), m.offset()))
                    sc2.acknowledge(m, AcknowledgeType.ACCEPT)
        assert released <= seen, f'un-acked records should redeliver: {released} not in {seen}'
        assert accepted.isdisjoint(seen), f'acked records should not redeliver: {accepted & seen}'
    finally:
        sc2.close()


def test_gc_without_close_leaves_group_and_redelivers(kafka_cluster):
    """Same as the implicit-release case, but via GC instead of close():
    dropping the consumer leaves the group and releases the held record. The
    unit test only checks this doesn't crash; here we check it re-delivers.
    """
    topic = kafka_cluster.create_topic_and_wait_propogation('test-share-consumer-close-gc-redeliver')
    group_id = unique_id('test-share-consumer-close-gc-redeliver')

    sc1 = kafka_cluster.share_consumer({'group.id': group_id, 'share.acknowledgement.mode': 'implicit'})
    sc1.subscribe([topic])

    producer = kafka_cluster.cimpl_producer()
    producer.produce(topic, value=b'msg-0')
    producer.flush(timeout=10.0)

    batch = poll_first_batch(sc1)
    assert batch, 'sc1 received nothing'
    held = {(m.topic(), m.partition(), m.offset()) for m in batch}

    # No close(): drop the only reference and let GC tear it down.
    ref = weakref.ref(sc1)
    del sc1
    gc.collect()
    assert ref() is None, 'sc1 was not collected'

    sc2 = kafka_cluster.share_consumer({'group.id': group_id, 'share.acknowledgement.mode': 'implicit'})
    try:
        sc2.subscribe([topic])
        redelivered = poll_first_batch(sc2, timeout_s=10.0)
        seen = {(m.topic(), m.partition(), m.offset()) for m in redelivered}
        assert held <= seen, f'GC teardown should redeliver the record; held {held}, sc2 saw {seen}'
    finally:
        sc2.close()


def test_callback_reentrancy_guard_covers_all_methods(kafka_cluster):
    """Any consumer method called from inside the ack-commit callback fails
    with _STATE and leaves the consumer usable. Extends
    test_callback_reentrancy_guard (which only checks commit_async +
    set_callback) to poll, the subscribe family, commit_sync, and acknowledge.

    close() is left out on purpose: calling it from the callback currently
    bricks the consumer even though the call is rejected (a known bug, tracked
    separately), which would wreck the rest of the test.
    """
    topic = kafka_cluster.create_topic_and_wait_propogation('test-share-consumer-close-cb-reentrancy-all')

    sc = kafka_cluster.share_consumer({'share.acknowledgement.mode': 'explicit'})
    captured = {}
    held_msg = []
    try:

        def reentrant_cb(offsets, exc):
            msg = held_msg[0]
            probes = {
                'poll': lambda: sc.poll(timeout=0.1),
                'subscribe': lambda: sc.subscribe([topic]),
                'unsubscribe': sc.unsubscribe,
                'subscription': sc.subscription,
                'commit_sync': lambda: sc.commit_sync(timeout=1.0),
                'acknowledge': lambda: sc.acknowledge(msg, AcknowledgeType.ACCEPT),
                'acknowledge_offset': lambda: sc.acknowledge_offset(topic, 0, 0, AcknowledgeType.ACCEPT),
            }
            for name, fn in probes.items():
                try:
                    fn()
                    captured[name] = 'no-exception'
                except KafkaException as ex:
                    captured[name] = ex.args[0].code()
                except Exception as ex:  # noqa: BLE001 - record whatever it raised
                    captured[name] = repr(ex)

        sc.set_acknowledgement_commit_callback(reentrant_cb)
        sc.subscribe([topic])

        producer = kafka_cluster.cimpl_producer()
        producer.produce(topic, value=b'msg-0')
        producer.flush(timeout=10.0)

        batch = poll_first_batch(sc)
        assert batch
        held_msg.append(batch[0])
        for m in batch:
            sc.acknowledge(m, AcknowledgeType.ACCEPT)

        # Commit now; the callback runs on the next poll.
        sc.commit_async()
        deadline = time.time() + 10.0
        while time.time() < deadline and not captured:
            sc.poll(timeout=0.5)

        assert captured, 'callback never ran'
        for name, code in captured.items():
            assert code == KafkaError._STATE, f'{name}() from callback returned {code!r}, expected _STATE'

        # Rejected calls shouldn't have changed anything.
        assert sc.subscription() == [topic]
    finally:
        # Clear the cb before close so its drain doesn't trip the guard again.
        sc.set_acknowledgement_commit_callback(None)
        sc.close()


def test_close_after_unsubscribe_is_clean(kafka_cluster):
    """close() after unsubscribe() is a clean, quick teardown — nothing pending."""
    topic = kafka_cluster.create_topic_and_wait_propogation('test-share-consumer-close-after-unsub')

    sc = kafka_cluster.share_consumer()
    sc.subscribe([topic])

    producer = kafka_cluster.cimpl_producer()
    producer.produce(topic, value=b'msg-0')
    producer.flush(timeout=10.0)
    drain_share_consumers([sc], 1, timeout_s=10.0)

    sc.unsubscribe()
    assert sc.subscription() == []

    start = time.time()
    sc.close()
    assert time.time() - start < 10.0, 'close() after unsubscribe() should be prompt'


def test_close_immediately_after_subscribe_is_clean(kafka_cluster):
    """subscribe() then an immediate close(), before any poll or join — should
    be quick and clean, not a hang.
    """
    topic = kafka_cluster.create_topic_and_wait_propogation('test-share-consumer-close-after-sub')

    sc = kafka_cluster.share_consumer()
    sc.subscribe([topic])

    start = time.time()
    sc.close()
    assert time.time() - start < 10.0, 'close() right after subscribe() should be prompt'
