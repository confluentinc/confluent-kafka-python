#!/usr/bin/env python
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

"""
Tests for the sync Consumer's re-entrancy, on a single thread.

A rebalance/commit callback fires synchronously, on the same thread that's
blocked inside the top-level call that triggered it (poll(), consume(),
close(), etc.). A re-entrant call made from within such a callback presents that
same thread's own ID as its gate identity, which already matches
gate_owner -- recognized as a legitimate re-entrant call (gate_depth
incremented) rather than illegal concurrent access.
"""

import time
from uuid import uuid1

from confluent_kafka import TopicPartition


def _new_consumer(kafka_cluster, conf=None):
    consumer_conf = {
        'group.id': str(uuid1()),
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False,
    }
    if conf:
        consumer_conf.update(conf)
    return kafka_cluster.cimpl_consumer(consumer_conf)


def test_on_assign_calls_assign_from_callback(kafka_cluster):
    """on_assign callback re-entrantly calls consumer.assign() (eager
    protocol), triggered by poll()."""
    topic = kafka_cluster.create_topic_and_wait_propogation("test_on_assign_calls_assign")
    kafka_cluster.seed_topic(topic, value_source=[b'hello'])

    consumer = _new_consumer(kafka_cluster)

    assign_called = []

    def on_assign(consumer, partitions):
        assign_called.append(partitions)
        consumer.assign(partitions)

    consumer.subscribe([topic], on_assign=on_assign)

    msg = consumer.poll(10)

    print(f"assign_called={len(assign_called)}, msg={msg.value() if msg else None}")
    assert assign_called, "on_assign was never invoked"
    assert msg is not None
    assert msg.value() == b'hello'

    consumer.close()


def test_on_assign_calls_incremental_assign_from_callback(kafka_cluster):
    """Same re-entrant shape as above, but under the cooperative-sticky
    protocol via incremental_assign()."""
    topic = kafka_cluster.create_topic_and_wait_propogation("test_on_assign_calls_incremental_assign")
    kafka_cluster.seed_topic(topic, value_source=[b'hello'])

    consumer = _new_consumer(kafka_cluster, {'partition.assignment.strategy': 'cooperative-sticky'})

    assign_called = []

    def on_assign(consumer, partitions):
        assign_called.append(partitions)
        consumer.incremental_assign(partitions)

    consumer.subscribe([topic], on_assign=on_assign)

    msg = consumer.poll(10)

    print(f"assign_called={len(assign_called)}, msg={msg.value() if msg else None}")
    assert assign_called, "on_assign was never invoked"
    assert msg is not None
    assert msg.value() == b'hello'

    consumer.close()


def test_on_assign_calls_assign_from_consume_callback(kafka_cluster):
    """Same re-entrant shape as test_on_assign_calls_assign_from_callback,
    but the top-level call that triggers on_assign is consume() rather
    than poll()."""
    topic = kafka_cluster.create_topic_and_wait_propogation("test_on_assign_calls_assign_from_consume")
    kafka_cluster.seed_topic(topic, value_source=[b'hello'])

    consumer = _new_consumer(kafka_cluster)

    assign_called = []

    def on_assign(consumer, partitions):
        assign_called.append(partitions)
        consumer.assign(partitions)

    consumer.subscribe([topic], on_assign=on_assign)

    msgs = consumer.consume(num_messages=1, timeout=10)

    print(f"assign_called={len(assign_called)}, msgs={[m.value() for m in msgs]}")
    assert assign_called, "on_assign was never invoked"
    assert msgs
    assert msgs[0].value() == b'hello'

    consumer.close()


def test_on_revoke_calls_unassign_from_callback(kafka_cluster):
    """on_revoke callback re-entrantly calls consumer.unassign() (eager
    protocol). unsubscribe() does not itself synchronously trigger
    on_revoke -- librdkafka defers the revoke and delivers it via a later
    poll() call, same as any other rebalance/commit callback."""
    topic = kafka_cluster.create_topic_and_wait_propogation("test_on_revoke_calls_unassign")
    kafka_cluster.seed_topic(topic, value_source=[b'hello'])

    consumer = _new_consumer(kafka_cluster)

    revoke_called = []

    def on_revoke(consumer, partitions):
        revoke_called.append(partitions)
        # tests.common.TestConsumer.unassign() requires partitions
        # (unlike the plain C-level Consumer.unassign()), so it can
        # transparently use incremental_unassign() under the new
        # "consumer" group protocol -- see tests/common/__init__.py.
        consumer.unassign(partitions)

    consumer.subscribe([topic], on_revoke=on_revoke)

    msg = consumer.poll(10)
    assert msg is not None
    assert msg.value() == b'hello'

    consumer.unsubscribe()
    consumer.poll(0)

    print(f"revoke_called={len(revoke_called)}")
    assert revoke_called, "on_revoke was never invoked"

    consumer.close()


def test_on_revoke_calls_incremental_unassign_from_callback(kafka_cluster):
    """Same re-entrant shape as above, but under the cooperative-sticky
    protocol via incremental_unassign()."""
    topic = kafka_cluster.create_topic_and_wait_propogation("test_on_revoke_calls_incremental_unassign")
    kafka_cluster.seed_topic(topic, value_source=[b'hello'])

    consumer = _new_consumer(kafka_cluster, {'partition.assignment.strategy': 'cooperative-sticky'})

    revoke_called = []

    def on_assign(consumer, partitions):
        consumer.incremental_assign(partitions)

    def on_revoke(consumer, partitions):
        revoke_called.append(partitions)
        consumer.incremental_unassign(partitions)

    consumer.subscribe([topic], on_assign=on_assign, on_revoke=on_revoke)

    msg = consumer.poll(10)
    assert msg is not None
    assert msg.value() == b'hello'

    consumer.unsubscribe()
    consumer.poll(0)

    print(f"revoke_called={len(revoke_called)}")
    assert revoke_called, "on_revoke was never invoked"

    consumer.close()


def test_on_lost_calls_unassign_from_callback(kafka_cluster):
    """on_lost fires when the assignment is lost involuntarily
    -- rd_kafka_assignment_lost(rk) is true, e.g. after a
    max.poll.interval.ms violation gets the member evicted from the group
    -- a different branch in Consumer_rebalance_cb than the normal revoke
    path. Forced here with a very short max.poll.interval.ms/
    session.timeout.ms plus a deliberate stall past it. Re-entrantly calls
    consumer.unassign() from inside on_lost."""
    topic = kafka_cluster.create_topic_and_wait_propogation("test_on_lost_calls_unassign")
    kafka_cluster.seed_topic(topic, value_source=[b'hello'])

    consumer = _new_consumer(
        kafka_cluster,
        {
            'session.timeout.ms': 6000,
            'max.poll.interval.ms': 6000,
            'heartbeat.interval.ms': 1000,
        },
    )

    lost_called = []
    revoke_called = []

    def on_revoke(consumer, partitions):
        revoke_called.append(partitions)

    def on_lost(consumer, partitions):
        lost_called.append(partitions)
        consumer.unassign(partitions)

    consumer.subscribe([topic], on_revoke=on_revoke, on_lost=on_lost)

    msg = consumer.poll(10)
    assert msg is not None
    assert msg.value() == b'hello'

    # Stall well past max.poll.interval.ms/session.timeout.ms without
    # polling, so the broker evicts this member from the group -- the next
    # poll() will detect the lost assignment and fire on_lost rather than
    # on_revoke.
    time.sleep(9)

    for _ in range(50):
        consumer.poll(0.1)
        if lost_called:
            break

    print(f"lost_called={len(lost_called)}, revoke_called={len(revoke_called)}")
    assert lost_called, "on_lost was never invoked"
    assert not revoke_called, "on_revoke fired instead of on_lost -- assignment wasn't actually lost"

    consumer.close()


def test_on_commit_calls_commit_from_callback(kafka_cluster):
    """on_commit callback re-entrantly calls consumer.commit()
    (asynchronous=False) -- e.g. a retry-on-failure pattern."""
    topic = kafka_cluster.create_topic_and_wait_propogation("test_on_commit_calls_commit")
    kafka_cluster.seed_topic(topic, value_source=[b'hello'])

    commit_called = []
    retried_commit_results = []
    consumer = None
    msg = None

    def on_commit(err, partitions):
        commit_called.append((err, partitions))
        if len(commit_called) == 1:
            result = consumer.commit(message=msg, asynchronous=False)
            retried_commit_results.append(result)

    consumer = _new_consumer(kafka_cluster, {'on_commit': on_commit})

    def on_assign(consumer, partitions):
        consumer.assign(partitions)

    consumer.subscribe([topic], on_assign=on_assign)

    msg = consumer.poll(10)
    assert msg is not None

    # on_commit only fires for asynchronous commits, and is served by a
    # later poll() call.
    consumer.commit(message=msg, asynchronous=True)
    for _ in range(50):
        consumer.poll(0.1)
        if commit_called:
            break

    print(f"commit_called={len(commit_called)}, retried_commit_results={retried_commit_results}")
    assert commit_called, "on_commit was never invoked"
    assert retried_commit_results, "the re-entrant commit() call from inside on_commit never completed"

    consumer.close()


def test_on_commit_calls_asynchronous_commit_from_callback(kafka_cluster):
    """Same re-entrant shape as above, but the re-entrant call is itself
    commit(asynchronous=True) -- a different branch in
    Consumer__commit_internal (the async path returns None immediately
    instead of blocking on rd_kafka_commit_queue() and building a topic/
    partition/offset/err list). The re-entrant async commit() schedules
    yet another on_commit firing, served by a later poll()."""
    topic = kafka_cluster.create_topic_and_wait_propogation("test_on_commit_calls_async_commit")
    kafka_cluster.seed_topic(topic, value_source=[b'hello'])

    commit_called = []
    reentrant_results = []
    consumer = None
    msg = None

    def on_commit(err, partitions):
        commit_called.append((err, partitions))
        if len(commit_called) == 1:
            result = consumer.commit(message=msg, asynchronous=True)
            reentrant_results.append(result)

    consumer = _new_consumer(kafka_cluster, {'on_commit': on_commit})

    def on_assign(consumer, partitions):
        consumer.assign(partitions)

    consumer.subscribe([topic], on_assign=on_assign)

    msg = consumer.poll(10)
    assert msg is not None

    consumer.commit(message=msg, asynchronous=True)
    for _ in range(50):
        consumer.poll(0.1)
        if len(commit_called) == 2:
            break

    print(f"commit_called={len(commit_called)}, reentrant_results={reentrant_results}")
    assert reentrant_results, "the re-entrant asynchronous commit() call from inside on_commit never completed"
    assert reentrant_results[0] is None, "asynchronous commit() should return None"
    assert len(commit_called) == 2, (
        f"expected on_commit to fire exactly twice (initial + re-entrant asynchronous commit()), "
        f"got {len(commit_called)} calls: {commit_called}"
    )

    consumer.close()


def test_store_offsets_calls_store_offsets_from_callback(kafka_cluster):
    """store_offsets() called re-entrantly from within on_revoke."""
    topic = kafka_cluster.create_topic_and_wait_propogation("test_store_offsets_from_callback")
    kafka_cluster.seed_topic(topic, value_source=[b'hello'])

    # store_offsets() requires manual offset storage.
    consumer = _new_consumer(kafka_cluster, {'enable.auto.offset.store': False})

    store_offsets_results = []
    msg = None

    def on_assign(consumer, partitions):
        consumer.assign(partitions)

    def on_revoke(consumer, partitions):
        # By the time on_revoke can fire, the first poll() has already
        # returned a real message to store an offset for.
        if msg is not None:
            result = consumer.store_offsets(message=msg)
            store_offsets_results.append(result)
        consumer.unassign(partitions)

    consumer.subscribe([topic], on_assign=on_assign, on_revoke=on_revoke)

    msg = consumer.poll(10)
    assert msg is not None

    # unsubscribe() doesn't synchronously trigger on_revoke -- it's the
    # poll(0) right after that actually delivers it.
    consumer.unsubscribe()
    consumer.poll(0)

    print(f"store_offsets_results={store_offsets_results}")
    assert store_offsets_results, "the re-entrant store_offsets() call from inside on_revoke never completed"
    assert store_offsets_results[0] is None, "store_offsets() should return None"

    consumer.close()


def test_on_assign_calls_non_reentrancy_eligible_method(kafka_cluster):
    """on_assign re-entrantly calls consumer.pause()/consumer.position()/
    consumer.get_watermark_offsets()/consumer.offsets_for_times(), none of
    which is one of the 5 originally reentrancy-eligible methods
    (assign/unassign/incremental_assign/incremental_unassign/commit).
    Confirms the gate treats every gated method uniformly."""
    topic = kafka_cluster.create_topic_and_wait_propogation("test_on_assign_calls_non_reentrancy_eligible")
    kafka_cluster.seed_topic(topic, value_source=[b'hello'])

    consumer = _new_consumer(kafka_cluster)

    calls_made = []

    def on_assign(consumer, partitions):
        consumer.assign(partitions)
        calls_made.append('assign')
        consumer.pause(partitions)
        calls_made.append('pause')
        calls_made.append(('position', consumer.position(partitions)))
        consumer.resume(partitions)
        calls_made.append('resume')
        calls_made.append(('watermark', consumer.get_watermark_offsets(partitions[0])))
        calls_made.append(
            (
                'offsets_for_times',
                consumer.offsets_for_times([TopicPartition(p.topic, p.partition, 0) for p in partitions]),
            )
        )

    consumer.subscribe([topic], on_assign=on_assign)

    msg = consumer.poll(10)

    print(f"calls_made={calls_made}, msg={msg.value() if msg else None}")
    assert [c if isinstance(c, str) else c[0] for c in calls_made] == [
        'assign',
        'pause',
        'position',
        'resume',
        'watermark',
        'offsets_for_times',
    ], f"expected all 6 re-entrant calls to run in order, got: {calls_made}"
    assert msg is not None
    assert msg.value() == b'hello'

    consumer.close()
