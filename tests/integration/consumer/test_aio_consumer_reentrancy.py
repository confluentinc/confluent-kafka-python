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
Tests for AIOConsumer re-entrant calls.

A rebalance/commit callback runs on whichever ThreadPoolExecutor worker
thread triggered it, bridged onto the event loop via
run_coroutine_threadsafe(); if the callback awaits a public AIOConsumer
method (e.g. consumer.assign(...)), that call may be re-dispatched to a
*different* worker thread.
"""

import asyncio
import inspect
from uuid import uuid1

from confluent_kafka import TopicPartition
from tests.common import TestAIOConsumer


def called_by():
    return inspect.stack()[1].function


async def _new_aio_consumer(kafka_cluster, conf=None):
    consumer_conf = kafka_cluster.client_conf(
        {
            'group.id': str(uuid1()),
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False,
        }
    )
    if conf:
        consumer_conf.update(conf)
    return TestAIOConsumer(consumer_conf, max_workers=2)


async def test_on_assign_calls_assign_from_callback(kafka_cluster):
    """on_assign callback re-entrantly awaits consumer.assign(),
    the re-entrant call is dispatched back through the executor
    and may land on a different worker thread than the one blocked in the
    callback bridge. Must complete without error and the consumer must be
    usable afterward."""
    topic = kafka_cluster.create_topic_and_wait_propogation("test_aio_on_assign_calls_assign")
    kafka_cluster.seed_topic(topic, value_source=[b'hello'])

    consumer = await _new_aio_consumer(kafka_cluster)

    assign_called = []

    async def on_assign(consumer, partitions):
        assign_called.append(partitions)
        await consumer.assign(partitions)

    await consumer.subscribe([topic], on_assign=on_assign)

    msg = await consumer.poll(10)

    print(f"{called_by()}: assign_called={len(assign_called)}, msg={msg.value() if msg else None}")
    assert assign_called, "on_assign was never invoked"
    assert msg is not None
    assert msg.value() == b'hello'

    await consumer.close()


async def test_on_assign_calls_assign_from_consume_callback(kafka_cluster):
    """Same re-entrant shape as test_on_assign_calls_assign_from_callback,
    but the top-level call that triggers on_assign is consume() rather
    than poll()"""
    topic = kafka_cluster.create_topic_and_wait_propogation("test_aio_on_assign_calls_assign_from_consume")
    kafka_cluster.seed_topic(topic, value_source=[b'hello'])

    consumer = await _new_aio_consumer(kafka_cluster)

    assign_called = []

    async def on_assign(consumer, partitions):
        assign_called.append(partitions)
        await consumer.assign(partitions)

    await consumer.subscribe([topic], on_assign=on_assign)

    msgs = await consumer.consume(num_messages=1, timeout=10)

    print(f"{called_by()}: assign_called={len(assign_called)}, msgs={[m.value() for m in msgs]}")
    assert assign_called, "on_assign was never invoked"
    assert msgs
    assert msgs[0].value() == b'hello'

    await consumer.close()


async def test_on_assign_calls_incremental_assign_from_callback(kafka_cluster):
    """Same re-entrant shape as above, but under the cooperative-sticky
    protocol via incremental_assign()."""
    topic = kafka_cluster.create_topic_and_wait_propogation("test_aio_on_assign_calls_incremental_assign")
    kafka_cluster.seed_topic(topic, value_source=[b'hello'])

    consumer = await _new_aio_consumer(kafka_cluster, {'partition.assignment.strategy': 'cooperative-sticky'})

    assign_called = []

    async def on_assign(consumer, partitions):
        assign_called.append(partitions)
        await consumer.incremental_assign(partitions)

    await consumer.subscribe([topic], on_assign=on_assign)

    msg = await consumer.poll(10)

    print(f"{called_by()}: assign_called={len(assign_called)}, msg={msg.value() if msg else None}")
    assert assign_called, "on_assign was never invoked"
    assert msg is not None
    assert msg.value() == b'hello'

    await consumer.close()


async def test_on_revoke_calls_unassign_from_callback(kafka_cluster):
    """on_revoke callback re-entrantly awaits consumer.unassign() (eager
    protocol)."""
    topic = kafka_cluster.create_topic_and_wait_propogation("test_aio_on_revoke_calls_unassign")
    kafka_cluster.seed_topic(topic, value_source=[b'hello'])

    consumer = await _new_aio_consumer(kafka_cluster)

    revoke_called = []

    async def on_revoke(consumer, partitions):
        revoke_called.append(partitions)
        await consumer.unassign(partitions)

    await consumer.subscribe([topic], on_revoke=on_revoke)

    msg = await consumer.poll(10)
    assert msg is not None
    assert msg.value() == b'hello'

    await consumer.unsubscribe()
    await consumer.poll(0)

    print(f"{called_by()}: revoke_called={len(revoke_called)}")
    assert revoke_called, "on_revoke was never invoked"

    await consumer.close()


async def test_on_revoke_calls_unassign_from_close_callback(kafka_cluster):
    """on_revoke callback re-entrantly awaits consumer.unassign(), fired
    synchronously from within close() itself"""
    topic = kafka_cluster.create_topic_and_wait_propogation("test_aio_on_revoke_calls_unassign_from_close")
    kafka_cluster.seed_topic(topic, value_source=[b'hello'])

    consumer = await _new_aio_consumer(kafka_cluster)

    revoke_called = []

    async def on_revoke(consumer, partitions):
        revoke_called.append(partitions)
        await consumer.unassign(partitions)

    await consumer.subscribe([topic], on_revoke=on_revoke)

    msg = await consumer.poll(10)
    assert msg is not None
    assert msg.value() == b'hello'

    # close() itself is the trigger here, not a subsequent poll().
    await consumer.close()

    print(f"{called_by()}: revoke_called={len(revoke_called)}")
    assert revoke_called, "on_revoke was never invoked from close()"


async def test_on_revoke_calls_incremental_unassign_from_close_callback(kafka_cluster):
    """Same shape as above, but under the cooperative-sticky protocol
    via incremental_unassign()."""
    topic = kafka_cluster.create_topic_and_wait_propogation("test_aio_on_revoke_calls_incremental_unassign_from_close")
    kafka_cluster.seed_topic(topic, value_source=[b'hello'])

    consumer = await _new_aio_consumer(kafka_cluster, {'partition.assignment.strategy': 'cooperative-sticky'})

    revoke_called = []

    async def on_assign(consumer, partitions):
        await consumer.incremental_assign(partitions)

    async def on_revoke(consumer, partitions):
        revoke_called.append(partitions)
        await consumer.incremental_unassign(partitions)

    await consumer.subscribe([topic], on_assign=on_assign, on_revoke=on_revoke)

    msg = await consumer.poll(10)
    assert msg is not None
    assert msg.value() == b'hello'

    await consumer.close()

    print(f"{called_by()}: revoke_called={len(revoke_called)}")
    assert revoke_called, "on_revoke was never invoked from close()"


async def test_on_assign_calls_non_reentrancy_eligible_method(kafka_cluster):
    """on_assign reentrantly awaits consumer.pause()/consumer.position(),
    neither of which is one of the 5 originally reentrancy-eligible methods
    (assign/unassign/incremental_assign/incremental_unassign/commit). Every
    AIOConsumer method generates-or-reuses an identity uniformly (see
    _AIOConsumer._call()), so this must work exactly the same as the
    specially-tested 5 -- confirming the design generalizes rather than
    happening to work only for the methods it was originally built for."""
    topic = kafka_cluster.create_topic_and_wait_propogation("test_aio_on_assign_calls_non_reentrancy_eligible")
    kafka_cluster.seed_topic(topic, value_source=[b'hello'])

    consumer = await _new_aio_consumer(kafka_cluster)

    pause_called = []
    position_results = []

    async def on_assign(consumer, partitions):
        await consumer.assign(partitions)
        await consumer.pause(partitions)
        pause_called.append(partitions)
        position_results.append(await consumer.position(partitions))
        await consumer.resume(partitions)

    await consumer.subscribe([topic], on_assign=on_assign)

    # Poll in a loop until msg arrives
    msg = None
    for _ in range(30):
        msg = await consumer.poll(1)
        if msg is not None:
            break

    print(f"{called_by()}: pause_called={len(pause_called)}, position_results={position_results}")
    assert pause_called, "on_assign was never invoked"
    assert position_results, "position() from within on_assign never completed"
    assert msg is not None
    assert msg.value() == b'hello'

    await consumer.close()


async def test_on_assign_calls_get_watermark_offsets_and_offsets_for_times(kafka_cluster):
    """on_assign reentrantly awaits consumer.get_watermark_offsets() and
    consumer.offsets_for_times() -- the two remaining gated methods with an
    _internal entry point that had never been exercised reentrantly
    anywhere in this suite."""
    topic = kafka_cluster.create_topic_and_wait_propogation("test_aio_on_assign_calls_watermark_and_times")
    kafka_cluster.seed_topic(topic, value_source=[b'hello'])

    consumer = await _new_aio_consumer(kafka_cluster)

    watermark_results = []
    offsets_for_times_results = []

    async def on_assign(consumer, partitions):
        await consumer.assign(partitions)
        watermark_results.append(await consumer.get_watermark_offsets(partitions[0]))
        offsets_for_times_results.append(
            await consumer.offsets_for_times([TopicPartition(p.topic, p.partition, 0) for p in partitions])
        )

    await consumer.subscribe([topic], on_assign=on_assign)

    msg = await consumer.poll(10)

    print(
        f"{called_by()}: watermark_results={watermark_results}, "
        f"offsets_for_times_results={offsets_for_times_results}"
    )
    assert watermark_results, "get_watermark_offsets() from within on_assign never completed"
    assert offsets_for_times_results, "offsets_for_times() from within on_assign never completed"
    assert msg is not None
    assert msg.value() == b'hello'

    await consumer.close()


async def test_on_commit_calls_commit_from_callback(kafka_cluster):
    """on_commit callback re-entrantly awaits consumer.commit(), dispatched back
    through the executor, may land on a different worker thread than the one blocked in the callback
    bridge."""
    topic = kafka_cluster.create_topic_and_wait_propogation("test_aio_on_commit_calls_commit")
    kafka_cluster.seed_topic(topic, value_source=[b'hello'])

    commit_called = []
    retried_commit_results = []
    consumer = None
    msg = None

    async def on_commit(err, partitions):
        commit_called.append((err, partitions))
        if len(commit_called) == 1:
            # Simulate a user retrying the commit from within the callback
            result = await consumer.commit(message=msg, asynchronous=False)
            retried_commit_results.append(result)

    consumer = await _new_aio_consumer(kafka_cluster, {'on_commit': on_commit})

    async def on_assign(consumer, partitions):
        await consumer.assign(partitions)

    await consumer.subscribe([topic], on_assign=on_assign)

    msg = await consumer.poll(10)
    assert msg is not None

    # on_commit only fires for asynchronous commits, and is served by a
    # later poll() call
    await consumer.commit(message=msg, asynchronous=True)
    for _ in range(50):
        await consumer.poll(0.1)
        if commit_called:
            break

    print(f"{called_by()}: commit_called={len(commit_called)}, retried_commit_results={retried_commit_results}")
    assert commit_called, "on_commit was never invoked"
    assert retried_commit_results, "the re-entrant commit() call from inside on_commit never completed"

    await consumer.close()


async def test_on_commit_calls_asynchronous_commit_from_callback(kafka_cluster):
    """Same re-entrant shape as test_on_commit_calls_commit_from_callback,
    but the re-entrant call is itself commit(asynchronous=True) rather than
    a synchronous retry. The re-entrant async commit() schedules yet another
    on_commit firing, served by a later poll()."""
    topic = kafka_cluster.create_topic_and_wait_propogation("test_aio_on_commit_calls_async_commit")
    kafka_cluster.seed_topic(topic, value_source=[b'hello'])

    commit_called = []
    reentrant_results = []
    consumer = None
    msg = None

    async def on_commit(err, partitions):
        commit_called.append((err, partitions))
        if len(commit_called) == 1:
            # Re-entrant call, but asynchronous this time: returns None
            # immediately rather than blocking for a broker round-trip.
            result = await consumer.commit(message=msg, asynchronous=True)
            reentrant_results.append(result)

    consumer = await _new_aio_consumer(kafka_cluster, {'on_commit': on_commit})

    async def on_assign(consumer, partitions):
        await consumer.assign(partitions)

    await consumer.subscribe([topic], on_assign=on_assign)

    msg = await consumer.poll(10)
    assert msg is not None

    await consumer.commit(message=msg, asynchronous=True)
    # Keep polling until BOTH the first on_commit and the second one it
    # schedules (via its own re-entrant asynchronous commit()) have fired.
    for _ in range(50):
        await consumer.poll(0.1)
        if len(commit_called) == 2:
            break

    print(f"{called_by()}: commit_called={len(commit_called)}, reentrant_results={reentrant_results}")
    assert reentrant_results, "the re-entrant asynchronous commit() call from inside on_commit never completed"
    assert reentrant_results[0] is None, "asynchronous commit() should return None"
    assert len(commit_called) == 2, (
        f"expected on_commit to fire exactly twice (initial + re-entrant asynchronous commit()), "
        f"got {len(commit_called)} calls: {commit_called}"
    )

    await consumer.close()


async def test_store_offsets_calls_store_offsets_from_callback(kafka_cluster):
    """store_offsets() called re-entrantly from within on_revoke."""
    topic = kafka_cluster.create_topic_and_wait_propogation("test_aio_store_offsets_from_callback")
    kafka_cluster.seed_topic(topic, value_source=[b'hello'])

    # store_offsets() requires manual offset storage.
    consumer = await _new_aio_consumer(kafka_cluster, {'enable.auto.offset.store': False})

    store_offsets_results = []
    msg = None

    async def on_assign(consumer, partitions):
        await consumer.assign(partitions)

    async def on_revoke(consumer, partitions):
        # By the time on_revoke can fire, the first poll() has already
        # returned a real message to store an offset for.
        if msg is not None:
            result = await consumer.store_offsets(message=msg)
            store_offsets_results.append(result)
        await consumer.unassign(partitions)

    await consumer.subscribe([topic], on_assign=on_assign, on_revoke=on_revoke)

    msg = await consumer.poll(10)
    assert msg is not None

    await consumer.unsubscribe()
    await consumer.poll(0)

    print(f"{called_by()}: store_offsets_results={store_offsets_results}")
    assert store_offsets_results, "the re-entrant store_offsets() call from inside on_revoke never completed"
    assert store_offsets_results[0] is None, "store_offsets() should return None"

    await consumer.close()


async def test_reentrant_call_error_releases_gate(kafka_cluster):
    """A re-entrant call from within on_commit that itself raises an exception,
    must still release the gate on every return path.
    If it didn't, gate_owner would stay stuck on the identity that raised,
    and the plain (non-re-entrant) commit() call made afterward would be
    incorrectly rejected as illegal concurrent access."""
    topic = kafka_cluster.create_topic_and_wait_propogation("test_reentrant_call_error_releases_gate")
    kafka_cluster.seed_topic(topic, value_source=[b'hello'])

    commit_called = []
    reentrant_errors = []
    consumer = None
    msg = None

    async def on_commit(err, partitions):
        commit_called.append((err, partitions))
        if len(commit_called) == 1:
            # Deliberately invalid: message and offsets are mutually
            # exclusive, so this reentrant call must raise ValueError.
            try:
                await consumer.commit(message=msg, offsets=[], asynchronous=False)
            except ValueError as e:
                reentrant_errors.append(e)

    consumer = await _new_aio_consumer(kafka_cluster, {'on_commit': on_commit})

    async def on_assign(consumer, partitions):
        await consumer.assign(partitions)

    await consumer.subscribe([topic], on_assign=on_assign)

    msg = await consumer.poll(10)
    assert msg is not None

    await consumer.commit(message=msg, asynchronous=True)
    for _ in range(50):
        await consumer.poll(0.1)
        if commit_called:
            break

    print(f"{called_by()}: commit_called={len(commit_called)}, reentrant_errors={reentrant_errors}")
    assert commit_called, "on_commit was never invoked"
    assert reentrant_errors, "the reentrant commit() call never raised ValueError as expected"

    # The gate must have been released despite the reentrant call's error --
    # this plain, non-re-entrant commit() call must succeed, not raise
    # ConcurrentModificationException from a stuck gate_owner.
    result = await consumer.commit(message=msg, asynchronous=False)
    assert result is not None

    await consumer.close()


async def test_concurrent_aio_consumers_independent_gates(kafka_cluster):
    """Several independent AIOConsumer instances, each with its own
    reentrant on_assign, driven concurrently via asyncio.gather(). The
    identity each instance generates comes from a single process-wide
    counter shared across all AIOConsumer instances, but gate_owner is a per-Handle field.
    This confirms two different instances' generated identities never interfere
    with each other's gate, even though they're drawn from the same
    counter."""

    async def _run_reentrant_assign_consumer(topic_prefix):
        topic = kafka_cluster.create_topic_and_wait_propogation(topic_prefix)
        kafka_cluster.seed_topic(topic, value_source=[topic.encode()])

        consumer = await _new_aio_consumer(kafka_cluster)

        assign_called = []

        async def on_assign(consumer, partitions):
            assign_called.append(partitions)
            await consumer.assign(partitions)

        await consumer.subscribe([topic], on_assign=on_assign)

        msg = await consumer.poll(10)

        await consumer.close()

        return topic, assign_called, msg

    n_consumers = 30
    results = await asyncio.gather(
        *[_run_reentrant_assign_consumer(f"test_concurrent_gates_{i}") for i in range(n_consumers)]
    )

    for topic, assign_called, msg in results:
        print(f"{called_by()}: topic={topic}, assign_called={len(assign_called)}, msg={msg.value() if msg else None}")
        assert assign_called, f"on_assign was never invoked for {topic}"
        assert msg is not None, f"no message received for {topic}"
        assert msg.value() == topic.encode(), f"unexpected message value for {topic}"


async def test_on_lost_calls_unassign_from_callback(kafka_cluster):
    """on_lost fires when the assignment is lost involuntarily -- rd_kafka_assignment_lost(rk) is true, e.g. after a
    max.poll.interval.ms violation gets the member evicted from the group
    -- a different branch in Consumer_rebalance_cb than the normal revoke
    path (see the block comment there). Forced here with a very short
    max.poll.interval.ms/session.timeout.ms plus a deliberate stall past
    it. Reentrantly awaits consumer.unassign() from inside on_lost."""
    topic = kafka_cluster.create_topic_and_wait_propogation("test_aio_on_lost_calls_unassign")
    kafka_cluster.seed_topic(topic, value_source=[b'hello'])

    consumer = await _new_aio_consumer(
        kafka_cluster,
        {
            'session.timeout.ms': 6000,
            'max.poll.interval.ms': 6000,
            'heartbeat.interval.ms': 1000,
        },
    )

    lost_called = []
    revoke_called = []

    async def on_revoke(consumer, partitions):
        revoke_called.append(partitions)

    async def on_lost(consumer, partitions):
        lost_called.append(partitions)
        await consumer.unassign(partitions)

    await consumer.subscribe([topic], on_revoke=on_revoke, on_lost=on_lost)

    msg = await consumer.poll(10)
    assert msg is not None
    assert msg.value() == b'hello'

    # Stall well past max.poll.interval.ms/session.timeout.ms without
    # polling, so the broker evicts this member from the group -- the next
    # poll() will detect the lost assignment and fire on_lost.
    await asyncio.sleep(9)

    for _ in range(50):
        await consumer.poll(0.1)
        if lost_called:
            break

    print(f"{called_by()}: lost_called={len(lost_called)}, revoke_called={len(revoke_called)}")
    assert lost_called, "on_lost was never invoked"
    assert not revoke_called, "on_revoke fired instead of on_lost -- assignment wasn't actually lost"

    await consumer.close()
