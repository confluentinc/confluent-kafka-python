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
Tests for the AIO Consumer gate's serialization behavior.

Out of multiple independent calls racing on the same underlying Consumer
Handle, a colliding call waits for the current holder to release the gate,
then proceeds -- it is not rejected. Only a genuinely re-entrant call (same
identity) is admitted immediately. Also covers same-identity concurrency:
concurrent or un-awaited calls a callback dispatches back into the Consumer
(gather(), or an un-awaited create_task()) all present the same identity,
so the gate alone can't tell them apart from a single sequential re-entrant call.
"""

import asyncio
import time
from uuid import uuid1

from tests.common import TestAIOConsumer


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
    return TestAIOConsumer(consumer_conf, max_workers=3)


async def test_gate_waits_for_concurrent_non_reentrant_calls(kafka_cluster):
    """Two independent, non-re-entrant calls on the same AIOConsumer at the
    same time: one long poll() (holding the gate for its full timeout) and
    one position() launched shortly after, from a separate task, while the
    poll() is still in flight. position() must wait for poll() to release
    the gate, then succeed. The consumer must remain usable afterward."""
    topic = kafka_cluster.create_topic_and_wait_propogation("test_gate_waits_concurrent")
    kafka_cluster.seed_topic(topic, value_source=[b'hello'])

    consumer = await _new_aio_consumer(kafka_cluster)

    async def on_assign(consumer, partitions):
        await consumer.assign(partitions)

    await consumer.subscribe([topic], on_assign=on_assign)

    # Get the assignment so we have a real TopicPartition to pass to the
    # colliding position() call below.
    msg = await consumer.poll(10)
    assert msg is not None
    partitions = await consumer.assignment()
    assert partitions

    poll_task = asyncio.create_task(consumer.poll(2))
    # Give poll() a moment to actually enter the gate on its worker thread
    # before launching the colliding call.
    await asyncio.sleep(0.2)
    t0 = time.time()
    position_task = asyncio.create_task(consumer.position(partitions))

    poll_result, position_result = await asyncio.gather(poll_task, position_task, return_exceptions=True)
    position_elapsed = time.time() - t0

    print(f"poll_result={poll_result}, position_result={position_result}, position_elapsed={position_elapsed:.2f}")
    assert not isinstance(poll_result, BaseException), f"poll_task unexpectedly raised: {poll_result}"
    assert not isinstance(
        position_result, BaseException
    ), f"expected position_task to wait and then succeed, got: {position_result}"
    # position_task started ~0.2s in; poll_task releases the gate at ~2.0s,
    # so it must have actually waited rather than slipping through
    # immediately.
    assert position_elapsed >= 1.0, (
        f"position_task returned in {position_elapsed:.2f}s -- too fast to "
        f"have actually waited for poll()'s gate hold"
    )

    # The consumer must still be usable afterward.
    result = await consumer.assignment()
    assert result

    await consumer.close()


async def test_close_waits_for_poll_then_succeeds(kafka_cluster):
    """A concurrent close() launched while a long poll() is still in flight
    (and holding the gate) must wait for poll() to release the gate,
    then close() genuinely proceeds."""
    topic = kafka_cluster.create_topic_and_wait_propogation("test_close_waits_for_poll")
    kafka_cluster.seed_topic(topic, value_source=[b'hello'])

    consumer = await _new_aio_consumer(kafka_cluster)

    async def on_assign(consumer, partitions):
        await consumer.assign(partitions)

    await consumer.subscribe([topic], on_assign=on_assign)

    msg = await consumer.poll(10)
    assert msg is not None

    poll_task = asyncio.create_task(consumer.poll(2))
    await asyncio.sleep(0.2)
    t0 = time.time()
    close_task = asyncio.create_task(consumer.close())

    poll_result, close_result = await asyncio.gather(poll_task, close_task, return_exceptions=True)
    close_elapsed = time.time() - t0

    print(f"poll_result={poll_result}, close_result={close_result}, close_elapsed={close_elapsed:.2f}")
    assert not isinstance(poll_result, BaseException), f"poll_task unexpectedly raised: {poll_result}"
    assert not isinstance(
        close_result, BaseException
    ), f"expected close_task to wait and then succeed, got: {close_result}"
    # close_task started ~0.2s in; poll_task releases the gate at ~2.0s, so
    # close() must have actually waited rather than racing it.
    assert close_elapsed >= 1.0, (
        f"close_task returned in {close_elapsed:.2f}s -- too fast to have " f"actually waited for poll()'s gate hold"
    )


async def test_gate_waits_for_top_level_call_during_reentrant_call(kafka_cluster):
    """on_assign (fired synchronously from within poll()) makes a whole
    loop of re-entrant incremental_assign()/incremental_unassign() calls,
    each legitimately borrowing the gate by presenting the identity poll()
    generated -- every one of them must succeed. Meanwhile, a genuinely
    independent top-level commit() launched from a separate task, while
    poll() is still in progress, presents its own different identity and
    must wait for the gate rather than being rejected, then succeed once
    poll() (and its re-entrant loop) finishes."""
    topic = kafka_cluster.create_topic_and_wait_propogation("test_gate_waits_top_level_during_reentrant")

    consumer = await _new_aio_consumer(kafka_cluster, {'partition.assignment.strategy': 'cooperative-sticky'})

    loop_results = []
    n_iterations = 20

    async def on_assign(consumer, partitions):
        for i in range(n_iterations):
            await consumer.incremental_assign(partitions)
            await consumer.incremental_unassign(partitions)
            loop_results.append(i)
        # Leave the partitions actually assigned so poll() can deliver the
        # message seeded below once the collision has happened.
        await consumer.incremental_assign(partitions)

    await consumer.subscribe([topic], on_assign=on_assign)

    async def seed_after_collision():
        # Seed a message so poll_task can find it
        # and return instead of waiting out its full timeout.
        await asyncio.sleep(0.2)
        kafka_cluster.seed_topic(topic, value_source=[b'hello'])

    poll_task = asyncio.create_task(consumer.poll(10))
    # A short, deterministic head start for poll_task to reach the gate first.
    await asyncio.sleep(0.05)
    t0 = time.time()
    commit_task = asyncio.create_task(consumer.commit(asynchronous=False))
    seed_task = asyncio.create_task(seed_after_collision())

    poll_result, commit_result, _ = await asyncio.gather(poll_task, commit_task, seed_task, return_exceptions=True)
    commit_elapsed = time.time() - t0

    print(
        f"loop_results={len(loop_results)}/{n_iterations}, poll_result={poll_result}, "
        f"commit_result={commit_result}, commit_elapsed={commit_elapsed:.2f}"
    )
    # commit_elapsed isn't asserted against a floor: its wait is bounded by
    # the re-entrant loop's own duration plus a real broker round-trip,
    # neither of which is as controllable as poll()'s fixed timeout in the
    # other tests in this file -- it's printed for diagnostics only.
    assert (
        len(loop_results) == n_iterations
    ), f"expected all {n_iterations} re-entrant loop calls to succeed, got {len(loop_results)}"
    assert not isinstance(poll_result, BaseException), f"poll_task unexpectedly raised: {poll_result}"
    assert poll_result is not None
    assert poll_result.value() == b'hello'
    assert not isinstance(
        commit_result, BaseException
    ), f"expected the independent top-level commit() call to wait and then succeed, got: {commit_result}"

    result = await consumer.assignment()
    assert result

    await consumer.close()


async def test_gathered_reentrant_calls_from_callback_serialize(kafka_cluster):
    """on_assign completes the required incremental_assign() first (fast),
    then reentrantly fires two concurrent consume() calls via
    asyncio.gather(). Both dispatches present the same identity, so the gate
    alone would let them both through at once, the per-callback lock must
    serialize them instead."""
    topic = kafka_cluster.create_topic_and_wait_propogation(
        "test_gathered_reentrant_serialize", conf={'num_partitions': 4}
    )

    consumer = await _new_aio_consumer(kafka_cluster, {'partition.assignment.strategy': 'cooperative-sticky'})

    elapsed = []

    async def on_assign(consumer, partitions):
        await consumer.incremental_assign(partitions)
        t0 = time.time()
        await asyncio.gather(
            consumer.consume(num_messages=1, timeout=1.0),
            consumer.consume(num_messages=1, timeout=1.0),
        )
        elapsed.append(time.time() - t0)

    await consumer.subscribe([topic], on_assign=on_assign)
    await consumer.poll(10)

    result = await consumer.assignment()

    print(f"elapsed={elapsed}, result={result}")
    assert elapsed, "on_assign was never invoked"
    assert elapsed[0] >= 2.0, (
        f"the two concurrent consume() calls completed in {elapsed[0]:.2f}s -- "
        f"too fast to have been serialized (each blocks ~1.0s with nothing to consume, "
        f"so a genuinely serialized pair must take >= 2.0s)"
    )
    assert len(result) == 4, f"expected all 4 partitions assigned, got {result}"

    await consumer.close()


async def test_joined_create_task_from_callback_serializes(kafka_cluster):
    """Same shape as test_gathered_reentrant_calls_from_callback_serialize,
    but after completing the required incremental_assign(), on_assign fires one
    consume() call via create_task() without immediately awaiting it, makes
    its own reentrant consume() call directly, and only awaits the created
    task afterward -- i.e. the task is created early but still joined before
    the callback returns. Must still serialize."""
    topic = kafka_cluster.create_topic_and_wait_propogation(
        "test_joined_task_reentrant_serialize", conf={'num_partitions': 4}
    )

    consumer = await _new_aio_consumer(kafka_cluster, {'partition.assignment.strategy': 'cooperative-sticky'})

    elapsed = []

    async def on_assign(consumer, partitions):
        await consumer.incremental_assign(partitions)
        t0 = time.time()
        task = asyncio.create_task(consumer.consume(num_messages=1, timeout=1.0))
        await consumer.consume(num_messages=1, timeout=1.0)
        await task
        elapsed.append(time.time() - t0)

    await consumer.subscribe([topic], on_assign=on_assign)
    await consumer.poll(10)

    result = await consumer.assignment()

    print(f"elapsed={elapsed}, result={result}")
    assert elapsed, "on_assign was never invoked"
    assert elapsed[0] >= 2.0, (
        f"the created-but-not-yet-awaited task and the direct reentrant call completed in "
        f"{elapsed[0]:.2f}s -- too fast to have been serialized (each blocks ~1.0s with "
        f"nothing to consume, so a genuinely serialized pair must take >= 2.0s)"
    )
    assert len(result) == 4, f"expected all 4 partitions assigned, got {result}"

    await consumer.close()


async def test_detached_create_task_from_callback_still_serializes_afterward(kafka_cluster):
    """on_assign fires a consume() call via create_task() and returns
    WITHOUT ever awaiting it. This lets the top-level poll() that invoked
    on_assign fully return and release the gate while the detached call may
    still be in flight (dispatched with on_assign's identity, on its own executor worker)."""
    topic = kafka_cluster.create_topic_and_wait_propogation(
        "test_detached_task_reentrant_serialize", conf={'num_partitions': 4}
    )

    consumer = await _new_aio_consumer(kafka_cluster, {'partition.assignment.strategy': 'cooperative-sticky'})

    detached_tasks = []

    async def on_assign(consumer, partitions):
        await consumer.incremental_assign(partitions)
        detached_tasks.append(asyncio.create_task(consumer.consume(num_messages=1, timeout=1.0)))

    await consumer.subscribe([topic], on_assign=on_assign)
    await consumer.poll(10)

    assert detached_tasks, "on_assign was never invoked"

    t0 = time.time()
    await consumer.consume(num_messages=1, timeout=1.0)
    elapsed = time.time() - t0

    result = await consumer.assignment()
    await detached_tasks[0]  # join it before close() so nothing is left dangling

    print(f"elapsed={elapsed:.2f}s, result={result}")
    assert elapsed >= 1.0, (
        f"the unrelated call issued right after poll() returned completed in {elapsed:.2f}s -- "
        f"too fast to have waited for on_assign's still-in-flight detached call, meaning the "
        f"gate let them run concurrently"
    )
    assert len(result) == 4, f"expected all 4 partitions assigned, got {result}"

    await consumer.close()


async def test_reentrant_gather_one_call_fails_lock_still_releases(kafka_cluster):
    """on_commit fires two concurrent reentrant commit() calls via gather():
    one deliberately invalid and one valid. The invalid call's failure must
    not leave the per-callback dispatch lock stuck -- the valid concurrent
    call must still complete, and the consumer must remain fully usable afterward."""
    topic = kafka_cluster.create_topic_and_wait_propogation("test_reentrant_gather_failure_releases_lock")
    kafka_cluster.seed_topic(topic, value_source=[b'hello'])

    commit_called = []
    reentrant_results = []
    consumer = None
    msg = None

    async def on_commit(err, partitions):
        commit_called.append((err, partitions))
        if len(commit_called) == 1:
            results = await asyncio.gather(
                consumer.commit(message=msg, offsets=[], asynchronous=False),  # invalid: raises ValueError
                consumer.commit(message=msg, asynchronous=False),  # valid
                return_exceptions=True,
            )
            reentrant_results.append(results)

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

    print(f"commit_called={len(commit_called)}, reentrant_results={reentrant_results}")
    assert commit_called, "on_commit was never invoked"
    assert reentrant_results, "the concurrent reentrant commit() calls never completed"
    results = reentrant_results[0]
    errors = [r for r in results if isinstance(r, Exception)]
    successes = [r for r in results if not isinstance(r, Exception)]
    assert len(errors) == 1 and isinstance(
        errors[0], ValueError
    ), f"expected exactly one ValueError from the invalid concurrent call, got: {results}"
    assert len(successes) == 1, f"expected the other concurrent commit() call to succeed, got: {results}"

    # The lock must have released despite the failure -- this plain,
    # non-re-entrant commit() call afterward must succeed.
    result = await consumer.commit(message=msg, asynchronous=False)
    assert result is not None

    await consumer.close()
