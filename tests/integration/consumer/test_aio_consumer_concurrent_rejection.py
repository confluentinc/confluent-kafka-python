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
Tests for AIO Consumers gate's rejection path.

Out of multiple independent calls racing on the same underlying Consumer Handle,
only one must be allowed.
"""

import asyncio
from uuid import uuid1

import pytest

from confluent_kafka import ConcurrentModificationException
from confluent_kafka.aio._AIOConsumer import AIOConsumer
from tests.integration.conftest import consumer_gate_enabled

pytestmark = pytest.mark.skipif(
    not consumer_gate_enabled(),
    reason="Consumer gate is a no-op on Python versions <=3.14 GIL based; "
    "see CFL_CONSUMER_GATE_ENABLED in confluent_kafka.h",
)


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
    return AIOConsumer(consumer_conf, max_workers=2)


async def test_gate_rejects_concurrent_non_reentrant_calls(kafka_cluster):
    """Two independent, non-re-entrant calls on the same AIOConsumer at the
    same time: one long poll() (holding the gate for its full timeout) and
    one assign() launched shortly after, from a separate task, while the
    poll() is still in flight. Exactly one should raise ConcurrentModificationException.
    The consumer must remain usable afterward."""
    topic = kafka_cluster.create_topic_and_wait_propogation("test_gate_rejects_concurrent")
    kafka_cluster.seed_topic(topic, value_source=[b'hello'])

    consumer = await _new_aio_consumer(kafka_cluster)

    async def on_assign(consumer, partitions):
        await consumer.assign(partitions)

    await consumer.subscribe([topic], on_assign=on_assign)

    # Get the assignment so we have a real TopicPartition to pass to the
    # colliding assign() call below.
    msg = await consumer.poll(10)
    assert msg is not None
    partitions = await consumer.assignment()
    assert partitions

    poll_task = asyncio.create_task(consumer.poll(2))
    # Give poll() a moment to actually enter the gate on its worker thread
    # before launching the colliding call.
    await asyncio.sleep(0.2)
    assign_task = asyncio.create_task(consumer.assign(partitions))

    poll_result, assign_result = await asyncio.gather(poll_task, assign_task, return_exceptions=True)

    print(f"poll_result={poll_result}, assign_result={assign_result}")
    # poll_task had a head start, so it must already own the gate by
    # the time assign_task tries to enter -- assign_task is the one
    # expected to be rejected.
    assert not isinstance(poll_result, BaseException), f"poll_task unexpectedly raised: {poll_result}"
    assert isinstance(
        assign_result, ConcurrentModificationException
    ), f"expected assign_task to be rejected with ConcurrentModificationException, got: {assign_result}"

    # The consumer must still be usable after the rejection.
    result = await consumer.assignment()
    assert result

    await consumer.close()


async def test_gate_rejects_concurrent_close_during_poll(kafka_cluster):
    """A concurrent close() launched while a long poll() is still in flight
    (and holding the gate) must be rejected, not silently interleave with
    the in-progress poll() and tear down self->rk out from under it. The
    consumer must remain open and usable afterward -- a successful
    rejection means close() never got far enough to touch self->rk at
    all."""
    topic = kafka_cluster.create_topic_and_wait_propogation("test_gate_rejects_close_during_poll")
    kafka_cluster.seed_topic(topic, value_source=[b'hello'])

    consumer = await _new_aio_consumer(kafka_cluster)

    async def on_assign(consumer, partitions):
        await consumer.assign(partitions)

    await consumer.subscribe([topic], on_assign=on_assign)

    msg = await consumer.poll(10)
    assert msg is not None

    poll_task = asyncio.create_task(consumer.poll(2))
    await asyncio.sleep(0.2)
    close_task = asyncio.create_task(consumer.close())

    poll_result, close_result = await asyncio.gather(poll_task, close_task, return_exceptions=True)

    print(f"poll_result={poll_result}, close_result={close_result}")
    assert not isinstance(poll_result, BaseException), f"poll_task unexpectedly raised: {poll_result}"
    assert isinstance(
        close_result, ConcurrentModificationException
    ), f"expected close_task to be rejected with ConcurrentModificationException, got: {close_result}"

    # The consumer must still be open and usable -- the rejected close()
    # must not have torn down self->rk.
    result = await consumer.assignment()
    assert result

    await consumer.close()


async def test_gate_rejects_top_level_call_during_reentrant_call(kafka_cluster):
    """on_assign (fired synchronously from within poll()) makes a whole
    loop of re-entrant incremental_assign()/incremental_unassign() calls,
    each legitimately borrowing the gate by presenting the identity poll()
    generated -- every one of them must succeed. Meanwhile, a genuinely
    independent top-level commit() launched from a separate task, sometime
    while that loop is still running, presents its own different identity
    and must collide with poll()'s -- it must be rejected with
    ConcurrentModificationException."""
    topic = kafka_cluster.create_topic_and_wait_propogation("test_gate_rejects_top_level_during_reentrant")
    kafka_cluster.seed_topic(topic, value_source=[b'hello'])

    consumer = await _new_aio_consumer(kafka_cluster, {'partition.assignment.strategy': 'cooperative-sticky'})

    loop_results = []
    n_iterations = 20

    async def on_assign(consumer, partitions):
        for i in range(n_iterations):
            await consumer.incremental_assign(partitions)
            await consumer.incremental_unassign(partitions)
            loop_results.append(i)
        # Leave the partitions actually assigned so poll() can deliver the
        # seeded message.
        await consumer.incremental_assign(partitions)

    await consumer.subscribe([topic], on_assign=on_assign)

    poll_task = asyncio.create_task(consumer.poll(10))
    # Launch the colliding top-level call while the on_assign loop is
    # (almost certainly) still running -- it fires synchronously as the
    # very first thing inside poll(), before poll() can return a message.
    await asyncio.sleep(0.2)
    commit_task = asyncio.create_task(consumer.commit(asynchronous=False))

    poll_result, commit_result = await asyncio.gather(poll_task, commit_task, return_exceptions=True)

    print(
        f"loop_results={len(loop_results)}/{n_iterations}, " f"poll_result={poll_result}, commit_result={commit_result}"
    )
    assert (
        len(loop_results) == n_iterations
    ), f"expected all {n_iterations} re-entrant loop calls to succeed, got {len(loop_results)}"
    assert not isinstance(poll_result, BaseException), f"poll_task unexpectedly raised: {poll_result}"
    assert poll_result is not None
    assert poll_result.value() == b'hello'
    assert isinstance(
        commit_result, ConcurrentModificationException
    ), f"expected the independent top-level commit() call to be rejected, got: {commit_result}"

    result = await consumer.assignment()
    assert result

    await consumer.close()
