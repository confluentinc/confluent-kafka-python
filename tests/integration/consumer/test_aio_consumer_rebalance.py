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
Tests for AIOConsumer rebalance-callback re-entrancy.

A rebalance/commit callback runs on
whichever ThreadPoolExecutor worker thread is blocked inside poll(), and is
bridged onto the event loop via run_coroutine_threadsafe(); if the callback
itself awaits a public AIOConsumer method (e.g. consumer.assign(...)), that
call gets re-dispatched to the executor and may land on a *different*
worker thread.
"""

import inspect
from uuid import uuid1

from confluent_kafka.aio._AIOConsumer import AIOConsumer


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
    return AIOConsumer(consumer_conf, max_workers=2)


async def test_on_assign_calls_assign_from_callback(kafka_cluster):
    """on_assign callback re-entrantly awaits consumer.assign() (eager
    protocol): the re-entrant call is dispatched back through the executor
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
    protocol) when the subscription is dropped via unsubscribe()."""
    topic = kafka_cluster.create_topic_and_wait_propogation("test_aio_on_revoke_calls_unassign")
    kafka_cluster.seed_topic(topic, value_source=[b'hello'])

    consumer = await _new_aio_consumer(kafka_cluster)

    revoke_called = []

    async def on_revoke(consumer, partitions):
        revoke_called.append(partitions)
        await consumer.unassign()

    await consumer.subscribe([topic], on_revoke=on_revoke)

    msg = await consumer.poll(10)
    assert msg is not None
    assert msg.value() == b'hello'

    await consumer.unsubscribe()
    await consumer.poll(0)

    print(f"{called_by()}: revoke_called={len(revoke_called)}")
    assert revoke_called, "on_revoke was never invoked"

    await consumer.close()


async def test_on_assign_default_fallback_without_calling_assign(kafka_cluster):
    """on_assign callback that does *not* call assign()/incremental_assign()
    itself: librdkafka performs the fallback assign automatically. No
    re-entrancy here."""
    topic = kafka_cluster.create_topic_and_wait_propogation("test_aio_on_assign_default_fallback")
    kafka_cluster.seed_topic(topic, value_source=[b'hello'])

    consumer = await _new_aio_consumer(kafka_cluster)

    assign_called = []

    async def on_assign(consumer, partitions):
        assign_called.append(partitions)
        # Deliberately does not call consumer.assign(partitions).

    await consumer.subscribe([topic], on_assign=on_assign)

    msg = await consumer.poll(10)

    print(f"{called_by()}: assign_called={len(assign_called)}, msg={msg.value() if msg else None}")
    assert assign_called, "on_assign was never invoked"
    assert msg is not None
    assert msg.value() == b'hello'

    await consumer.close()


async def test_on_commit_calls_commit_from_callback(kafka_cluster):
    """on_commit callback re-entrantly awaits consumer.commit() (e.g. a
    retry-on-failure pattern): dispatched back through the executor, may
    land on a different worker thread than the one blocked in the callback
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
            # Simulate a user retrying the commit from within the callback --
            # the re-entrant call this test exists to exercise.
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
