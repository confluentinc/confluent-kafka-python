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
Miscellaneous AIOConsumer tests
"""

import asyncio
import concurrent.futures
import inspect
from uuid import uuid1

import pytest

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
    return TestAIOConsumer(consumer_conf)

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


async def test_shared_executor_across_aio_consumers(kafka_cluster):
    """Two independent AIOConsumer instances, each with its own reentrant
    on_assign, sharing a single externally-supplied ThreadPoolExecutor
    (AIOConsumer's executor constructor param). gate_owner is a per-Handle
    field, so the two instances' gates must stay independent even though
    they share not just the process-wide identity counter but the very same worker threads."""
    topic_a = kafka_cluster.create_topic_and_wait_propogation("test_shared_executor_a")
    topic_b = kafka_cluster.create_topic_and_wait_propogation("test_shared_executor_b")
    kafka_cluster.seed_topic(topic_a, value_source=[b'hello-a'])
    kafka_cluster.seed_topic(topic_b, value_source=[b'hello-b'])

    # 4 workers: 2 per instance, since each may need a second worker free
    # for a re-entrant call made from within a callback blocked on the
    # first.
    shared_executor = concurrent.futures.ThreadPoolExecutor(max_workers=4)

    def make_consumer(topic_suffix):
        consumer_conf = kafka_cluster.client_conf(
            {
                'group.id': str(uuid1()),
                'auto.offset.reset': 'earliest',
                'enable.auto.commit': False,
            }
        )
        return TestAIOConsumer(consumer_conf, executor=shared_executor)

    consumer_a = make_consumer(topic_a)
    consumer_b = make_consumer(topic_b)

    assign_called_a = []
    assign_called_b = []

    async def on_assign_a(consumer, partitions):
        assign_called_a.append(partitions)
        await consumer.assign(partitions)

    async def on_assign_b(consumer, partitions):
        assign_called_b.append(partitions)
        await consumer.assign(partitions)

    try:
        await consumer_a.subscribe([topic_a], on_assign=on_assign_a)
        await consumer_b.subscribe([topic_b], on_assign=on_assign_b)

        msg_a, msg_b = await asyncio.gather(consumer_a.poll(10), consumer_b.poll(10))

        print(
            f"{called_by()}: assign_called_a={len(assign_called_a)}, msg_a={msg_a.value() if msg_a else None}, "
            f"assign_called_b={len(assign_called_b)}, msg_b={msg_b.value() if msg_b else None}"
        )
        assert assign_called_a, "on_assign was never invoked for consumer_a"
        assert assign_called_b, "on_assign was never invoked for consumer_b"
        assert msg_a is not None and msg_a.value() == b'hello-a'
        assert msg_b is not None and msg_b.value() == b'hello-b'

        await consumer_a.close()
        await consumer_b.close()
    finally:
        shared_executor.shutdown(wait=True)


async def test_call_after_close_raises(kafka_cluster):
    """Calling any AIOConsumer method after close() has already completed
    must raise cleanly (RuntimeError("Consumer closed")"""
    topic = kafka_cluster.create_topic_and_wait_propogation("test_call_after_close_raises")
    kafka_cluster.seed_topic(topic, value_source=[b'hello'])

    consumer = await _new_aio_consumer(kafka_cluster)
    await consumer.subscribe([topic])
    msg = await consumer.poll(10)
    assert msg is not None

    await consumer.close()

    with pytest.raises(RuntimeError, match="Consumer closed"):
        await consumer.poll(1)

    with pytest.raises(RuntimeError, match="Consumer closed"):
        await consumer.close()

async def test_async_context_manager_closes_on_exit(kafka_cluster):
    """The consumer must be usable inside the ctx manager block,
    and __aexit__ must actually close it -- confirmed by a subsequent call
    raising the same way test_call_after_close_raises expects."""
    topic = kafka_cluster.create_topic_and_wait_propogation("test_async_context_manager_closes")
    kafka_cluster.seed_topic(topic, value_source=[b'hello'])

    consumer_conf = kafka_cluster.client_conf(
        {
            'group.id': str(uuid1()),
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False,
        }
    )

    async with TestAIOConsumer(consumer_conf, max_workers=2) as consumer:
        await consumer.subscribe([topic])
        msg = await consumer.poll(10)
        assert msg is not None
        assert msg.value() == b'hello'

    with pytest.raises(RuntimeError, match="Consumer closed"):
        await consumer.poll(1)
