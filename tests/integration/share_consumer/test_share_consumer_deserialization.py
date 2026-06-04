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

"""Integration tests for DeserializingShareConsumer.

Roundtrip deserialization plus failure handling: a poison record is marked
(``msg.error()``) and kept rather than raised, regardless of
``share.acknowledgement.mode``, so the batch keeps flowing and the application
can ``REJECT`` it (KIP-932). In implicit mode librdkafka accepts the failed
record on the next poll, which keeps the consumer making forward progress.
"""

import time

from confluent_kafka import AcknowledgeType, KafkaError
from confluent_kafka.serialization import (
    IntegerDeserializer,
    IntegerSerializer,
    StringDeserializer,
    StringSerializer,
)
from tests.common import drain_share_consumers


def _poison_value_deserializer(data, ctx):
    """Decodes UTF-8, but raises on the sentinel value ``b'POISON'``."""
    if data == b'POISON':
        raise ValueError('cannot deserialize poison record')
    return data.decode('utf-8')


def _drain_explicit(share_consumer, n, timeout_s=20.0, poll_timeout_s=0.5):
    """Explicit-mode drain.

    Acknowledges every returned record (good -> ACCEPT, errored -> REJECT) before
    the next poll and commits each batch, until ``n`` distinct records are seen or
    ``timeout_s`` elapses. Returns ``{(partition, offset): Message}``.
    """
    seen = {}
    deadline = time.time() + timeout_s
    while time.time() < deadline and len(seen) < n:
        batch = share_consumer.poll(timeout=poll_timeout_s)
        for m in batch:
            seen[(m.partition(), m.offset())] = m
            if m.error() is not None:
                share_consumer.acknowledge(m, AcknowledgeType.REJECT)
            else:
                share_consumer.acknowledge(m, AcknowledgeType.ACCEPT)
        if batch:
            share_consumer.commit_sync(timeout=10.0)
    return seen


def test_roundtrip_value_deserialization(kafka_cluster):
    """Produced serialized values are deserialized on poll."""
    topic = kafka_cluster.create_topic_and_wait_propogation('test-dsc-roundtrip-value')
    n = 10

    producer = kafka_cluster.producer(value_serializer=StringSerializer())
    sc = kafka_cluster.deserializing_share_consumer(value_deserializer=StringDeserializer())
    try:
        sc.subscribe([topic])
        expected = [f'msg-{i}' for i in range(n)]
        for v in expected:
            producer.produce(topic, value=v)
        producer.flush(timeout=10.0)

        received = drain_share_consumers([sc], n)[0]
        assert sorted(m.value() for m in received) == sorted(expected)
        assert all(isinstance(m.value(), str) for m in received)
    finally:
        sc.close()


def test_roundtrip_key_and_value_deserialization(kafka_cluster):
    """Both key and value deserializers are applied."""
    topic = kafka_cluster.create_topic_and_wait_propogation('test-dsc-roundtrip-kv')
    n = 5

    producer = kafka_cluster.producer(key_serializer=StringSerializer(), value_serializer=IntegerSerializer())
    sc = kafka_cluster.deserializing_share_consumer(
        key_deserializer=StringDeserializer(), value_deserializer=IntegerDeserializer()
    )
    try:
        sc.subscribe([topic])
        for i in range(n):
            producer.produce(topic, key=f'k-{i}', value=i)
        producer.flush(timeout=10.0)

        received = drain_share_consumers([sc], n)[0]
        got = sorted((m.key(), m.value()) for m in received)
        assert got == sorted((f'k-{i}', i) for i in range(n))
    finally:
        sc.close()


def test_explicit_poison_marked_not_raised(kafka_cluster):
    """Explicit mode: a record that fails to deserialize is marked (not raised)
    and the rest of the batch still comes through deserialized."""
    topic = kafka_cluster.create_topic_and_wait_propogation('test-dsc-explicit-poison')

    producer = kafka_cluster.cimpl_producer()
    sc = kafka_cluster.deserializing_share_consumer(
        {'share.acknowledgement.mode': 'explicit'},
        value_deserializer=_poison_value_deserializer,
    )
    try:
        sc.subscribe([topic])
        for v in (b'alpha', b'POISON', b'beta'):
            producer.produce(topic, value=v)
        producer.flush(timeout=10.0)

        seen = _drain_explicit(sc, 3)
        assert len(seen) == 3, 'explicit mode must not lose the batch on a poison record'

        good = [m for m in seen.values() if m.error() is None]
        bad = [m for m in seen.values() if m.error() is not None]
        assert sorted(m.value() for m in good) == ['alpha', 'beta']
        assert len(bad) == 1
        assert bad[0].error().code() == KafkaError._VALUE_DESERIALIZATION
        assert bad[0].value() == b'POISON'  # raw bytes preserved
    finally:
        sc.close()


def test_explicit_poison_reject_not_redelivered(kafka_cluster):
    """Explicit mode: a REJECTed poison record is archived and not redelivered;
    ACCEPTed good records are not redelivered either."""
    topic = kafka_cluster.create_topic_and_wait_propogation('test-dsc-explicit-reject')

    producer = kafka_cluster.cimpl_producer()
    sc = kafka_cluster.deserializing_share_consumer(
        {'share.acknowledgement.mode': 'explicit'},
        value_deserializer=_poison_value_deserializer,
    )
    try:
        sc.subscribe([topic])
        for v in (b'good-1', b'POISON', b'good-2'):
            producer.produce(topic, value=v)
        producer.flush(timeout=10.0)

        seen = _drain_explicit(sc, 3)  # ACCEPT good, REJECT poison, commit each batch
        assert len(seen) == 3
        assert any(
            m.error() is not None and m.error().code() == KafkaError._VALUE_DESERIALIZATION for m in seen.values()
        )

        # REJECT -> Archived, ACCEPT -> Acknowledged: nothing should come back.
        extra = _drain_explicit(sc, 1, timeout_s=5.0)
        assert extra == {}, f'records redelivered after acknowledgement: {list(extra)}'
    finally:
        sc.close()


def test_implicit_poison_marked_not_raised(kafka_cluster):
    """Implicit mode marks a poison record (msg.error set) instead of raising —
    share.acknowledgement.mode makes no difference to this."""
    topic = kafka_cluster.create_topic_and_wait_propogation('test-dsc-implicit-mark')

    producer = kafka_cluster.cimpl_producer()
    sc = kafka_cluster.deserializing_share_consumer(value_deserializer=_poison_value_deserializer)
    try:
        sc.subscribe([topic])
        producer.produce(topic, value=b'POISON')
        producer.flush(timeout=10.0)

        poison = None
        deadline = time.time() + 20.0
        while time.time() < deadline and poison is None:
            for m in sc.poll(timeout=0.5):
                if m.error() is not None and m.error().code() == KafkaError._VALUE_DESERIALIZATION:
                    poison = m
        assert poison is not None, 'poison record never surfaced'
        assert poison.value() == b'POISON'  # raw bytes preserved
    finally:
        sc.close()


def test_implicit_poison_forward_progress(kafka_cluster):
    """A marked poison record is implicitly accepted on the next poll, so a good
    record produced afterwards still gets delivered (no getting stuck on poison)."""
    topic = kafka_cluster.create_topic_and_wait_propogation('test-dsc-implicit-progress')

    producer = kafka_cluster.cimpl_producer()
    sc = kafka_cluster.deserializing_share_consumer(value_deserializer=_poison_value_deserializer)
    try:
        sc.subscribe([topic])
        producer.produce(topic, value=b'POISON')
        producer.flush(timeout=10.0)

        # Phase 1: drain until the poison surfaces as a marked record.
        saw_poison = False
        deadline = time.time() + 20.0
        while time.time() < deadline and not saw_poison:
            for m in sc.poll(timeout=0.5):
                if m.error() is not None and m.error().code() == KafkaError._VALUE_DESERIALIZATION:
                    saw_poison = True
        assert saw_poison, 'poison should have surfaced as a marked record'

        # Phase 2: a good record produced afterwards must still be delivered.
        producer.produce(topic, value=b'good')
        producer.flush(timeout=10.0)

        got = []
        deadline = time.time() + 20.0
        while time.time() < deadline and not got:
            for m in sc.poll(timeout=0.5):
                if m.error() is None:
                    got.append(m.value())
        assert got == ['good'], f'expected forward progress to deliver "good", got {got}'
    finally:
        sc.close()
