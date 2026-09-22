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
#
"""Unit tests for AsyncSerializingProducer and AsyncDeserializingConsumer.

Broker-free, like tests/test_serde_builders.py: the clients are constructed
against an unreachable broker and the produce/poll calls of the underlying
AIOProducer/AIOConsumer are patched out.
"""

import asyncio

import pytest

from confluent_kafka import KafkaError
from confluent_kafka.aio import AIOConsumer, AIOProducer, AsyncDeserializingConsumer, AsyncSerializingProducer
from confluent_kafka.cimpl import Message
from confluent_kafka.error import (
    ConsumeError,
    KeySerializationError,
    ValueDeserializationError,
    ValueSerializationError,
)
from confluent_kafka.serialization import (
    DeserializerBuilder,
    SerializerBuilder,
    StringDeserializer,
    StringSerializer,
)

CLUSTER_ID = 'lkc-testcluster'
TOPIC = 't'


def _producer_conf(**extra):
    conf = {'bootstrap.servers': 'localhost:9092', 'socket.timeout.ms': 10}
    conf.update(extra)
    return conf


def _consumer_conf(**extra):
    conf = {'group.id': 'test-async-builders', 'bootstrap.servers': 'localhost:9092', 'socket.timeout.ms': 10}
    conf.update(extra)
    return conf


async def _producer(**extra):
    # buffer_timeout=0 keeps the timeout monitoring task out of the picture
    return await AsyncSerializingProducer(_producer_conf(**extra), buffer_timeout=0)


async def _consumer(**extra):
    return await AsyncDeserializingConsumer(_consumer_conf(**extra))


class _TrackingSerializer(StringSerializer):
    """Blocking serializer recording the resolver it was handed and how often it was closed."""

    def __init__(self):
        super().__init__()
        self.resolver = None
        self.closed = 0

    def set_cluster_id_resolver(self, resolver):
        self.resolver = resolver

    def close(self):
        self.closed += 1


class _AsyncTrackingSerializer(_TrackingSerializer):
    """Asyncio serializer: its call is a coroutine and it closes through aclose(), like the SR asyncio serdes."""

    async def __call__(self, obj, ctx):
        return super().__call__(obj, ctx)

    def close(self):
        raise AssertionError("asyncio serdes are closed through aclose()")

    async def aclose(self):
        self.closed += 1


class _AsyncTrackingDeserializer(StringDeserializer):
    def __init__(self):
        super().__init__()
        self.resolver = None
        self.closed = 0

    def set_cluster_id_resolver(self, resolver):
        self.resolver = resolver

    async def __call__(self, data, ctx):
        return super().__call__(data, ctx)

    async def aclose(self):
        self.closed += 1


class _AsyncBuilder(SerializerBuilder):
    """Builder whose build() is a coroutine, like the SR asyncio builders."""

    def __init__(self, serde):
        self._serde = serde
        self.calls = []

    async def build(self, conf, is_key):
        self.calls.append(is_key)
        remaining = dict(conf)
        remaining.pop('my.builder.prop', None)
        return self._serde, remaining


class _SyncBuilder(SerializerBuilder):
    def __init__(self, serde):
        self._serde = serde

    def build(self, conf, is_key):
        return self._serde, dict(conf)


class _AsyncDeserializerBuilder(DeserializerBuilder):
    def __init__(self, serde):
        self._serde = serde

    async def build(self, conf, is_key):
        return self._serde, dict(conf)


@pytest.fixture
def cluster_id_calls(monkeypatch):
    calls = []

    async def _cluster_id(self, timeout=-1):
        calls.append(timeout)
        return CLUSTER_ID

    monkeypatch.setattr(AIOProducer, 'cluster_id', _cluster_id)
    monkeypatch.setattr(AIOConsumer, 'cluster_id', _cluster_id)
    return calls


@pytest.fixture
def produced(monkeypatch):
    """Capture what reaches AIOProducer.produce instead of queueing it."""
    calls = []

    async def _produce(self, topic, value=None, key=None, **kwargs):
        calls.append((topic, value, key, kwargs))
        fut = asyncio.get_running_loop().create_future()
        fut.set_result(None)
        return fut

    monkeypatch.setattr(AIOProducer, 'produce', _produce)
    return calls


def _make_message(value=None, key=None, topic=TOPIC, error=None):
    return Message(topic, 0, 0, key, value, None, error, (0, 0), -1.0, -1)


@pytest.fixture
def polled(monkeypatch):
    """Feed messages to AIOConsumer.poll / consume from a list."""
    queue = []

    async def _poll(self, timeout=-1):
        return queue.pop(0) if queue else None

    async def _consume(self, num_messages=1, timeout=-1):
        batch, queue[:] = queue[:num_messages], queue[num_messages:]
        return batch

    monkeypatch.setattr(AIOConsumer, 'poll', _poll)
    monkeypatch.setattr(AIOConsumer, 'consume', _consume)
    return queue


# --- construction -----------------------------------------------------------


async def test_producer_awaits_async_builders(cluster_id_calls):
    key_serializer = _TrackingSerializer()
    value_serializer = _AsyncTrackingSerializer()
    key_builder = _SyncBuilder(key_serializer)
    value_builder = _AsyncBuilder(value_serializer)

    producer = await _producer(**{'key.serializer.builder': key_builder, 'value.serializer.builder': value_builder})
    try:
        assert producer._key_serializer is key_serializer
        assert producer._value_serializer is value_serializer
        assert value_builder.calls == [False]
        assert producer._owned_serdes == [key_serializer, value_serializer]
    finally:
        await producer.close()


async def test_builder_leftovers_reach_the_client(cluster_id_calls):
    # 'my.builder.prop' is not a librdkafka property: the client only
    # constructs because the builder popped it
    producer = await _producer(**{'my.builder.prop': 1, 'value.serializer.builder': _AsyncBuilder(StringSerializer())})
    await producer.close()


async def test_producer_rejects_serializer_and_builder_together(cluster_id_calls):
    with pytest.raises(ValueError, match='Cannot configure both'):
        await _producer(
            **{'value.serializer': StringSerializer(), 'value.serializer.builder': _AsyncBuilder(StringSerializer())}
        )


async def test_producer_is_generic():
    producer = await AsyncSerializingProducer[str, str](_producer_conf(), buffer_timeout=0)
    await producer.close()


async def test_consumer_awaits_async_builders(cluster_id_calls):
    deserializer = _AsyncTrackingDeserializer()
    consumer = await _consumer(**{'value.deserializer.builder': _AsyncDeserializerBuilder(deserializer)})
    try:
        assert consumer._value_deserializer is deserializer
        assert consumer._owned_serdes == [deserializer]
    finally:
        await consumer.close()


# --- cluster id -------------------------------------------------------------


async def test_cluster_id_is_resolved_lazily(cluster_id_calls):
    serializer = _AsyncTrackingSerializer()
    producer = await _producer(**{'value.serializer': serializer})
    try:
        # construction handed over a resolver without invoking it
        assert cluster_id_calls == []
        assert serializer.resolver is not None

        assert await serializer.resolver() == CLUSTER_ID
        assert cluster_id_calls == [60.0]
    finally:
        await producer.close()


async def test_consumer_hands_its_deserializers_a_resolver(cluster_id_calls):
    deserializer = _AsyncTrackingDeserializer()
    consumer = await _consumer(**{'value.deserializer': deserializer})
    try:
        assert cluster_id_calls == []
        assert await deserializer.resolver() == CLUSTER_ID
        assert cluster_id_calls == [60.0]
    finally:
        await consumer.close()


# --- produce ----------------------------------------------------------------


async def test_produce_serializes_with_async_and_blocking_serializers(cluster_id_calls, produced):
    producer = await _producer(
        **{'key.serializer': StringSerializer('utf_8'), 'value.serializer': _AsyncTrackingSerializer()}
    )
    try:
        fut = await producer.produce(TOPIC, key='k', value='v', partition=3)
        assert fut.done()
        assert produced == [(TOPIC, b'v', b'k', {'partition': 3})]
    finally:
        await producer.close()


async def test_produce_without_serializers_passes_bytes_through(cluster_id_calls, produced):
    producer = await _producer()
    try:
        await producer.produce(TOPIC, key=b'k', value=b'v')
        assert produced == [(TOPIC, b'v', b'k', {})]
    finally:
        await producer.close()


async def test_produce_wraps_serialization_errors(cluster_id_calls, produced):
    async def broken(obj, ctx):
        raise RuntimeError("boom")

    key_broken = await _producer(**{'key.serializer': broken, 'value.serializer': StringSerializer()})
    value_broken = await _producer(**{'key.serializer': StringSerializer(), 'value.serializer': broken})
    try:
        with pytest.raises(KeySerializationError):
            await key_broken.produce(TOPIC, key='k', value='v')
        with pytest.raises(ValueSerializationError):
            await value_broken.produce(TOPIC, key='k', value='v')
        assert produced == []
    finally:
        await key_broken.close()
        await value_broken.close()


# --- poll / consume ---------------------------------------------------------


async def test_poll_deserializes_key_and_value(cluster_id_calls, polled):
    consumer = await _consumer(
        **{'key.deserializer': StringDeserializer(), 'value.deserializer': _AsyncTrackingDeserializer()}
    )
    try:
        polled.append(_make_message(value=b'v', key=b'k'))
        msg = await consumer.poll(0)

        assert msg.deserialized_key() == 'k'
        assert msg.deserialized_value() == 'v'
        assert msg.value() is msg.deserialized_value()

        assert await consumer.poll(0) is None
    finally:
        await consumer.close()


async def test_poll_raises_on_message_errors(cluster_id_calls, polled):
    consumer = await _consumer(**{'value.deserializer': _AsyncTrackingDeserializer()})
    try:
        polled.append(_make_message(error=KafkaError(KafkaError._PARTITION_EOF)))
        with pytest.raises(ConsumeError):
            await consumer.poll(0)
    finally:
        await consumer.close()


async def test_consume_deserializes_a_batch_and_passes_errors_through(cluster_id_calls, polled):
    consumer = await _consumer(**{'value.deserializer': _AsyncTrackingDeserializer()})
    try:
        errored = _make_message(error=KafkaError(KafkaError._PARTITION_EOF))
        polled.extend([_make_message(value=b'1'), errored, _make_message(value=b'2')])

        msgs = await consumer.consume(3, timeout=0)

        assert [m.deserialized_value() for m in msgs] == ['1', None, '2']
        assert msgs[1] is errored
        assert await consumer.consume(3, timeout=0) == []
    finally:
        await consumer.close()


# --- ownership --------------------------------------------------------------


async def test_producer_closes_built_serdes_after_itself(cluster_id_calls, monkeypatch):
    order = []
    original_close = AIOProducer.close

    async def _close(self):
        order.append('producer')
        await original_close(self)

    monkeypatch.setattr(AIOProducer, 'close', _close)

    class _OrderedSerializer(_AsyncTrackingSerializer):
        async def aclose(self):
            order.append('serde')
            await super().aclose()

    supplied = _AsyncTrackingSerializer()
    built = _OrderedSerializer()
    producer = await _producer(**{'key.serializer': supplied, 'value.serializer.builder': _AsyncBuilder(built)})

    await producer.close()
    # a second close is a no-op: AIOProducer.close() cannot run twice, and the
    # serdes were already released
    await producer.close()

    assert order == ['producer', 'serde']
    assert built.closed == 1
    assert supplied.closed == 0


async def test_producer_context_manager_closes_built_serdes(cluster_id_calls):
    built = _AsyncTrackingSerializer()
    async with await _producer(**{'value.serializer.builder': _AsyncBuilder(built)}):
        assert built.closed == 0
    assert built.closed == 1


async def test_consumer_closes_built_serdes(cluster_id_calls):
    supplied = _AsyncTrackingDeserializer()
    built = _AsyncTrackingDeserializer()
    consumer = await _consumer(
        **{'key.deserializer': supplied, 'value.deserializer.builder': _AsyncDeserializerBuilder(built)}
    )

    await consumer.close()
    await consumer.close()

    assert built.closed == 1
    assert supplied.closed == 0


async def test_built_serdes_are_closed_when_the_client_fails_to_construct(cluster_id_calls):
    built = _AsyncTrackingSerializer()

    with pytest.raises(Exception):
        await _producer(**{'not.a.property': 'x', 'value.serializer.builder': _AsyncBuilder(built)})

    assert built.closed == 1


async def test_built_serdes_are_closed_when_a_later_builder_fails(cluster_id_calls):
    class _FailingBuilder(SerializerBuilder):
        async def build(self, conf, is_key):
            raise RuntimeError("builder broke")

    built = _AsyncTrackingSerializer()

    with pytest.raises(RuntimeError, match='builder broke'):
        await _producer(
            **{'key.serializer.builder': _AsyncBuilder(built), 'value.serializer.builder': _FailingBuilder()}
        )

    assert built.closed == 1


# --- Schema Registry asyncio serdes -----------------------------------------

sr_avro = pytest.importorskip('confluent_kafka.schema_registry.avro')
sr_client = pytest.importorskip('confluent_kafka.schema_registry._async.schema_registry_client')

SR_CONF = {'url': 'mock://'}
AVRO_SCHEMA = (
    '{"type":"record","name":"User","fields":['
    '{"name":"name","type":"string"},{"name":"favorite_number","type":"long"}]}'
)
USER = {'name': 'alice', 'favorite_number': 7}


@pytest.fixture
def sr_client_closes(monkeypatch):
    closed = []

    async def _aclose(self):
        closed.append(self)

    monkeypatch.setattr(sr_client.AsyncSchemaRegistryClient, 'aclose', _aclose)
    return closed


async def test_sr_builders_round_trip_through_the_async_clients(cluster_id_calls, produced, polled, sr_client_closes):
    # one mock registry shared by both sides, as a real one would be
    registry = sr_client.AsyncSchemaRegistryClient.new_client(SR_CONF)

    producer = await _producer(
        **{
            'key.serializer': StringSerializer('utf_8'),
            'value.serializer.builder': sr_avro.AsyncAvroSerializerBuilder(
                schema_registry_client=registry,
                schema=AVRO_SCHEMA,
                serializer_config={'subject.name.strategy.type': 'TOPIC'},
            ),
        }
    )
    consumer = await _consumer(
        **{
            'key.deserializer': StringDeserializer('utf_8'),
            'value.deserializer.builder': sr_avro.AsyncAvroDeserializerBuilder(schema_registry_client=registry),
        }
    )
    try:
        assert isinstance(producer._value_serializer, sr_avro.AsyncAvroSerializer)
        assert isinstance(consumer._value_deserializer, sr_avro.AsyncAvroDeserializer)

        await producer.produce(TOPIC, key='k', value=USER)
        _topic, value_bytes, key_bytes, _kwargs = produced[0]
        assert key_bytes == b'k'
        assert value_bytes[:1] == b'\x00'  # Schema Registry framing

        polled.append(_make_message(value=value_bytes, key=key_bytes))
        msg = await consumer.poll(0)
        assert msg.deserialized_key() == 'k'
        assert msg.deserialized_value() == USER
    finally:
        await producer.close()
        await consumer.close()

    # the registry was supplied by the application, so nobody closed it
    assert sr_client_closes == []


async def test_sr_client_built_from_config_is_closed_with_the_producer(cluster_id_calls, sr_client_closes):
    producer = await _producer(
        **{
            'value.serializer.builder': sr_avro.AsyncAvroSerializerBuilder(
                schema_registry_config=SR_CONF, schema=AVRO_SCHEMA
            )
        }
    )
    registry = producer._value_serializer._registry
    assert registry is not None

    await producer.close()

    assert sr_client_closes == [registry]


async def test_sr_serde_resolves_the_cluster_id_through_the_producer(cluster_id_calls, produced):
    registry = sr_client.AsyncSchemaRegistryClient.new_client(SR_CONF)
    producer = await _producer(
        **{
            'value.serializer.builder': sr_avro.AsyncAvroSerializerBuilder(
                schema_registry_client=registry, schema=AVRO_SCHEMA
            )
        }
    )
    try:
        # the default (associated) strategy asks the producer on the first message only
        assert cluster_id_calls == []
        await producer.produce(TOPIC, value=USER)
        assert cluster_id_calls == [60.0]
        await producer.produce(TOPIC, value=USER)
        assert cluster_id_calls == [60.0]
        # no association exists, so the strategy fell back to <topic>-value
        assert await registry.get_latest_version(TOPIC + '-value') is not None
    finally:
        await producer.close()


# --- produce fails fast, before the serializers run ---------------------------


def _recording_serializer(calls):
    def serializer(obj, ctx):
        calls.append(ctx.topic)
        return obj.encode('utf_8')

    return serializer


async def test_produce_after_close_fails_before_serializing(cluster_id_calls, produced):
    calls = []
    producer = await _producer(**{'value.serializer': _recording_serializer(calls)})
    await producer.close()

    with pytest.raises(RuntimeError, match='closed'):
        await producer.produce('t', value='x')
    assert calls == []
    assert produced == []


async def test_produce_rejects_a_non_str_topic_before_serializing(cluster_id_calls, produced):
    calls = []
    producer = await _producer(**{'value.serializer': _recording_serializer(calls)})
    try:
        with pytest.raises(TypeError, match='topic must be a str, not NoneType'):
            await producer.produce(None, value='x')
        assert calls == []
        assert produced == []
    finally:
        await producer.close()


# --- a message without a topic cannot be deserialized -------------------------


async def test_missing_topic_is_a_deserialization_error(cluster_id_calls, polled):
    consumer = await _consumer(**{'value.deserializer': StringDeserializer()})
    try:
        polled.append(_make_message(value=b'v', topic=None))
        with pytest.raises(ValueDeserializationError, match='non-empty topic name') as exc_info:
            await consumer.poll(0)
        assert exc_info.value.kafka_message.value() == b'v'
    finally:
        await consumer.close()


async def test_missing_topic_passes_through_without_deserializers(cluster_id_calls, polled):
    consumer = await _consumer()
    try:
        polled.append(_make_message(value=b'v', key=b'k', topic=None))
        msg = await consumer.poll(0)
        assert (msg.key(), msg.value()) == (b'k', b'v')
    finally:
        await consumer.close()
