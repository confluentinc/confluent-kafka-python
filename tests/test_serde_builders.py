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
"""Unit tests for serde builders and cluster id propagation.

Broker-free: the clients are constructed against an unreachable broker, which
librdkafka accepts without connecting, and ``cluster_id`` is patched out so no
metadata request is ever issued.
"""

import pytest

from confluent_kafka import DeserializingConsumer, SerializingProducer
from confluent_kafka.cimpl import Message
from confluent_kafka.serialization import (
    Deserializer,
    DeserializerBuilder,
    Serializer,
    SerializerBuilder,
    StringDeserializer,
    StringSerializer,
)

CLUSTER_ID = 'lkc-testcluster'


def _producer_conf(**extra):
    conf = {'bootstrap.servers': 'localhost:9092', 'socket.timeout.ms': 10}
    conf.update(extra)
    return conf


def _consumer_conf(**extra):
    conf = {'group.id': 'test-builders', 'bootstrap.servers': 'localhost:9092', 'socket.timeout.ms': 10}
    conf.update(extra)
    return conf


class _ClusterIdSerializer(StringSerializer):
    """Serializer that asks for the cluster id, like the SR serdes do."""

    def __init__(self, needs=True):
        super().__init__()
        self._needs = needs
        self.cluster_id = None

    def needs_cluster_id(self):
        return self._needs

    def set_cluster_id(self, cluster_id):
        self.cluster_id = cluster_id
        self._needs = False


class _ClusterIdDeserializer(StringDeserializer):
    """Deserializing counterpart of :class:`_ClusterIdSerializer`."""

    def __init__(self, needs=True):
        super().__init__()
        self._needs = needs
        self.cluster_id = None

    def needs_cluster_id(self):
        return self._needs

    def set_cluster_id(self, cluster_id):
        self.cluster_id = cluster_id
        self._needs = False


class _RecordingBuilder(SerializerBuilder):
    """Records how it was called and consumes ``my.builder.prop`` from the config."""

    def __init__(self, serde=None):
        self._serde = serde if serde is not None else StringSerializer()
        self.calls = []

    def build(self, conf, is_key):
        self.calls.append((dict(conf), is_key))
        remaining = dict(conf)
        remaining.pop('my.builder.prop', None)
        return self._serde, remaining


class _RecordingDeserializerBuilder(DeserializerBuilder):
    def __init__(self, serde=None):
        self._serde = serde if serde is not None else StringDeserializer()
        self.calls = []

    def build(self, conf, is_key):
        self.calls.append((dict(conf), is_key))
        remaining = dict(conf)
        remaining.pop('my.builder.prop', None)
        return self._serde, remaining


@pytest.fixture
def no_cluster_id(monkeypatch):
    """Fail the test if the cluster id is fetched, and record it when it is expected."""
    calls = []

    def _cluster_id(self, timeout=-1):
        calls.append(timeout)
        return CLUSTER_ID

    monkeypatch.setattr(SerializingProducer, 'cluster_id', _cluster_id, raising=False)
    monkeypatch.setattr(DeserializingConsumer, 'cluster_id', _cluster_id, raising=False)
    return calls


# --- builder wiring ---------------------------------------------------------


def test_producer_builds_value_serializer(no_cluster_id):
    builder = _RecordingBuilder()
    producer = SerializingProducer(_producer_conf(**{'value.serializer.builder': builder}))

    assert producer._value_serializer is builder._serde
    assert producer._key_serializer is None
    assert [is_key for _conf, is_key in builder.calls] == [False]


def test_producer_builds_both_serializers(no_cluster_id):
    key_builder = _RecordingBuilder()
    value_builder = _RecordingBuilder()
    producer = SerializingProducer(
        _producer_conf(**{'key.serializer.builder': key_builder, 'value.serializer.builder': value_builder})
    )

    assert producer._key_serializer is key_builder._serde
    assert producer._value_serializer is value_builder._serde
    # the key builder runs first and is told so
    assert [is_key for _conf, is_key in key_builder.calls] == [True]
    assert [is_key for _conf, is_key in value_builder.calls] == [False]


def test_builder_sees_client_conf_and_its_leftovers_reach_the_client(no_cluster_id):
    # 'my.builder.prop' is not a librdkafka property, so the client would fail
    # to construct if the builder's leftover config were not the one used.
    builder = _RecordingBuilder()
    SerializingProducer(_producer_conf(**{'my.builder.prop': 'consumed', 'value.serializer.builder': builder}))

    seen_conf, _is_key = builder.calls[0]
    assert seen_conf['my.builder.prop'] == 'consumed'
    assert seen_conf['bootstrap.servers'] == 'localhost:9092'


def test_builders_are_chained_so_the_second_sees_the_first_leftovers(no_cluster_id):
    key_builder = _RecordingBuilder()
    value_builder = _RecordingBuilder()
    SerializingProducer(
        _producer_conf(
            **{
                'my.builder.prop': 'consumed',
                'key.serializer.builder': key_builder,
                'value.serializer.builder': value_builder,
            }
        )
    )

    assert 'my.builder.prop' in key_builder.calls[0][0]
    # the key builder popped it, so the value builder never sees it
    assert 'my.builder.prop' not in value_builder.calls[0][0]


def test_consumer_builds_deserializers(no_cluster_id):
    key_builder = _RecordingDeserializerBuilder()
    value_builder = _RecordingDeserializerBuilder()
    consumer = DeserializingConsumer(
        _consumer_conf(**{'key.deserializer.builder': key_builder, 'value.deserializer.builder': value_builder})
    )

    assert consumer._key_deserializer is key_builder._serde
    assert consumer._value_deserializer is value_builder._serde
    assert [is_key for _conf, is_key in key_builder.calls] == [True]
    assert [is_key for _conf, is_key in value_builder.calls] == [False]


@pytest.mark.parametrize('field', ['key', 'value'])
def test_producer_rejects_serializer_and_builder_together(no_cluster_id, field):
    conf = _producer_conf(
        **{
            '{}.serializer'.format(field): StringSerializer(),
            '{}.serializer.builder'.format(field): _RecordingBuilder(),
        }
    )
    with pytest.raises(ValueError, match='Cannot configure both'):
        SerializingProducer(conf)


@pytest.mark.parametrize('field', ['key', 'value'])
def test_consumer_rejects_deserializer_and_builder_together(no_cluster_id, field):
    conf = _consumer_conf(
        **{
            '{}.deserializer'.format(field): StringDeserializer(),
            '{}.deserializer.builder'.format(field): _RecordingDeserializerBuilder(),
        }
    )
    with pytest.raises(ValueError, match='Cannot configure both'):
        DeserializingConsumer(conf)


def test_builder_returning_a_non_dict_conf_is_reported(no_cluster_id):
    class BadBuilder(SerializerBuilder):
        def build(self, conf, is_key):
            return StringSerializer(), None

    with pytest.raises(ValueError, match='value.serializer.builder'):
        SerializingProducer(_producer_conf(**{'value.serializer.builder': BadBuilder()}))


def test_builder_returning_a_bare_serde_is_reported(no_cluster_id):
    class BadBuilder(SerializerBuilder):
        def build(self, conf, is_key):
            return StringSerializer()

    with pytest.raises(ValueError, match='must return a'):
        SerializingProducer(_producer_conf(**{'value.serializer.builder': BadBuilder()}))


# --- cluster id propagation -------------------------------------------------


def test_cluster_id_not_fetched_when_no_serde_needs_it(no_cluster_id):
    SerializingProducer(_producer_conf(**{'value.serializer': StringSerializer()}))
    assert no_cluster_id == []


def test_cluster_id_not_fetched_for_plain_callables(no_cluster_id):
    # a lambda has no needs_cluster_id() at all and must not trip the lookup
    SerializingProducer(_producer_conf(**{'value.serializer': lambda obj, ctx: b''}))
    assert no_cluster_id == []


def test_cluster_id_propagated_to_serializer(no_cluster_id):
    serializer = _ClusterIdSerializer()
    producer = SerializingProducer(_producer_conf(**{'value.serializer': serializer}))

    assert serializer.cluster_id == CLUSTER_ID
    assert producer._value_serializer is serializer
    assert no_cluster_id == [60.0]


def test_cluster_id_fetched_once_for_both_serializers(no_cluster_id):
    key_serializer = _ClusterIdSerializer()
    value_serializer = _ClusterIdSerializer()
    SerializingProducer(_producer_conf(**{'key.serializer': key_serializer, 'value.serializer': value_serializer}))

    assert key_serializer.cluster_id == CLUSTER_ID
    assert value_serializer.cluster_id == CLUSTER_ID
    assert len(no_cluster_id) == 1


def test_cluster_id_only_given_to_the_serde_that_asked(no_cluster_id):
    needy = _ClusterIdSerializer()
    content = _ClusterIdSerializer(needs=False)
    SerializingProducer(_producer_conf(**{'key.serializer': content, 'value.serializer': needy}))

    assert needy.cluster_id == CLUSTER_ID
    assert content.cluster_id is None


def test_cluster_id_propagated_to_deserializer(no_cluster_id):
    deserializer = _ClusterIdDeserializer()
    DeserializingConsumer(_consumer_conf(**{'value.deserializer': deserializer}))

    assert deserializer.cluster_id == CLUSTER_ID
    assert no_cluster_id == [60.0]


def test_cluster_id_propagated_to_built_serde(no_cluster_id):
    serializer = _ClusterIdSerializer()
    SerializingProducer(_producer_conf(**{'value.serializer.builder': _RecordingBuilder(serializer)}))

    assert serializer.cluster_id == CLUSTER_ID


# --- Schema Registry builders -----------------------------------------------

# The Schema Registry builders are optional extras, so skip rather than fail
# when they are not installed.
sr_avro = pytest.importorskip('confluent_kafka.schema_registry.avro')
sr_json = pytest.importorskip('confluent_kafka.schema_registry.json_schema')

SR_CONF = {'url': 'http://localhost:8081'}
AVRO_SCHEMA = (
    '{"type":"record","name":"User","fields":['
    '{"name":"name","type":"string"},{"name":"favorite_number","type":"long"}]}'
)


def _identity(obj, ctx):
    return obj


@pytest.mark.parametrize(
    'builder_factory',
    [
        # constructor parameters
        lambda: sr_avro.AvroSerializerBuilder(
            schema_registry_config=SR_CONF,
            schema=AVRO_SCHEMA,
            to_dict=_identity,
            serializer_config={'auto.register.schemas': False},
        ),
        # the equivalent setter chain
        lambda: sr_avro.AvroSerializerBuilder()
        .set_schema_registry_config(SR_CONF)
        .set_schema(AVRO_SCHEMA)
        .set_to_dict(_identity)
        .set_serializer_config({'auto.register.schemas': False}),
    ],
    ids=['constructor', 'setters'],
)
def test_avro_builder_accepts_both_forms(builder_factory):
    serializer, remaining = builder_factory().build({'bootstrap.servers': 'localhost:9092'}, False)

    # the config the client still needs is handed back untouched
    assert remaining == {'bootstrap.servers': 'localhost:9092'}
    # the values reached the serializer rather than being silently dropped
    assert serializer._auto_register is False
    assert serializer._to_dict is _identity


def test_builder_without_sr_config_passes_a_none_client():
    # The JSON deserializer accepts no Schema Registry client (the json_consumer
    # example relies on this). The builder must pass None through rather than
    # fabricating a client from empty config, which would fail with
    # "Missing required configuration property url".
    deserializer, _ = sr_json.JSONDeserializerBuilder(schema=AVRO_SCHEMA, from_dict=_identity).build({}, False)

    assert deserializer._registry is None


def test_builder_constructor_and_setters_agree():
    from_ctor, _ = sr_json.JSONDeserializerBuilder(schema=AVRO_SCHEMA, schema_registry_config=SR_CONF).build({}, False)
    from_setters, _ = (
        sr_json.JSONDeserializerBuilder().set_schema(AVRO_SCHEMA).set_schema_registry_config(SR_CONF).build({}, False)
    )

    assert type(from_ctor) is type(from_setters)


def test_setters_override_constructor_arguments():
    builder = sr_avro.AvroSerializerBuilder(
        schema_registry_config=SR_CONF, schema=AVRO_SCHEMA, serializer_config={'auto.register.schemas': True}
    )
    builder.set_serializer_config({'auto.register.schemas': False})

    serializer, _ = builder.build({}, False)

    assert serializer._auto_register is False


def test_sr_serde_needs_cluster_id_until_it_is_configured():
    # the default subject name strategy is the associated one, which resolves
    # subjects against the cluster id
    serializer, _ = sr_avro.AvroSerializerBuilder(schema_registry_config=SR_CONF, schema=AVRO_SCHEMA).build({}, False)
    assert serializer.needs_cluster_id() is True

    serializer.set_cluster_id(CLUSTER_ID)
    assert serializer.needs_cluster_id() is False

    # an explicitly configured cluster id means no lookup is needed at all
    configured, _ = sr_avro.AvroSerializerBuilder(
        schema_registry_config=SR_CONF,
        schema=AVRO_SCHEMA,
        serializer_config={'subject.name.strategy.conf': {'subject.name.strategy.kafka.cluster.id': 'lkc-configured'}},
    ).build({}, False)
    assert configured.needs_cluster_id() is False


# --- default serde hooks ----------------------------------------------------


def test_base_serdes_do_not_need_the_cluster_id():
    assert Serializer().needs_cluster_id() is False
    assert Deserializer().needs_cluster_id() is False
    # setting it anyway is a no-op rather than an error
    assert Serializer().set_cluster_id(CLUSTER_ID) is None
    assert Deserializer().set_cluster_id(CLUSTER_ID) is None


# --- deserialized accessors -------------------------------------------------


def _make_message(value=None, key=None, topic='t'):
    return Message(topic, 0, 0, key, value, None, None, (0, 0), -1.0, -1)


def test_deserialized_accessors_return_the_deserialized_objects(no_cluster_id):
    consumer = DeserializingConsumer(
        _consumer_conf(**{'key.deserializer': StringDeserializer(), 'value.deserializer': StringDeserializer()})
    )
    msg = consumer._deserialize(_make_message(value=b'v', key=b'k'))

    assert msg.deserialized_key() == 'k'
    assert msg.deserialized_value() == 'v'


def test_deserialized_accessors_alias_key_and_value(no_cluster_id):
    # they are two views of one slot; nothing about key()/value() changed
    consumer = DeserializingConsumer(_consumer_conf(**{'value.deserializer': lambda data, ctx: {'payload': data}}))
    msg = consumer._deserialize(_make_message(value=b'v', key=b'k'))

    assert msg.deserialized_value() is msg.value()
    assert msg.deserialized_key() is msg.key()
    assert msg.value() == {'payload': b'v'}


def test_deserialized_accessors_on_a_raw_message():
    # no deserializer ran, so they hand back the raw payload
    msg = _make_message(value=b'v', key=b'k')
    assert msg.deserialized_value() == b'v'
    assert msg.deserialized_key() == b'k'


def test_deserialized_accessors_preserve_none():
    msg = _make_message()
    assert msg.deserialized_value() is None
    assert msg.deserialized_key() is None
