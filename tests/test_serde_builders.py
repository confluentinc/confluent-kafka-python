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
"""Unit tests for serde builders, cluster id resolution and serde ownership.

Broker-free: the clients are constructed against an unreachable broker, which
librdkafka accepts without connecting, and ``cluster_id`` is patched out so no
metadata request is ever issued.
"""

import pytest

from confluent_kafka import DeserializingConsumer, KafkaException, SerializingProducer
from confluent_kafka._serde_builder import build_serdes, pop_serde_props
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


class _TrackingSerializer(StringSerializer):
    """Serializer recording the resolver it was handed and how often it was closed."""

    def __init__(self):
        super().__init__()
        self.resolver = None
        self.closed = 0

    def set_cluster_id_resolver(self, resolver):
        self.resolver = resolver

    def close(self):
        self.closed += 1


class _TrackingDeserializer(StringDeserializer):
    """Deserializing counterpart of :class:`_TrackingSerializer`."""

    def __init__(self):
        super().__init__()
        self.resolver = None
        self.closed = 0

    def set_cluster_id_resolver(self, resolver):
        self.resolver = resolver

    def close(self):
        self.closed += 1


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


class _FailingBuilder(SerializerBuilder):
    def build(self, conf, is_key):
        raise RuntimeError("builder broke")


@pytest.fixture
def cluster_id_calls(monkeypatch):
    """Patch out the metadata request behind ``cluster_id()`` and record its invocations."""
    calls = []

    def _cluster_id(self, timeout=-1):
        calls.append(timeout)
        return CLUSTER_ID

    monkeypatch.setattr(SerializingProducer, 'cluster_id', _cluster_id, raising=False)
    monkeypatch.setattr(DeserializingConsumer, 'cluster_id', _cluster_id, raising=False)
    return calls


# --- builder wiring ---------------------------------------------------------


def test_producer_builds_value_serializer(cluster_id_calls):
    builder = _RecordingBuilder()
    producer = SerializingProducer(_producer_conf(**{'value.serializer.builder': builder}))

    assert producer._value_serializer is builder._serde
    assert producer._key_serializer is None
    assert [is_key for _conf, is_key in builder.calls] == [False]


def test_producer_builds_both_serializers(cluster_id_calls):
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


def test_builder_sees_client_conf_and_its_leftovers_reach_the_client(cluster_id_calls):
    # 'my.builder.prop' is not a librdkafka property, so the client would fail
    # to construct if the builder's leftover config were not the one used.
    builder = _RecordingBuilder()
    SerializingProducer(_producer_conf(**{'my.builder.prop': 'consumed', 'value.serializer.builder': builder}))

    seen_conf, _is_key = builder.calls[0]
    assert seen_conf['my.builder.prop'] == 'consumed'
    assert seen_conf['bootstrap.servers'] == 'localhost:9092'


class _ConsumingBuilder(SerializerBuilder):
    """Builder consuming the given properties and recording the configuration it saw."""

    def __init__(self, *props):
        self._props = props
        self.seen = None

    def build(self, conf, is_key):
        self.seen = dict(conf)
        remaining = {k: v for k, v in conf.items() if k not in self._props}
        return StringSerializer(), remaining


def test_every_builder_sees_the_full_conf_and_only_what_all_left_reaches_the_client(cluster_id_calls):
    # As in Go: a property both serdes need must reach both builders, and a
    # property consumed by either must not reach librdkafka. None of the three
    # is a librdkafka property, so the producer only constructs if all were
    # filtered out.
    key_builder = _ConsumingBuilder('shared.prop', 'key.only.prop')
    value_builder = _ConsumingBuilder('shared.prop', 'value.only.prop')
    SerializingProducer(
        _producer_conf(
            **{
                'shared.prop': 'both',
                'key.only.prop': 'k',
                'value.only.prop': 'v',
                'key.serializer.builder': key_builder,
                'value.serializer.builder': value_builder,
            }
        )
    )

    assert key_builder.seen['shared.prop'] == 'both' and key_builder.seen['value.only.prop'] == 'v'
    assert value_builder.seen['shared.prop'] == 'both' and value_builder.seen['key.only.prop'] == 'k'


def test_the_client_conf_is_the_intersection_of_the_builders_leftovers():
    specs, conf = pop_serde_props(
        {
            'bootstrap.servers': 'b',
            'shared.prop': 'both',
            'key.only.prop': 'k',
            'value.only.prop': 'v',
            'key.serializer.builder': _ConsumingBuilder('shared.prop', 'key.only.prop'),
            'value.serializer.builder': _ConsumingBuilder('shared.prop', 'value.only.prop'),
        },
        'key.serializer',
        'value.serializer',
    )

    _serdes, _owned, client_conf = build_serdes(specs, conf)

    assert client_conf == {'bootstrap.servers': 'b'}


def test_a_builder_cannot_leak_a_property_back_into_the_conf(cluster_id_calls):
    class _MutatingBuilder(SerializerBuilder):
        def build(self, conf, is_key):
            conf['not.a.kafka.prop'] = True  # mutates its own copy only
            return StringSerializer(), {k: v for k, v in conf.items() if k != 'not.a.kafka.prop'}

    # would fail to construct if the mutation reached the shared configuration
    SerializingProducer(
        _producer_conf(
            **{'key.serializer.builder': _MutatingBuilder(), 'value.serializer.builder': _RecordingBuilder()}
        )
    )


def test_consumer_builds_deserializers(cluster_id_calls):
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
def test_producer_rejects_serializer_and_builder_together(cluster_id_calls, field):
    conf = _producer_conf(
        **{
            '{}.serializer'.format(field): StringSerializer(),
            '{}.serializer.builder'.format(field): _RecordingBuilder(),
        }
    )
    with pytest.raises(ValueError, match='Cannot configure both'):
        SerializingProducer(conf)


@pytest.mark.parametrize('field', ['key', 'value'])
def test_consumer_rejects_deserializer_and_builder_together(cluster_id_calls, field):
    conf = _consumer_conf(
        **{
            '{}.deserializer'.format(field): StringDeserializer(),
            '{}.deserializer.builder'.format(field): _RecordingDeserializerBuilder(),
        }
    )
    with pytest.raises(ValueError, match='Cannot configure both'):
        DeserializingConsumer(conf)


def test_builder_returning_a_non_dict_conf_is_reported(cluster_id_calls):
    class BadBuilder(SerializerBuilder):
        def build(self, conf, is_key):
            return StringSerializer(), None

    with pytest.raises(ValueError, match='value.serializer.builder'):
        SerializingProducer(_producer_conf(**{'value.serializer.builder': BadBuilder()}))


def test_builder_returning_a_bare_serde_is_reported(cluster_id_calls):
    class BadBuilder(SerializerBuilder):
        def build(self, conf, is_key):
            return StringSerializer()

    with pytest.raises(ValueError, match='must return a'):
        SerializingProducer(_producer_conf(**{'value.serializer.builder': BadBuilder()}))


# --- cluster id resolution --------------------------------------------------
#
# The client never asks the broker for the cluster id itself; it hands the
# serdes a resolver which they invoke on their first subject lookup.


def test_cluster_id_never_fetched_during_construction(cluster_id_calls):
    SerializingProducer(
        _producer_conf(**{'key.serializer': _TrackingSerializer(), 'value.serializer': _TrackingSerializer()})
    )
    DeserializingConsumer(_consumer_conf(**{'value.deserializer': _TrackingDeserializer()}))

    assert cluster_id_calls == []


def test_resolver_handed_to_every_serializer_that_takes_one(cluster_id_calls):
    key_serializer = _TrackingSerializer()
    value_serializer = _TrackingSerializer()
    SerializingProducer(_producer_conf(**{'key.serializer': key_serializer, 'value.serializer': value_serializer}))

    assert key_serializer.resolver is not None
    assert value_serializer.resolver is not None
    # both resolve through the producer, waiting up to the metadata timeout
    assert key_serializer.resolver() == CLUSTER_ID
    assert value_serializer.resolver() == CLUSTER_ID
    assert cluster_id_calls == [60.0, 60.0]


def test_resolver_handed_to_deserializers(cluster_id_calls):
    deserializer = _TrackingDeserializer()
    DeserializingConsumer(_consumer_conf(**{'value.deserializer': deserializer}))

    assert deserializer.resolver() == CLUSTER_ID
    assert cluster_id_calls == [60.0]


def test_resolver_handed_to_built_serde(cluster_id_calls):
    serializer = _TrackingSerializer()
    SerializingProducer(_producer_conf(**{'value.serializer.builder': _RecordingBuilder(serializer)}))

    assert serializer.resolver() == CLUSTER_ID


def test_plain_callables_and_base_serdes_are_left_alone(cluster_id_calls):
    # a lambda has no set_cluster_id_resolver() at all and must not trip anything
    SerializingProducer(
        _producer_conf(**{'key.serializer': lambda obj, ctx: b'', 'value.serializer': StringSerializer()})
    )
    assert cluster_id_calls == []


def test_base_serdes_accept_a_resolver_and_close_as_no_ops():
    assert Serializer().set_cluster_id_resolver(lambda: CLUSTER_ID) is None
    assert Deserializer().set_cluster_id_resolver(lambda: CLUSTER_ID) is None
    assert Serializer().close() is None
    assert Deserializer().close() is None


# --- ownership --------------------------------------------------------------
#
# Serdes the client built are its own and closed with it; serdes handed in
# ready-made stay the application's.


def test_producer_closes_the_serdes_it_built(cluster_id_calls):
    key_serializer = _TrackingSerializer()
    value_serializer = _TrackingSerializer()
    producer = SerializingProducer(
        _producer_conf(
            **{
                'key.serializer.builder': _RecordingBuilder(key_serializer),
                'value.serializer.builder': _RecordingBuilder(value_serializer),
            }
        )
    )

    producer.close()

    assert key_serializer.closed == 1
    assert value_serializer.closed == 1


def test_producer_leaves_app_supplied_serdes_open(cluster_id_calls):
    supplied = _TrackingSerializer()
    built = _TrackingSerializer()
    producer = SerializingProducer(
        _producer_conf(**{'key.serializer': supplied, 'value.serializer.builder': _RecordingBuilder(built)})
    )

    producer.close()

    assert supplied.closed == 0
    assert built.closed == 1


def test_producer_close_is_idempotent(cluster_id_calls):
    serializer = _TrackingSerializer()
    producer = SerializingProducer(_producer_conf(**{'value.serializer.builder': _RecordingBuilder(serializer)}))

    producer.close()
    producer.close()

    assert serializer.closed == 1


def test_producer_context_manager_closes_built_serdes(cluster_id_calls):
    serializer = _TrackingSerializer()
    with SerializingProducer(_producer_conf(**{'value.serializer.builder': _RecordingBuilder(serializer)})):
        assert serializer.closed == 0

    assert serializer.closed == 1


def test_consumer_closes_the_serdes_it_built(cluster_id_calls):
    supplied = _TrackingDeserializer()
    built = _TrackingDeserializer()
    consumer = DeserializingConsumer(
        _consumer_conf(
            **{'key.deserializer': supplied, 'value.deserializer.builder': _RecordingDeserializerBuilder(built)}
        )
    )

    consumer.close()
    consumer.close()

    assert supplied.closed == 0
    assert built.closed == 1


def test_consumer_context_manager_closes_built_serdes(cluster_id_calls):
    deserializer = _TrackingDeserializer()
    with DeserializingConsumer(
        _consumer_conf(**{'value.deserializer.builder': _RecordingDeserializerBuilder(deserializer)})
    ):
        assert deserializer.closed == 0

    assert deserializer.closed == 1


def test_built_serdes_are_closed_when_the_client_fails_to_construct(cluster_id_calls):
    serializer = _TrackingSerializer()
    # an unknown property makes the underlying Producer refuse the configuration
    conf = _producer_conf(**{'not.a.property': 'x', 'value.serializer.builder': _RecordingBuilder(serializer)})

    with pytest.raises(KafkaException):
        SerializingProducer(conf)

    assert serializer.closed == 1


def test_built_serdes_are_closed_when_a_later_builder_fails(cluster_id_calls):
    key_serializer = _TrackingSerializer()
    conf = _producer_conf(
        **{'key.serializer.builder': _RecordingBuilder(key_serializer), 'value.serializer.builder': _FailingBuilder()}
    )

    with pytest.raises(RuntimeError, match='builder broke'):
        SerializingProducer(conf)

    assert key_serializer.closed == 1


def test_all_built_serdes_are_closed_even_if_one_fails_to(cluster_id_calls):
    class _BrokenClose(_TrackingSerializer):
        def close(self):
            super().close()
            raise RuntimeError("close broke")

    first = _BrokenClose()
    second = _TrackingSerializer()
    producer = SerializingProducer(
        _producer_conf(
            **{
                'key.serializer.builder': _RecordingBuilder(first),
                'value.serializer.builder': _RecordingBuilder(second),
            }
        )
    )

    with pytest.raises(RuntimeError, match='close broke'):
        producer.close()

    assert first.closed == 1
    assert second.closed == 1


# --- Schema Registry builders -----------------------------------------------

# The Schema Registry builders are optional extras, so skip rather than fail
# when they are not installed.
sr_avro = pytest.importorskip('confluent_kafka.schema_registry.avro')
sr_json = pytest.importorskip('confluent_kafka.schema_registry.json_schema')
sr_client = pytest.importorskip('confluent_kafka.schema_registry.schema_registry_client')

SR_CONF = {'url': 'mock://'}
AVRO_SCHEMA = (
    '{"type":"record","name":"User","fields":['
    '{"name":"name","type":"string"},{"name":"favorite_number","type":"long"}]}'
)


def _identity(obj, ctx):
    return obj


@pytest.fixture
def sr_client_closes(monkeypatch):
    """Record the Schema Registry clients that get closed."""
    closed = []

    def _close(self):
        closed.append(self)

    monkeypatch.setattr(sr_client.SchemaRegistryClient, 'close', _close)
    return closed


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
    # nothing to own, nothing to close
    deserializer.close()


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


def test_sr_serde_forwards_the_resolver_to_the_associated_strategy():
    # the default subject name strategy is the associated one, which resolves
    # subjects against the cluster id
    serializer, _ = sr_avro.AvroSerializerBuilder(schema_registry_config=SR_CONF, schema=AVRO_SCHEMA).build({}, False)

    def resolver():
        return CLUSTER_ID

    serializer.set_cluster_id_resolver(resolver)
    assert serializer._subject_name_func._cluster_id_resolver is resolver


def test_sr_serde_ignores_the_resolver_with_other_strategies():
    serializer, _ = sr_avro.AvroSerializerBuilder(
        schema_registry_config=SR_CONF,
        schema=AVRO_SCHEMA,
        serializer_config={'subject.name.strategy.type': 'TOPIC'},
    ).build({}, False)

    # nothing to hand it to; must not raise
    serializer.set_cluster_id_resolver(lambda: CLUSTER_ID)


def test_config_built_sr_client_is_owned_and_closed_with_the_serde(sr_client_closes):
    serializer, _ = sr_avro.AvroSerializerBuilder(schema_registry_config=SR_CONF, schema=AVRO_SCHEMA).build({}, False)
    registry = serializer._registry
    assert registry is not None

    serializer.close()
    serializer.close()

    assert sr_client_closes == [registry]


def test_app_supplied_sr_client_is_not_closed_with_the_serde(sr_client_closes):
    client = sr_client.SchemaRegistryClient.new_client(SR_CONF)
    serializer, _ = sr_avro.AvroSerializerBuilder(schema_registry_client=client, schema=AVRO_SCHEMA).build({}, False)
    assert serializer._registry is client

    serializer.close()

    assert sr_client_closes == []


def test_builder_rejects_a_client_and_a_config_together(sr_client_closes):
    # no precedence between the two: the ambiguity is an error, as in .NET
    client = sr_client.SchemaRegistryClient.new_client(SR_CONF)
    builder = sr_avro.AvroSerializerBuilder(
        schema_registry_client=client, schema_registry_config={'url': 'http://unused:8081'}, schema=AVRO_SCHEMA
    )

    with pytest.raises(ValueError, match='both a Schema Registry client and a configuration'):
        builder.build({}, False)

    # nothing was created, and the supplied client is untouched
    assert sr_client_closes == []


def test_deserializer_builder_rejects_a_client_and_a_config_together(sr_client_closes):
    client = sr_client.SchemaRegistryClient.new_client(SR_CONF)
    builder = sr_avro.AvroDeserializerBuilder().set_schema_registry_client(client).set_schema_registry_config(SR_CONF)

    with pytest.raises(ValueError, match='use one or the other'):
        builder.build({}, False)

    assert sr_client_closes == []


def test_builder_closes_the_sr_client_it_created_when_the_serde_fails(sr_client_closes):
    # an unknown serializer property is rejected by the serializer constructor,
    # after the builder has already created the client
    builder = sr_avro.AvroSerializerBuilder(
        schema_registry_config=SR_CONF, schema=AVRO_SCHEMA, serializer_config={'not.a.property': True}
    )

    with pytest.raises(ValueError):
        builder.build({}, False)

    assert len(sr_client_closes) == 1


def test_builder_closes_the_serde_it_built_when_the_init_callback_fails(sr_client_closes):
    def broken_init(serializer):
        raise RuntimeError("init broke")

    builder = sr_avro.AvroSerializerBuilder(
        schema_registry_config=SR_CONF, schema=AVRO_SCHEMA, serializer_init=broken_init
    )

    with pytest.raises(RuntimeError, match='init broke'):
        builder.build({}, False)

    # the serializer already owned the client the builder created for it
    assert len(sr_client_closes) == 1


def test_deserializer_builder_closes_the_serde_it_built_when_the_init_callback_fails(sr_client_closes):
    def broken_init(deserializer):
        raise RuntimeError("init broke")

    builder = sr_avro.AvroDeserializerBuilder(schema_registry_config=SR_CONF, deserializer_init=broken_init)

    with pytest.raises(RuntimeError, match='init broke'):
        builder.build({}, False)

    assert len(sr_client_closes) == 1


def test_builder_does_not_close_a_supplied_sr_client_when_the_serde_fails(sr_client_closes):
    client = sr_client.SchemaRegistryClient.new_client(SR_CONF)
    builder = sr_avro.AvroSerializerBuilder(
        schema_registry_client=client, schema=AVRO_SCHEMA, serializer_config={'not.a.property': True}
    )

    with pytest.raises(ValueError):
        builder.build({}, False)

    assert sr_client_closes == []


def test_producer_closes_the_sr_client_of_the_serde_it_built(cluster_id_calls, sr_client_closes):
    producer = SerializingProducer(
        _producer_conf(
            **{
                'value.serializer.builder': sr_avro.AvroSerializerBuilder(
                    schema_registry_config=SR_CONF, schema=AVRO_SCHEMA
                )
            }
        )
    )
    registry = producer._value_serializer._registry

    producer.close()

    assert sr_client_closes == [registry]


# --- deserialized accessors -------------------------------------------------


def _make_message(value=None, key=None, topic='t'):
    return Message(topic, 0, 0, key, value, None, None, (0, 0), -1.0, -1)


def test_deserialized_accessors_return_the_deserialized_objects(cluster_id_calls):
    consumer = DeserializingConsumer(
        _consumer_conf(**{'key.deserializer': StringDeserializer(), 'value.deserializer': StringDeserializer()})
    )
    msg = consumer._deserialize(_make_message(value=b'v', key=b'k'))

    assert msg.deserialized_key() == 'k'
    assert msg.deserialized_value() == 'v'


def test_deserialized_accessors_alias_key_and_value(cluster_id_calls):
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


# --- produce fails fast, before the serializers run ---------------------------


def _recording_serializer(calls):
    def serializer(obj, ctx):
        calls.append(ctx.topic)
        return obj.encode('utf_8')

    return serializer


def test_produce_after_close_fails_before_serializing(cluster_id_calls):
    calls = []
    producer = SerializingProducer(_producer_conf(**{'value.serializer': _recording_serializer(calls)}))
    producer.close()

    with pytest.raises(RuntimeError, match='closed'):
        producer.produce('t', value='x')
    assert calls == []


def test_delivery_callback_may_produce_again_during_close(cluster_id_calls):
    # The flush inside Producer.close() dispatches delivery reports; a report
    # producing again on the closing thread must not hit the closed check.
    # No broker is reachable, so the reports carry a timeout error.
    calls = []
    producer = SerializingProducer(
        _producer_conf(**{'message.timeout.ms': 100, 'value.serializer': _recording_serializer(calls)})
    )
    reports = []

    def on_second_delivery(err, msg):
        reports.append(('second', err is not None))

    def on_first_delivery(err, msg):
        reports.append(('first', err is not None))
        producer.produce('t', value='again', on_delivery=on_second_delivery)

    producer.produce('t', value='x', on_delivery=on_first_delivery)

    assert producer.close() is True

    # both messages were serialized and reported; only the late one is refused
    assert calls == ['t', 't']
    assert reports == [('first', True), ('second', True)]
    with pytest.raises(RuntimeError, match='closed'):
        producer.produce('t', value='late')
    assert calls == ['t', 't']


def test_produce_rejects_a_non_str_topic_before_serializing(cluster_id_calls):
    calls = []
    producer = SerializingProducer(_producer_conf(**{'value.serializer': _recording_serializer(calls)}))
    try:
        with pytest.raises(TypeError, match='topic must be a str, not NoneType'):
            producer.produce(None, value='x')
        assert calls == []
    finally:
        producer.close()
