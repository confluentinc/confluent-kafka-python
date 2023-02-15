#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright 2020 Confluent Inc.
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
import json
import fastavro

import pytest

from confluent_kafka import TopicPartition
from confluent_kafka.serialization import (MessageField,
                                           SerializationContext)
from confluent_kafka.schema_registry.avro import (AvroSerializer,
                                                  AvroDeserializer)

from confluent_kafka.schema_registry import Schema, SchemaReference


class User(object):
    schema_str = """
        {
            "namespace": "confluent.io.examples.serialization.avro",
            "name": "User",
            "type": "record",
            "fields": [
                {"name": "name", "type": "string"},
                {"name": "favorite_number", "type": "int"},
                {"name": "favorite_color", "type": "string"}
            ]
        }
        """

    def __init__(self, name, favorite_number, favorite_color):
        self.name = name
        self.favorite_number = favorite_number
        self.favorite_color = favorite_color

    def __eq__(self, other):
        return all([
            self.name == other.name,
            self.favorite_number == other.favorite_number,
            self.favorite_color == other.favorite_color])


class AwardedUser(object):
    schema_str = """
        {
            "namespace": "confluent.io.examples.serialization.avro",
            "name": "AwardedUser",
            "type": "record",
            "fields": [
                {"name": "award", "type": "string"},
                {"name": "user", "type": "User"}
            ]
        }
    """

    def __init__(self, award, user):
        self.award = award
        self.user = user

    def __eq__(self, other):
        return all([
            self.award == other.award,
            self.user == other.user
        ])


@pytest.mark.parametrize("avsc, data, record_type",
                         [('basic_schema.avsc', {'name': 'abc'}, "record"),
                          ('primitive_string.avsc', u'Jämtland', "string"),
                          ('primitive_bool.avsc', True, "bool"),
                          ('primitive_float.avsc', 32768.2342, "float"),
                          ('primitive_double.avsc', 68.032768, "float")])
def test_avro_record_serialization(kafka_cluster, load_file, avsc, data, record_type):
    """
    Tests basic Avro serializer functionality

    Args:
        kafka_cluster (KafkaClusterFixture): cluster fixture
        load_file (callable(str)): Avro file reader
        avsc (str) avsc: Avro schema file
        data (object): data to be serialized

    """
    topic = kafka_cluster.create_topic("serialization-avro")
    sr = kafka_cluster.schema_registry()

    schema_str = load_file(avsc)
    value_serializer = AvroSerializer(sr, Schema(schema_str, 'AVRO'))

    value_deserializer = AvroDeserializer(sr)

    producer = kafka_cluster.producer(value_serializer=value_serializer)

    producer.produce(topic, value=data, partition=0)
    producer.flush()

    consumer = kafka_cluster.consumer(value_deserializer=value_deserializer)
    consumer.assign([TopicPartition(topic, 0)])

    msg = consumer.poll()
    actual = msg.value()

    if record_type == 'record':
        assert [v == actual[k] for k, v in data.items()]
    elif record_type == 'float':
        assert data == pytest.approx(actual)
    else:
        assert actual == data


@pytest.mark.parametrize("avsc, data,record_type",
                         [('basic_schema.avsc', dict(name='abc'), 'record'),
                          ('primitive_string.avsc', u'Jämtland', 'string'),
                          ('primitive_bool.avsc', True, 'bool'),
                          ('primitive_float.avsc', 768.2340, 'float'),
                          ('primitive_double.avsc', 6.868, 'float')])
def test_delivery_report_serialization(kafka_cluster, load_file, avsc, data, record_type):
    """
    Tests basic Avro serializer functionality

    Args:
        kafka_cluster (KafkaClusterFixture): cluster fixture
        load_file (callable(str)): Avro file reader
        avsc (str) avsc: Avro schema file
        data (object): data to be serialized

    """
    topic = kafka_cluster.create_topic("serialization-avro-dr")
    sr = kafka_cluster.schema_registry()
    schema_str = load_file(avsc)

    value_serializer = AvroSerializer(sr, Schema(schema_str, 'AVRO'))

    value_deserializer = AvroDeserializer(sr)

    producer = kafka_cluster.producer(value_serializer=value_serializer)

    def assert_cb(err, msg):
        actual = value_deserializer(msg.value(),
                                    SerializationContext(topic, MessageField.VALUE, msg.headers()))

        if record_type == "record":
            assert [v == actual[k] for k, v in data.items()]
        elif record_type == 'float':
            assert data == pytest.approx(actual)
        else:
            assert actual == data

    producer.produce(topic, value=data, partition=0, on_delivery=assert_cb)
    producer.flush()

    consumer = kafka_cluster.consumer(value_deserializer=value_deserializer)
    consumer.assign([TopicPartition(topic, 0)])

    msg = consumer.poll()
    actual = msg.value()

    # schema may include default which need not exist in the original
    if record_type == 'record':
        assert [v == actual[k] for k, v in data.items()]
    elif record_type == 'float':
        assert data == pytest.approx(actual)
    else:
        assert actual == data


def test_avro_record_serialization_custom(kafka_cluster):
    """
    Tests basic Avro serializer to_dict and from_dict object hook functionality.

    Args:
        kafka_cluster (KafkaClusterFixture): cluster fixture

    """
    topic = kafka_cluster.create_topic("serialization-avro")
    sr = kafka_cluster.schema_registry()

    user = User('Bowie', 47, 'purple')
    value_serializer = AvroSerializer(sr, Schema(User.schema_str, 'AVRO'),
                                      lambda user, ctx:
                                      dict(name=user.name,
                                           favorite_number=user.favorite_number,
                                           favorite_color=user.favorite_color))

    value_deserializer = AvroDeserializer(sr, Schema(User.schema_str, 'AVRO'),
                                          lambda user_dict, ctx:
                                          User(**user_dict))

    producer = kafka_cluster.producer(value_serializer=value_serializer)

    producer.produce(topic, value=user, partition=0)
    producer.flush()

    consumer = kafka_cluster.consumer(value_deserializer=value_deserializer)
    consumer.assign([TopicPartition(topic, 0)])

    msg = consumer.poll()
    user2 = msg.value()

    assert user2 == user


def _get_reference_data():
    user = User('Bowie', 47, 'purple')
    awarded_user = AwardedUser("Best In Show", user)

    ref_dict = json.loads(User.schema_str)
    named_schemas = {}
    fastavro.parse_schema(ref_dict, named_schemas=named_schemas)

    schema_ref = SchemaReference("confluent.io.examples.serialization.avro.User", "user", 1)
    references = [schema_ref]

    schema = Schema(AwardedUser.schema_str, 'AVRO', references, named_schemas)

    return awarded_user, schema


def _reference_common(kafka_cluster, awarded_user, serializer_schema, deserializer_schema):
    """
    Common (both reader and writer) avro schema reference test.

    Args:
        kafka_cluster (KafkaClusterFixture): cluster fixture

    """
    topic = kafka_cluster.create_topic("reference-avro")
    sr = kafka_cluster.schema_registry()

    sr.register_schema("user", Schema(User.schema_str, 'AVRO'))

    value_serializer = AvroSerializer(sr, serializer_schema,
                                      lambda awarded_user, ctx:
                                      dict(award=awarded_user.award,
                                           user=dict(name=awarded_user.user.name,
                                                     favorite_number=awarded_user.user.favorite_number,
                                                     favorite_color=awarded_user.user.favorite_color)))

    value_deserializer = AvroDeserializer(sr, deserializer_schema,
                                          lambda awarded_user_dict, ctx:
                                          AwardedUser(awarded_user_dict.get('award'),
                                                      User(awarded_user_dict.get('user').get('name'),
                                                           awarded_user_dict.get('user').get('favorite_number'),
                                                           awarded_user_dict.get('user').get('favorite_color'))))

    producer = kafka_cluster.producer(value_serializer=value_serializer)

    producer.produce(topic, value=awarded_user, partition=0)
    producer.flush()

    consumer = kafka_cluster.consumer(value_deserializer=value_deserializer)
    consumer.assign([TopicPartition(topic, 0)])

    msg = consumer.poll()
    awarded_user2 = msg.value()

    assert awarded_user2 == awarded_user


def test_avro_reader_reference(kafka_cluster):
    """
    Tests Avro schema reference relying on reader schema.

    Args:
        kafka_cluster (KafkaClusterFixture): cluster fixture

    """
    awarded_user, schema = _get_reference_data()

    _reference_common(kafka_cluster, awarded_user, schema, schema)


def test_avro_writer_reference(kafka_cluster):
    """
    Tests Avro schema reference relying on writer schema.

    Args:
        kafka_cluster (KafkaClusterFixture): cluster fixture

    """
    awarded_user, schema = _get_reference_data()

    _reference_common(kafka_cluster, awarded_user, schema, None)
