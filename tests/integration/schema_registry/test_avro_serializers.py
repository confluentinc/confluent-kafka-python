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

import pytest

from confluent_kafka import TopicPartition
from confluent_kafka.serialization import (MessageField,
                                           SerializationContext)
from confluent_kafka.schema_registry.avro import (AvroSerializer,
                                                  AvroDeserializer)


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
    value_serializer = AvroSerializer(schema_str, sr)

    value_deserializer = AvroDeserializer(schema_str, sr)

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

    value_serializer = AvroSerializer(schema_str, sr)

    value_deserializer = AvroDeserializer(schema_str, sr)

    producer = kafka_cluster.producer(value_serializer=value_serializer)

    def assert_cb(err, msg):
        actual = value_deserializer(msg.value(),
                                    SerializationContext(topic, MessageField.VALUE))

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
    value_serializer = AvroSerializer(User.schema_str, sr,
                                      lambda user, ctx:
                                      dict(name=user.name,
                                           favorite_number=user.favorite_number,
                                           favorite_color=user.favorite_color))

    value_deserializer = AvroDeserializer(User.schema_str, sr,
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
