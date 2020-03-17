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

import random

import pytest

from confluent_kafka.schema_registry.config import RegistrySerializerConfig
from confluent_kafka.serialization import AvroDeserializer, AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext


@pytest.mark.parametrize("avsc, data",
                         [('basic_schema.avsc', {'name': 'abc'}),
                          ('primitive_string.avsc', u'Jämtland'),
                          ('primitive_bool.avsc', True),
                          ('primitive_float.avsc', random.uniform(-32768.0, 32768.0)),
                          ('primitive_double.avsc', random.uniform(-32768.0, 32768.0))])
def test_avro_record_serialization(kafka_cluster, load_avsc, avsc, data):
    """
    Tests basic Avro serializer functionality

    Args:
        kafka_cluster (KafkaClusterFixture): cluster fixture
        load_avsc (callable(str)): Avro file reader
        avsc (str) avsc: Avro schema file
        data (object): data to be serialized

    Raises:
        AssertionError on test failure

    """
    topic = kafka_cluster.create_topic("serialization-avro")
    schema = load_avsc(avsc)
    sr_conf = kafka_cluster.schema_registry()

    value_serializer = AvroSerializer(RegistrySerializerConfig(sr_conf),
                                      schema=load_avsc(avsc))

    value_deserializer = AvroDeserializer(RegistrySerializerConfig(sr_conf),
                                          schema=load_avsc(avsc))

    producer = kafka_cluster.producer(value_serializer=value_serializer)

    producer.produce(topic, data)
    producer.flush()

    consumer = kafka_cluster.consumer(value_deserializer=value_deserializer)
    consumer.subscribe([topic])
    msg = consumer.poll()

    actual = msg.value()
    # schema may include default which need not exist in the original
    schema_type = schema.schema['type']
    if schema_type == "record":
        assert [v == actual[k] for k, v in data.items()]
    elif schema_type == 'float':
        assert data == pytest.approx(actual)
    else:
        assert actual == data


@pytest.mark.parametrize("avsc, data",
                         [('basic_schema.avsc', {'name': 'abc'}),
                          ('primitive_string.avsc', u'Jämtland'),
                          ('primitive_bool.avsc', True),
                          ('primitive_float.avsc', random.uniform(-32768.0, 32768.0)),
                          ('primitive_double.avsc', random.uniform(-32768.0, 32768.0))])
def test_delivery_report_serialization(kafka_cluster, load_avsc, avsc, data):
    """
    Tests basic Avro serializer functionality

    Args:
        kafka_cluster (KafkaClusterFixture): cluster fixture
        load_avsc (callable(str)): Avro file reader
        avsc (str) avsc: Avro schema file
        data (object): data to be serialized

    Raises:
        AssertionError on test failure

    """
    topic = kafka_cluster.create_topic("serialization-avro")
    schema = load_avsc(avsc)
    sr_conf = kafka_cluster.schema_registry()
    serializer_conf = RegistrySerializerConfig(sr_conf)

    value_serializer = AvroSerializer(serializer_conf,
                                      schema=load_avsc(avsc))

    value_deserializer = AvroDeserializer(serializer_conf,
                                          schema=load_avsc(avsc))

    producer = kafka_cluster.producer(value_serializer=value_serializer)

    def assert_cb(err, msg):
        actual = value_deserializer(msg.value(),
                                    SerializationContext(
                                        topic, MessageField.VALUE))

        assert type(actual) == type(data)

        schema_type = schema.schema['type']
        if schema_type == "record":
            assert [v == actual[k] for k, v in data.items()]
        elif schema_type == 'float':
            assert data == pytest.approx(actual)
        else:
            assert actual == data

    producer.produce(topic, data, on_delivery=assert_cb)
    producer.flush()

    consumer = kafka_cluster.consumer(value_deserializer=value_deserializer)
    consumer.subscribe([topic])
    msg = consumer.poll()

    actual = msg.value()
    # schema may include default which need not exist in the original
    schema_type = schema.schema['type']
    if schema_type == "record":
        assert [v == actual[k] for k, v in data.items()]
    elif schema_type == 'float':
        assert data == pytest.approx(actual)
    else:
        assert actual == data
