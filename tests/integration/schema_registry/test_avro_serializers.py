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
from confluent_kafka.schema_registry import MessageField, SerializationContext
from confluent_kafka.schema_registry.avro import AvroSerializer, AvroDeserializer


@pytest.mark.parametrize("avsc, data, record_type",
                         [('basic_schema.avsc', {'name': 'abc'}, "record"),
                          ('primitive_string.avsc', u'Jämtland', "string"),
                          ('primitive_bool.avsc', True, "bool"),
                          ('primitive_float.avsc', 32768.2342, "float"),
                          ('primitive_double.avsc', 68.032768, "float")])
def test_avro_record_serialization(kafka_cluster, load_avsc, avsc, data, record_type):
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
    client_conf = kafka_cluster.client_conf()
    topic = kafka_cluster.create_topic("serialization-avro")
    sr = kafka_cluster.schema_registry()

    schema = load_avsc(avsc)
    value_serializer = AvroSerializer(client_conf, sr,
                                      schema=schema)

    value_deserializer = AvroDeserializer(client_conf, sr,
                                          schema=schema)

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
                         [('basic_schema.avsc', {'name': 'abc'}, 'record'),
                          ('primitive_string.avsc', u'Jämtland', 'string'),
                          ('primitive_bool.avsc', True, 'bool'),
                          ('primitive_float.avsc', 768.2340, 'float'),
                          ('primitive_double.avsc', 6.868, 'float')])
def test_delivery_report_serialization(kafka_cluster, load_avsc, avsc, data, record_type):
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
    client_conf = kafka_cluster.client_conf()
    topic = kafka_cluster.create_topic("serialization-avro-dr")
    sr = kafka_cluster.schema_registry({'url': 'http://localhost:8081'})

    value_serializer = AvroSerializer(client_conf, sr,
                                      schema=load_avsc(avsc))

    value_deserializer = AvroDeserializer(client_conf, sr,
                                          schema=load_avsc(avsc))

    producer = kafka_cluster.producer(value_serializer=value_serializer)

    def assert_cb(err, msg):
        actual = value_deserializer(msg.value(),
                                    SerializationContext(
                                        topic, MessageField.VALUE))

        assert type(actual) == type(data)

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
