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

from confluent_kafka import \
    DoubleSerde, IntegerSerde, LongSerde, \
    ShortSerde, StringSerde


@pytest.mark.parametrize("serializer, data",
                         [(DoubleSerde(), random.uniform(-32768.0, 32768.0)),
                          (LongSerde(), random.getrandbits(63)),
                          (IntegerSerde(), random.randint(-32768, 32768)),
                          (ShortSerde(), random.randint(-32768, 32768)),
                          (DoubleSerde(), None),
                          (LongSerde(), None),
                          (IntegerSerde(), None),
                          (ShortSerde(), None)])
def test_numeric_serialization(kafka_cluster, serializer, data):
    """
    Tests basic serialization/deserialization of numeric types.

    Args:
        kafka_cluster (KafkaClusterFixture): cluster fixture
        serializer (Serde): serializer to test
        data(object(: input data

    Raises:
        AssertionError on test failure

    """
    topic = kafka_cluster.create_topic("serialization-numeric")

    producer = kafka_cluster.producer(value_serializer=serializer)
    producer.produce(topic, data)
    producer.flush()

    consumer = kafka_cluster.consumer(value_deserializer=serializer)
    consumer.subscribe([topic])

    msg = consumer.poll()

    assert msg.value() == data

    consumer.close()


@pytest.mark.parametrize("data, codec",
                         [(u'Jämtland', 'utf_8'),
                          (u'Härjedalen', 'utf_16'),
                          (None, 'utf_32')])
def test_string_serialization(kafka_cluster, data, codec):
    """
    Tests basic unicode serialization/deserialization functionality

    Args:
        kafka_cluster (KafkaClusterFixture): cluster fixture
        data (unicode): input data
        codec (str): encoding type

    Raises:
        AssertionError on test failure

    """
    topic = kafka_cluster.create_topic("serialization-string")

    producer = kafka_cluster.producer(value_serializer=StringSerde(codec))

    producer.produce(topic, value=data)
    producer.flush()

    consumer = kafka_cluster.consumer(value_deserializer=StringSerde(codec))

    consumer.subscribe([topic])

    msg = consumer.poll()

    assert msg.value() == data

    consumer.close()


@pytest.mark.parametrize("key_serializer, value_serializer, key, value",
                         [(DoubleSerde(), StringSerde('utf_8'),
                           random.uniform(-32768.0, 32768.0), u'Jämtland'),
                          (StringSerde('utf_32'), LongSerde(),
                           u'Härjedalen', random.getrandbits(63)),
                          (IntegerSerde(), ShortSerde(),
                           random.randint(-32768, 32768), random.randint(-32768, 32768))])
def test_mixed_serialization(kafka_cluster, key_serializer, value_serializer, key, value):
    """pyte
    Tests basic mixed serializer/deserializer functionality.

    Args:
        kafka_cluster (KafkaClusterFixture): cluster fixture
        key_serializer (Serde): key serializer
        value_serializer (Serde): value serializer
        key (object): key data
        value (object): value data

    Raises:
        AssertionError on test failure

    """
    topic = kafka_cluster.create_topic("serialization-numeric")

    producer = kafka_cluster.producer(key_serializer=key_serializer,
                                      value_serializer=value_serializer)
    producer.produce(topic, key=key, value=value)
    producer.flush()

    consumer = kafka_cluster.consumer(key_deserializer=key_serializer,
                                      value_deserializer=value_serializer)
    consumer.subscribe([topic])

    msg = consumer.poll()

    assert msg.key() == key
    assert msg.value() == value

    consumer.close()
