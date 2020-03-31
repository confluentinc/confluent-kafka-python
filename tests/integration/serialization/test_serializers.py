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

from confluent_kafka.serialization import (DoubleSerializer,
                                           IntegerSerializer,
                                           StringDeserializer,
                                           DoubleDeserializer,
                                           IntegerDeserializer,
                                           StringSerializer)


@pytest.mark.parametrize("serializer, deserializer, data",
                         [(DoubleSerializer(), DoubleDeserializer(), 6.21682154508147),
                          (IntegerSerializer(), IntegerDeserializer(), 4124),
                          (DoubleSerializer(), DoubleDeserializer(), None),
                          (IntegerSerializer(), IntegerDeserializer(), None)])
def test_numeric_serialization(kafka_cluster, serializer, deserializer, data):
    """
    Tests basic serialization/deserialization of numeric types.

    Args:
        kafka_cluster (KafkaClusterFixture): cluster fixture

        serializer (Serializer): serializer to test

        deserializer (Deserializer): deserializer to validate serializer

        data(object): input data

    """
    topic = kafka_cluster.create_topic("serialization-numeric")

    producer = kafka_cluster.producer(value_serializer=serializer)
    producer.produce(topic, value=data)
    producer.flush()

    consumer = kafka_cluster.consumer(value_deserializer=deserializer)
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

    """
    topic = kafka_cluster.create_topic("serialization-string")

    producer = kafka_cluster.producer(value_serializer=StringSerializer(codec))

    producer.produce(topic, value=data)
    producer.flush()

    consumer = kafka_cluster.consumer(value_deserializer=StringDeserializer(codec))

    consumer.subscribe([topic])

    msg = consumer.poll()

    assert msg.value() == data

    consumer.close()


@pytest.mark.parametrize("key_serializer, value_serializer, key_deserializer, value_deserializer, key, value",  # noqa: E501
                         [(DoubleSerializer(), StringSerializer('utf_8'),
                           DoubleDeserializer(), StringDeserializer(),
                           -31.2168215450814477, u'Jämtland'),
                          (StringSerializer('utf_16'), DoubleSerializer(),
                           StringDeserializer('utf_16'), DoubleDeserializer(),
                           u'Härjedalen', 1.2168215450814477)])
def test_mixed_serialization(kafka_cluster, key_serializer, value_serializer,
                             key_deserializer, value_deserializer, key, value):
    """
    Tests basic mixed serializer/deserializer functionality.

    Args:
        kafka_cluster (KafkaClusterFixture): cluster fixture

        key_serializer (Serializer): serializer to test

        key_deserializer (Deserializer): deserializer to validate serializer

        key (object): key data

        value (object): value data

    """
    topic = kafka_cluster.create_topic("serialization-numeric")

    producer = kafka_cluster.producer(key_serializer=key_serializer,
                                      value_serializer=value_serializer)
    producer.produce(topic, key=key, value=value)
    producer.flush()

    consumer = kafka_cluster.consumer(key_deserializer=key_deserializer,
                                      value_deserializer=value_deserializer)
    consumer.subscribe([topic])

    msg = consumer.poll()

    assert msg.key() == key
    assert msg.value() == value

    consumer.close()
