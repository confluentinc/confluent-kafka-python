#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
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
from sys import version_info
from uuid import uuid1

import pytest

from confluent_kafka import KafkaError
from confluent_kafka.serialization import double_deserializer, int_deserializer, long_deserializer, \
    short_deserializer, string_deserializer, double_serializer, long_serializer, int_serializer, short_serializer, \
    float_serializer, string_serializer
from confluent_kafka.serialization import float_deserializer


@pytest.mark.parametrize("serializer, deserializer, data",
                         [(double_serializer, "DoubleDeserializer", random.uniform(-32768.0, 32768.0)),
                          (long_serializer, "LongDeserializer", random.getrandbits(63)),
                          (int_serializer, "IntegerDeserializer", random.randint(-32768, 32768)),
                          (short_serializer, "ShortDeserializer", random.randint(-32768, 32768)), ])
def test_numeric_serialization(cluster_fixture, serializer, deserializer, data):
    topic = cluster_fixture.create_topic("serialization-numeric{}".format(str(uuid1())),
                                         {'num_partitions': 1})

    producer = cluster_fixture.producer()
    producer.produce(topic, value=serializer(data))
    producer.flush()

    consumer = cluster_fixture.java_consumer({
        'value.deserializer': "org.apache.kafka.common.serialization.{}".format(deserializer)
    })
    consumer.subscribe([topic])

    msg = consumer.poll(10.0)

    consumer.close()

    if deserializer == "DoubleDeserializer":
        assert float(msg.value()) == data
    else:
        assert int(msg.value()) == data


@pytest.mark.parametrize("serializer, deserializer, data",
                         [("DoubleSerializer", double_deserializer, random.uniform(-32768.0, 32768.0)),
                          ("LongSerializer", long_deserializer, random.getrandbits(63)),
                          ("IntegerSerializer", int_deserializer, random.randint(-32768, 32768)),
                          ("ShortSerializer", short_deserializer, random.randint(-32768, 32768)), ])
def test_numeric_deserialization(cluster_fixture, serializer, deserializer, data):
    topic = cluster_fixture.create_topic("deserialization-numeric{}".format(str(uuid1())),
                                         {'num_partitions': 1})

    producer = cluster_fixture.java_producer(
        {'value.serializer': "org.apache.kafka.common.serialization.{}".format(serializer)}
    )

    producer.produce(topic, repr(data))

    consumer = cluster_fixture.consumer()
    consumer.subscribe([topic])

    while True:
        msg = consumer.poll()

        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                break
            continue
        try:
            assert deserializer(msg.value()) == data
        except AssertionError as e:
            consumer.close()
            raise e

    consumer.close()


def test_float_serialization(cluster_fixture):
    topic = cluster_fixture.create_topic("serialization-float-{}".format(str(uuid1())),
                                         {'num_partitions': 1})
    producer = cluster_fixture.producer()

    data = random.uniform(-1.0, 1.0)
    producer.produce(topic, float_serializer(data))
    producer.flush()

    consumer = cluster_fixture.java_consumer({
        'value.deserializer': "org.apache.kafka.common.serialization.FloatDeserializer"
    })
    consumer.subscribe([topic])

    msg = consumer.poll(6.0)

    try:
        assert pytest.approx(float(msg.value()) - data, 0)
    except AssertionError as e:
        consumer.close()
        raise e

    consumer.close()


def test_float_deserialization(cluster_fixture):
    topic = cluster_fixture.create_topic("serialization-float-{}".format(str(uuid1())),
                                         {'num_partitions': 1})
    producer = cluster_fixture.java_producer(
        {'value.serializer': "org.apache.kafka.common.serialization.FloatSerializer"}
    )

    data = random.uniform(-1.0, 1.0)
    producer.produce(topic, data)

    consumer = cluster_fixture.consumer()
    consumer.subscribe([topic])

    while True:
        msg = consumer.poll()

        if msg is None:
            continue

        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                break
            continue

        try:
            assert pytest.approx(float_deserializer(msg.value()) - data, 0)
        except AssertionError as e:
            consumer.close()
            raise e

    consumer.close()


@pytest.mark.parametrize("data",
                         ["Jämtland",
                          "Härjedalen"])
def test_string_serialization(cluster_fixture, data):
    topic = cluster_fixture.create_topic("serialization-string-{}".format(str(uuid1())),
                                         {'num_partitions': 1})

    producer = cluster_fixture.producer()

    producer.produce(topic, value=string_serializer(data))
    producer.flush()

    consumer = cluster_fixture.java_consumer({
        'value.deserializer': "org.apache.kafka.common.serialization.StringDeserializer",
    })

    consumer.subscribe([topic])

    msg = consumer.poll(10.0)

    try:
        assert msg.value() == data
    except AssertionError as e:
        consumer.close()
        raise e

    consumer.close()


@pytest.mark.parametrize("data",
                         ["Jämtland",
                          "Härjedalen", ])
def test_string_deserialization(cluster_fixture, data):
    topic = cluster_fixture.create_topic("serialization-string-{}".format(str(uuid1())),
                                         {'num_partitions': 1})

    producer = cluster_fixture.java_producer(
        {'value.serializer': "org.apache.kafka.common.serialization.StringSerializer"}
    )

    producer.produce(topic, data)
    producer.flush()

    consumer = cluster_fixture.consumer()
    consumer.subscribe([topic])

    while True:
        msg = consumer.poll()

        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                break
            continue
        try:
            assert string_deserializer(msg.value()) == data if version_info >= (3, 4) else data.decode('utf-8')
        except AssertionError as e:
            consumer.close()
            raise e

    consumer.close()
