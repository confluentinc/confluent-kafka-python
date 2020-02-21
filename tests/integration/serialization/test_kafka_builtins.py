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
from copy import deepcopy
from uuid import uuid1

import pytest
from trivup.clusters.KafkaCluster import KafkaCluster

from confluent_kafka.cimpl import NewTopic
from confluent_kafka import Producer, Consumer
from confluent_kafka.admin import AdminClient
from confluent_kafka.serialization import DoubleSerializer, IntegerSerializer, LongSerializer, \
    ShortSerializer, StringSerializer, FloatSerializer

cluster = KafkaCluster(**{'with_sr': True})
cluster.wait_operational()


def create_topic(topic_prefix):
    """
    Creates a topic.

    :param str topic_prefix: topic name prefix

    :returns: Topic name
    :rtype: str
    """
    admin = AdminClient(cluster.client_conf())

    topic_name = "{}-{}".format(topic_prefix, str(uuid1()))
    future_topic = admin.create_topics(
        [NewTopic(topic_name, num_partitions=1, replication_factor=1)])

    future_topic.get(topic_name).result()

    return topic_name


def get_producer(value_serializer=None, key_serializer=None):
    """
    Creates a new producer

    :param Serializer value_serializer: serializer instance
    :param Serializer key_serializer: serializer instance

    :returns: Producer instance
    :rtype: Producer
    """
    return Producer(cluster.client_conf(),
                    value_serializer=value_serializer, key_serializer=key_serializer)


def get_consumer(group_prefix, value_serializer=None, key_serializer=None):
    """
    Creates a new Consumer

    :param str group_prefix: group id prefix
    :param Serializer value_serializer:
    :param Serializer key_serializer:

    :returns: Consumer instance
    """
    conf = deepcopy(cluster.client_conf())
    conf.update({'group.id': "{}-{}".format(group_prefix, str(uuid1())),
                 'auto.offset.reset': 'earliest'})
    return Consumer(conf,
                    value_serializer=value_serializer, key_serializer=key_serializer)


@pytest.mark.parametrize("serializer, data",
                         [(DoubleSerializer, random.uniform(-32768.0, 32768.0)),
                          (LongSerializer, random.getrandbits(63)),
                          (IntegerSerializer, random.randint(-32768, 32768)),
                          (ShortSerializer, random.randint(-32768, 32768)),
                          (DoubleSerializer, None),
                          (LongSerializer, None),
                          (IntegerSerializer, None),
                          (ShortSerializer, None)])
def test_numeric_serialization(serializer, data):
    """
    Tests basic serialization/deserialization of numeric types.

    :param Serializer serializer: serializer to test
    :param object data: input data

    :raises: AssertionError on test failure

    :returns: None
    :rtype: None
    """
    topic = create_topic("serialization-numeric")

    producer = get_producer(value_serializer=serializer)
    producer.produce(topic, data)
    producer.flush()

    consumer = get_consumer("serialization-numeric", value_serializer=serializer)
    consumer.subscribe([topic])

    msg = consumer.poll()

    assert msg.value() == data

    consumer.close()


@pytest.mark.parametrize("data",
                         [random.uniform(-1.0, 1.0),
                          None])
def test_float_serialization(data):
    """
    Tests basic float serialization/deserialization functionality.

    This test must be separated from the standard numeric types due
    to the nature of floats in Python. Some precision loss occurs
    when converting python's double-precision float to the expected
    single-precision float value. As such we only test that the
    single-precision approximated values is reasonably close to the original.

    :raises: AssertionError on test failure

    :returns: None
    :rtype: None
    """
    topic = create_topic("serialization-float")

    producer = get_producer(key_serializer=FloatSerializer)

    data = random.uniform(-1.0, 1.0)
    producer.produce(topic, key=data)
    producer.flush()

    consumer = get_consumer("serialization-float", key_serializer=FloatSerializer)
    consumer.subscribe([topic])

    msg = consumer.poll()

    assert pytest.approx(msg.key() - data, 0)

    consumer.close()


@pytest.mark.parametrize("data, codec",
                         [(u'Jämtland', 'utf_8'),
                          (u'Härjedalen', 'utf_16'),
                          (None, 'utf_32')])
def test_string_serialization(data, codec):
    """
    Tests basic unicode serialization/deserialization functionality

    :param unicode data: input data

    :raises: AssertionError on test failure

    :returns: None
    :rtype: None
    """
    topic = create_topic("serialization-string")

    producer = get_producer(value_serializer=StringSerializer(codec))

    producer.produce(topic, value=data)
    producer.flush()

    consumer = get_consumer("serialization-string",
                            value_serializer=StringSerializer(codec))

    consumer.subscribe([topic])

    msg = consumer.poll()

    assert msg.value() == data

    consumer.close()


@pytest.mark.parametrize("key_serializer, value_serializer, key, value",
                         [(DoubleSerializer, StringSerializer('utf_8'),
                           random.uniform(-32768.0, 32768.0), u'Jämtland'),
                          (StringSerializer('utf_32'), LongSerializer,
                           u'Härjedalen', random.getrandbits(63)),
                          (IntegerSerializer, ShortSerializer,
                           random.randint(-32768, 32768), random.randint(-32768, 32768))])
def test_mixed_serialization(key_serializer, value_serializer, key, value):
    """
    Tests basic mixed serializer/deserializer functionality.

    :param Serializer key_serializer: key serializer
    :param Serializer value_serializer: value serializer
    :param object key: key data
    :param object value: value data

    :raises: AssertionError on test failure

    :returns: None
    :rtype: None
    """
    topic = create_topic("serialization-numeric")

    producer = get_producer(key_serializer=key_serializer, value_serializer=value_serializer)
    producer.produce(topic, key=key, value=value)
    producer.flush()

    consumer = get_consumer("serialization-numeric",
                            key_serializer=key_serializer, value_serializer=value_serializer)
    consumer.subscribe([topic])

    msg = consumer.poll()

    assert msg.key() == key
    assert msg.value() == value

    consumer.close()
