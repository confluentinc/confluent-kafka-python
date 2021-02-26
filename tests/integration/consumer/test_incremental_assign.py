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
# limit

import pytest
from uuid import uuid1

from confluent_kafka import TopicPartition, OFFSET_BEGINNING, KafkaException, KafkaError


def test_incremental_assign(kafka_cluster):
    """
    Test incremental_assign and incremental_unassign
    """

    consumer_conf = {'group.id': str(uuid1()),
                     'enable.auto.commit': 'false',
                     'auto.offset.reset': 'error'}

    topic1 = kafka_cluster.create_topic("topic1")
    topic2 = kafka_cluster.create_topic("topic2")

    kafka_cluster.seed_topic(topic1, value_source=[b'a'])
    kafka_cluster.seed_topic(topic2, value_source=[b'b'])

    consumer = kafka_cluster.consumer(consumer_conf)

    consumer.incremental_assign([TopicPartition(topic1, 0, OFFSET_BEGINNING)])
    msg1 = consumer.poll(10)
    assert msg1 is not None
    assert 0 == msg1.offset()
    assert topic1 == msg1.topic()
    assert 0 == msg1.partition()
    consumer.commit(msg1)

    # should not be possible to incrementally assign to the same partition twice.
    with pytest.raises(KafkaException) as exc_info:
        consumer.incremental_assign([TopicPartition(topic1, 0, OFFSET_BEGINNING)])
    assert exc_info.value.args[0].code() == KafkaError._CONFLICT, \
        "Expected _CONFLICT, not {}".format(exc_info)

    consumer.incremental_assign([TopicPartition(topic2, 0, OFFSET_BEGINNING)])
    msg2 = consumer.poll(10)
    assert msg2 is not None
    assert 0 == msg2.offset()
    assert topic2 == msg2.topic()
    assert 0 == msg2.partition()

    consumer.incremental_unassign([TopicPartition(topic1, 0)])
    kafka_cluster.seed_topic(topic1, value_source=[b'aa'])
    msg3 = consumer.poll(2)
    assert msg3 is None

    kafka_cluster.seed_topic(topic2, value_source=[b'aaa'])
    msg4 = consumer.poll(10)
    assert msg4 is not None
    assert 1 == msg4.offset()
    assert topic2 == msg4.topic()
    assert 0 == msg4.partition()
    assert msg4.value() == b'aaa'

    consumer.incremental_assign([TopicPartition(topic1, 0)])
    msg5 = consumer.poll(10)
    assert msg5 is not None
    assert 1 == msg5.offset()
    assert topic1 == msg5.topic()
    assert 0 == msg5.partition()

    msg6 = consumer.poll(1)
    assert msg6 is None

    consumer.incremental_unassign([TopicPartition(topic1, 0)])

    # should not be possible to incrementally unassign from a partition not in the current assignment
    with pytest.raises(KafkaException) as exc_info:
        consumer.incremental_unassign([TopicPartition(topic1, 0)])
    assert exc_info.value.args[0].code() == KafkaError._INVALID_ARG, \
        "Expected _INVALID_ARG, not {}".format(exc_info)

    consumer.unassign()
