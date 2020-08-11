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
#

import pytest
from confluent_kafka import TopicPartition, OFFSET_END, KafkaError

from confluent_kafka.error import ConsumeError
from confluent_kafka.serialization import StringSerializer


def test_consume_error(kafka_cluster):
    """
    Tests to ensure librdkafka errors are propagated as
    an instance of ConsumeError.
    """
    topic = kafka_cluster.create_topic("test_commit_transaction")
    consumer_conf = {'group.id': 'pytest', 'enable.partition.eof': True}

    producer = kafka_cluster.producer()
    producer.produce(topic=topic, value="a")
    producer.flush()

    consumer = kafka_cluster.consumer(consumer_conf,
                                      value_deserializer=StringSerializer())
    consumer.assign([TopicPartition(topic, 0, OFFSET_END)])

    with pytest.raises(ConsumeError) as exc_info:
        # Trigger EOF error
        consumer.poll()
    assert exc_info.value.args[0].code() == KafkaError._PARTITION_EOF, \
        "Expected _PARTITION_EOF, not {}".format(exc_info)
