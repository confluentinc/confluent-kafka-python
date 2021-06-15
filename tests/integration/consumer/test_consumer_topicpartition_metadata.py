#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright 2021 Confluent Inc.
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

from confluent_kafka import TopicPartition, KafkaException


def test_consumer_topicpartition_metadata(kafka_cluster):

    topic = kafka_cluster.create_topic("test_topicpartition")
    consumer_conf = {'group.id': 'pytest'}

    c = kafka_cluster.consumer(consumer_conf)

    try:
        c.commit(offsets=[TopicPartition(topic, 0, 0)],
                 asynchronous=False)
    except KafkaException as e:
        print('commit failed with %s' % e)

    offsets = c.committed([TopicPartition(topic, 0, 0)])
    for tp in offsets:
        assert tp.metadata is None

    c.close()

    c = kafka_cluster.consumer(consumer_conf)
    metadata = "\x01abcdefg\\x00afd\\xff"

    try:
        c.commit(offsets=[TopicPartition(topic, 0, 1, metadata, len(metadata))],
                 asynchronous=False)
    except KafkaException as e:
        print('commit failed with %s' % e)

    offsets = c.committed([TopicPartition(topic, 0)], timeout=100)
    for tp in offsets:
        assert str.encode(metadata) == tp.metadata

    c.close()
