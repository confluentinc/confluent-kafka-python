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

from confluent_kafka import TopicPartition 

def test_read_after_commit_offset_only(kafka_cluster):
    """
    TODO: write doc
    """

    consumer_conf = {
            'group.id': 'test-store-offset-group',
            'auto.offset.reset': 'earliest',
            'enable.auto.offset.store': False,
            'enable.auto.commit': False,
        }

    topic = kafka_cluster.create_topic("test-store-offset-topic")

    kafka_cluster.seed_topic(topic)

    consumer = kafka_cluster.cimpl_consumer(consumer_conf)

    consumer.subscribe([topic])

    msgs = consumer.consume(num_messages=10, timeout=10)

    consumer.store_offsets(msgs[0])
    consumer.store_offsets(msgs[1])
    consumer.store_offsets(msgs[2])
    consumer.store_offsets(msgs[3])
    consumer.commit()

    consumer.seek(TopicPartition(topic, msgs[4].partition() ,msgs[4].offset()))

    newmsgs = consumer.consume(num_messages=10, timeout=10)

    assert newmsgs[0].offset() == msgs[4].offset()

    consumer.close()


