#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright 2022 Confluent Inc.
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

import time
from confluent_kafka import TopicPartition


def commit_and_check(consumer, topic, metadata):
    if metadata is None:
        consumer.commit(offsets=[TopicPartition(topic, 0, 1)], asynchronous=False)
    else:
        consumer.commit(offsets=[TopicPartition(topic, 0, 1, metadata)], asynchronous=False)

    offsets = consumer.committed([TopicPartition(topic, 0)], timeout=100)
    assert len(offsets) == 1
    assert offsets[0].metadata == metadata


def test_consumer_topicpartition_metadata(kafka_cluster):
    topic = kafka_cluster.create_topic_and_wait_propogation("test_topicpartition")
    consumer_conf = {'group.id': 'pytest'}

    c = kafka_cluster.consumer(consumer_conf)
    c.subscribe([topic])
    time.sleep(5)

    # Commit without any metadata.
    metadata = None
    commit_and_check(c, topic, metadata)

    # Commit with only ASCII metadata.
    metadata = 'hello world'
    commit_and_check(c, topic, metadata)

    # Commit with Unicode characters in metadata.
    metadata = 'नमस्ते दुनिया'
    commit_and_check(c, topic, metadata)

    # Commit with empty string as metadata.
    metadata = ''
    commit_and_check(c, topic, metadata)

    # Commit with invalid metadata (with null byte in the middle).
    metadata = 'xyz\x00abc'
    try:
        commit_and_check(c, topic, metadata)
        # We should never reach this point, since the prior statement should throw.
        assert False
    except ValueError as ve:
        assert 'embedded null character' in str(ve)

    c.close()
