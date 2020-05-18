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

from confluent_kafka.error import ConsumeError
from confluent_kafka.serialization import StringSerializer


def test_commit_transaction(kafka_cluster):
    topic = kafka_cluster.create_topic("test_commit_transaction")
    consumer_conf = {'enable.partition.eof': True}

    producer = kafka_cluster.producer()
    producer.produce(topic=topic, value="a")
    producer.flush()

    consumer = kafka_cluster.consumer(consumer_conf,
                                      value_deserializer=StringSerializer())
    consumer.subscribe([topic])

    # read only valid offset
    consumer.poll()

    with pytest.raises(ConsumeError, match="No more messages"):
        # Trigger EOF error
        consumer.poll()
