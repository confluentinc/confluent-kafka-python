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
import time
from uuid import uuid1

from confluent_kafka import KafkaException, KafkaError


def test_cooperative_rebalance_1(kafka_cluster):
    """
    Test cooperative-sticky assignor and various aspects
    of the incremental rebalancing API.
    """

    consumer_conf = {'group.id': str(uuid1()),
                     'partition.assignment.strategy': 'cooperative-sticky',
                     'enable.auto.commit': 'false',
                     'auto.offset.reset': 'earliest',
                     'heartbeat.interval.ms': '2000',
                     'session.timeout.ms': '6000',  # minimum allowed by broker
                     'max.poll.interval.ms': '6500'}

    class RebalanceState:
        def __init__(self):
            self.assign_count = 0
            self.revoke_count = 0
            self.lost_count = 0

        def on_assign(self, consumer, partitions):
            self.assign_count += 1
            assert 1 == len(partitions)

        def on_revoke(self, consumer, partitions):
            self.revoke_count += 1

        def on_lost(self, consumer, partitions):
            self.lost_count += 1

    reb = RebalanceState()

    topic1 = kafka_cluster.create_topic("topic1")
    topic2 = kafka_cluster.create_topic("topic2")

    consumer = kafka_cluster.consumer(consumer_conf)

    kafka_cluster.seed_topic(topic1, value_source=[b'a'])

    consumer.subscribe([topic1],
                       on_assign=reb.on_assign,
                       on_revoke=reb.on_revoke,
                       on_lost=reb.on_lost)
    msg = consumer.poll(10)
    assert msg is not None
    assert msg.value() == b'a'

    # Subscribe to a second one partition topic, the second assign
    # call should be incremental (checked in the handler).
    consumer.subscribe([topic1, topic2],
                       on_assign=reb.on_assign,
                       on_revoke=reb.on_revoke,
                       on_lost=reb.on_lost)

    kafka_cluster.seed_topic(topic2, value_source=[b'b'])
    msg2 = consumer.poll(10)
    assert msg2 is not None
    assert msg2.value() == b'b'

    assert 2 == reb.assign_count
    assert 0 == reb.lost_count
    assert 0 == reb.revoke_count

    msg3 = consumer.poll(1)
    assert msg3 is None

    # Exceed MaxPollIntervalMs => lost partitions.
    time.sleep(8)

    # Poll again to raise the max.poll error.
    with pytest.raises(KafkaException) as e:
        consumer.poll(1)
    assert e.value.args[0].code() == KafkaError._MAX_POLL_EXCEEDED

    # And poll again to trigger rebalance callbacks
    msg4 = consumer.poll(1)
    assert msg4 is None

    assert 2 == reb.assign_count
    assert 1 == reb.lost_count
    assert 0 == reb.revoke_count

    consumer.close()
