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

from confluent_kafka import KafkaError
from confluent_kafka.error import ConsumeError


def test_cooperative_rebalance_2(kafka_cluster):
    """
    Test that lost partitions events are directed to the
    revoked handler if the lost partitions handler isn't
    specified.
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

        def on_assign(self, consumer, partitions):
            self.assign_count += 1
            assert 1 == len(partitions)

        def on_revoke(self, consumer, partitions):
            self.revoke_count += 1

    reb = RebalanceState()

    topic1 = kafka_cluster.create_topic("topic1")

    consumer = kafka_cluster.consumer(consumer_conf)

    kafka_cluster.seed_topic(topic1, value_source=[b'a'])

    consumer.subscribe([topic1],
                       on_assign=reb.on_assign,
                       on_revoke=reb.on_revoke)
    msg = consumer.poll(10)
    assert msg is not None
    assert msg.value() == b'a'
    assert reb.assign_count == 1

    # Exceed MaxPollIntervalMs => lost partitions.
    time.sleep(8)

    with pytest.raises(ConsumeError) as exc_info:
        consumer.poll(1)
    assert exc_info.value.args[0].code() == KafkaError._MAX_POLL_EXCEEDED, \
        "Expected _MAX_POLL_EXCEEDED, not {}".format(exc_info)

    # The second call should trigger the revoked handler (because a lost
    # handler has not been specified).
    assert 0 == reb.revoke_count
    consumer.poll(1)
    assert 1 == reb.revoke_count

    consumer.close()
