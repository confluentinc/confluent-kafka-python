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

def test_consumer_memberid(kafka_cluster):
    """
    Test cooperative-sticky assignor and various aspects
    of the incremental rebalancing API.
    """

    consumer_conf = {
            'group.id': 'test'
           }

    topic = "testmemberid"

    kafka_cluster.create_topic(topic)
    

    consumer = kafka_cluster.consumer(consumer_conf)

    kafka_cluster.seed_topic(topic, value_source=[b'memberid'])

    consumer.subscribe([topic])
    msg = consumer.poll(10)
    assert msg is not None
    assert msg.value() == b'memberid'

    assert consumer.kafka_memberid() is not None 
    print("jing liu after poll memberid", consumer.kafka_memberid())

    consumer.close()