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
# limitations under the License.

import pytest
from tests.common import TestUtils


def test_consumer_memberid(kafka_cluster):
    """
    Test consumer memberid.
    """

    consumer_conf = {'group.id': 'test'}

    topic = "testmemberid"

    kafka_cluster.create_topic_and_wait_propogation(topic)

    consumer = kafka_cluster.consumer(consumer_conf)

    assert consumer is not None
    before_memberid = consumer.memberid()

    # With implementation of KIP-1082, member id is generated on the client
    # side for ConsumerGroupHeartbeat used in the `consumer` protocol
    # introduced in KIP-848
    if TestUtils.use_group_protocol_consumer():
        assert before_memberid is not None
        assert isinstance(before_memberid, str)
        assert len(before_memberid) > 0
    else:
        assert before_memberid is None

    kafka_cluster.seed_topic(topic, value_source=[b'memberid'])

    consumer.subscribe([topic])
    msg = consumer.poll(10)
    assert msg is not None
    assert msg.value() == b'memberid'
    after_memberid = consumer.memberid()
    assert isinstance(after_memberid, str)
    assert len(after_memberid) > 0
    if TestUtils.use_group_protocol_consumer():
        assert before_memberid == after_memberid
    consumer.close()

    with pytest.raises(RuntimeError) as error_info:
        consumer.memberid()
    assert error_info.value.args[0] == "Consumer closed"
