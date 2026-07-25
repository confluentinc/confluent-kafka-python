#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright 2025 Confluent Inc.
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


def test_consumer_clusterid(kafka_cluster):
    """
    Test consumer cluster_id.
    """

    consumer_conf = {'group.id': 'test'}

    topic = "testclusterid_consumer"

    kafka_cluster.create_topic_and_wait_propogation(topic)

    consumer = kafka_cluster.consumer(consumer_conf)

    assert consumer is not None

    kafka_cluster.seed_topic(topic, value_source=[b'clusterid'])

    consumer.subscribe([topic])
    msg = consumer.poll(10)
    assert msg is not None

    cluster_id = consumer.cluster_id()
    assert isinstance(cluster_id, str)
    assert len(cluster_id) > 0

    consumer.close()

    with pytest.raises(RuntimeError) as error_info:
        consumer.cluster_id()
    assert error_info.value.args[0] == "Consumer closed"
