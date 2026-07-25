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


def test_producer_clusterid(kafka_cluster):
    """
    Test producer cluster_id.
    """

    topic = "testclusterid_producer"

    kafka_cluster.create_topic_and_wait_propogation(topic)

    producer = kafka_cluster.cimpl_producer({})

    assert producer is not None

    delivered = []

    def on_delivery(err, msg):
        assert err is None
        delivered.append(msg)

    producer.produce(topic, value=b'clusterid', on_delivery=on_delivery)
    producer.flush(10)

    assert len(delivered) == 1

    cluster_id = producer.cluster_id()
    assert isinstance(cluster_id, str)
    assert len(cluster_id) > 0

    producer.close()

    with pytest.raises(RuntimeError) as error_info:
        producer.cluster_id()
    assert error_info.value.args[0] == "Producer has been closed"
