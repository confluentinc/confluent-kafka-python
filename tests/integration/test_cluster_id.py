#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright 2026 Confluent Inc.
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
#
"""``cluster_id()`` on every client type."""

import pytest

from confluent_kafka.aio import AIOConsumer, AIOProducer

TIMEOUT = 30.0


def test_cluster_id_agrees_across_clients(kafka_cluster):
    """The admin client, producer and consumer all report the same id as DescribeCluster."""
    admin = kafka_cluster.admin()
    producer = kafka_cluster.cimpl_producer()
    consumer = kafka_cluster.cimpl_consumer()

    try:
        cluster_id = admin.cluster_id(timeout=TIMEOUT)
        assert isinstance(cluster_id, str) and cluster_id

        assert producer.cluster_id(timeout=TIMEOUT) == cluster_id
        assert consumer.cluster_id(timeout=TIMEOUT) == cluster_id
        assert admin.describe_cluster(request_timeout=TIMEOUT).result().cluster_id == cluster_id

        # stable across repeated calls, served from librdkafka's metadata cache
        assert all(producer.cluster_id(timeout=TIMEOUT) == cluster_id for _ in range(50))
    finally:
        producer.flush()
        consumer.close()


def test_cluster_id_fails_after_close(kafka_cluster):
    consumer = kafka_cluster.cimpl_consumer()
    assert consumer.cluster_id(timeout=TIMEOUT)

    consumer.close()

    with pytest.raises(RuntimeError):
        consumer.cluster_id(timeout=1)


async def test_cluster_id_on_asyncio_clients(kafka_cluster):
    cluster_id = kafka_cluster.admin().cluster_id(timeout=TIMEOUT)

    producer = AIOProducer(kafka_cluster.client_conf())
    consumer = AIOConsumer(kafka_cluster.client_conf({'group.id': 'test-cluster-id-aio'}))
    try:
        assert await producer.cluster_id(timeout=TIMEOUT) == cluster_id
        assert await consumer.cluster_id(timeout=TIMEOUT) == cluster_id
    finally:
        await producer.close()
        await consumer.close()
