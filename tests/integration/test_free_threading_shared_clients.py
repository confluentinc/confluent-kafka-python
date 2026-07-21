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

import concurrent.futures
import sys
import sysconfig
import threading
import time
import uuid

import pytest

from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient

FREE_THREADED_BUILD = bool(sysconfig.get_config_var("Py_GIL_DISABLED"))

pytestmark = pytest.mark.skipif(
    not FREE_THREADED_BUILD,
    reason="requires a free-threaded CPython build",
)


def test_shared_clients_against_real_broker(kafka_cluster):
    assert not sys._is_gil_enabled()

    topic = kafka_cluster.create_topic_and_wait_propogation(
        "confluent-kafka-nogil",
        {"num_partitions": 1, "replication_factor": 1},
    )
    admin = AdminClient(kafka_cluster.client_conf())

    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
            metadata = list(executor.map(lambda _: admin.list_topics(timeout=5), range(32)))
        assert all(topic in item.topics for item in metadata)

        producer = Producer(kafka_cluster.client_conf({"queue.buffering.max.messages": 100_000}))
        delivered = 0
        delivered_lock = threading.Lock()

        def on_delivery(error, _message):
            nonlocal delivered
            assert error is None
            with delivered_lock:
                delivered += 1

        def produce(thread_id):
            for index in range(250):
                producer.produce(
                    topic,
                    key=f"{thread_id}:{index}",
                    value=b"free-threaded",
                    callback=on_delivery,
                )
                if index % 25 == 0:
                    producer.poll(0)

        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
            list(executor.map(produce, range(8)))

        assert producer.flush(20) == 0
        assert delivered == 2_000

        consumer = Consumer(
            kafka_cluster.client_conf(
                {
                    "group.id": f"confluent-kafka-nogil-{uuid.uuid4().hex}",
                    "auto.offset.reset": "earliest",
                    "enable.auto.commit": False,
                }
            )
        )
        consumer.subscribe([topic])
        seen = set()
        seen_lock = threading.Lock()
        stop = threading.Event()
        deadline = time.monotonic() + 30

        def consume():
            while not stop.is_set() and time.monotonic() < deadline:
                message = consumer.poll(0.2)
                if message is None:
                    continue
                assert message.error() is None
                with seen_lock:
                    seen.add(message.key())
                    if len(seen) == 2_000:
                        stop.set()

        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
            list(executor.map(lambda _: consume(), range(4)))

        consumer.close()
        assert len(seen) == 2_000
        assert not sys._is_gil_enabled()
    finally:
        kafka_cluster.delete_topic(topic)
