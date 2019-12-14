#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
#
# Copyright 2019 Confluent Inc.
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

import struct
import threading
from uuid import uuid4

import pytest

from confluent_kafka import Producer
from trivup.clusters.KafkaCluster import KafkaCluster

topic = "confluent_kafka" + str(uuid4())
kafka_cluster = None


@pytest.fixture(scope="module")
def topic_fixture():
    return topic


@pytest.fixture(scope="package")
def kafka_cluster_fixture():
    global kafka_cluster

    kafka_cluster = KafkaCluster()
    yield kafka_cluster

    kafka_cluster.stop(force=False, timeout=60)


@pytest.fixture(scope="session")
def print_commit_callback_fixture():
    def print_commit_result(err, partitions):
        if err is not None:
            print('# Failed to commit offsets: %s: %s' % (err, partitions))
        else:
            print('# Committed offsets for: %s' % partitions)

    return print_commit_result


@pytest.fixture(scope="session")
def error_cb_fixture():
    def error_cb(err):
        print('Error: %s' % err)

    return error_cb


@pytest.fixture(scope="session")
def delivery_report_fixture():
    def decorator(silent=False):
        return _MyTestDr(silent)

    return decorator


class _MyTestDr(object):
    """ Producer: Delivery report callback """

    def __init__(self, silent=False):
        super(_MyTestDr, self).__init__()
        self.msgs_delivered = 0
        self.bytes_delivered = 0
        self.silent = silent

    @staticmethod
    def _delivery(err, msg, silent=False):
        if err:
            print('Message delivery failed (%s [%s]): %s' %
                  (msg.topic(), str(msg.partition()), err))
            return 0
        else:
            if not silent:
                print('Message delivered to %s [%s] at offset [%s]: %s' %
                      (msg.topic(), msg.partition(), msg.offset(), msg.value()))
            return 1

    def delivery(self, err, msg):
        if err:
            print('Message delivery failed (%s [%s]): %s' %
                  (msg.topic(), str(msg.partition()), err))
            return
        elif not self.silent:
            print('Message delivered to %s [%s] at offset [%s]: %s' %
                  (msg.topic(), msg.partition(), msg.offset(), msg.value()))
        self.msgs_delivered += 1
        self.bytes_delivered += len(msg)


@pytest.fixture(scope="session")
def produce_headers_fixture():
    return [('foo1', 'bar'),
            ('foo1', 'bar2'),
            ('foo2', b'1'),
            (u'Jämtland', u'Härjedalen'),  # automatically utf-8 encoded
            ('nullheader', None),
            ('empty', ''),
            ('foobin', struct.pack('hhl', 10, 20, 30))]


@pytest.fixture(scope="session")
def expected_headers_fixture():
    return [('foo1', b'bar'),
            ('foo1', b'bar2'),
            ('foo2', b'1'),
            (u'Jämtland', b'H\xc3\xa4rjedalen'),  # not automatically utf-8 decoded
            ('nullheader', None),
            ('empty', b''),
            ('foobin', struct.pack('hhl', 10, 20, 30))]


@pytest.fixture(scope="session")
def cluster_producer_fixture():
    """ pre-populate topic with some data for consumer tests"""

    def _produce_decorator(num_messages, headers=dict()):
        produce_thread = threading.Thread(target=_produce,
                                          args=(num_messages, headers))
        produce_thread.daemon = True
        produce_thread.start()

    return _produce_decorator


def _produce(num_messages, headers=dict()):
    conf = kafka_cluster.client_conf()
    conf.update({'linger.ms': 500})
    p = Producer(conf)

    print('# priming cluster with %d messages to topic %s' % (num_messages, topic))
    for i in range(0, num_messages):
        while True:
            try:
                p.produce(topic, value="test-data", headers=headers)
                break
            except Exception:
                # Local queue is full (slow broker connection?)
                p.poll(0.1)
            continue
        p.poll(0.0)

    p.flush(10.0)

    print('# finished producing %d messages to topic %s' % (num_messages, topic))
