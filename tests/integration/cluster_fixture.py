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

import copy
import threading
from uuid import uuid4

from trivup.clusters.KafkaCluster import KafkaCluster

from confluent_kafka import Producer

import os


class ClusterFixture(object):
    _slots_ = ()
    """
    ClusterFixture factory.

    Provides client configs enabling integration tests to execute against a live cluster.
    """

    def __new__(cls, with_sr=False):
        conf = {k[6:]: v for (k, v) in os.environ.items() if k.startswith('KAFKA')}

        if bool(conf):
            return ExternalClusterFixture(conf, with_sr)

        return TrivupFixture(with_sr)


class ExternalClusterFixture(object):
    __slots__ = ['_conf', '_topic']

    """
        Configures integration tests clients to communicate with external(developer provided) cluster.
        All client configs are valid and follow the following naming convention.

    """

    OPTIONAL = ['TOPIC']
    REQUIRED = ['BOOTSTRAP_SERVERS', 'SCHEMA_REGISTRY_URL']
    PROPS = OPTIONAL + REQUIRED

    def __init__(self, conf, with_sr=False):

        self._topic = conf.pop('TOPIC', "confluent_kafka_test{}".format(str(uuid4())))

        for r in self.REQUIRED:
            if r not in conf:
                raise ValueError("{} is a required config".format(conf))

        if not with_sr:
            conf.pop('SCHEMA_REGISTRY_URL', None)

        self._conf = {k.lower().replace('_', '.'): v for (k, v) in conf.items()}

    @property
    def client_conf(self):
        return copy.deepcopy(self._conf)

    @property
    def topic(self):
        return self._topic

    @property
    def brokers(self):
        return self._conf.get('bootstrap.servers')

    @property
    def schema_registry(self):
        return self._conf.get('schema.registry.url')

    def produce(self, num_messages, value=None, key=None, headers=None):
        produce_thread = threading.Thread(target=_produce,
                                          args=(self.client_conf, self._topic, num_messages,
                                                value, key, headers))

        produce_thread.daemon = True
        produce_thread.start()


class TrivupFixture(object):
    __slots__ = ['cluster', '_topic']

    def __init__(self, with_sr=True):
        self.cluster = KafkaCluster(with_sr=with_sr)
        self._topic = "confluent_kafka_test{}".format(str(uuid4()))
        self.cluster.wait_operational()

    @property
    def client_conf(self):
        conf = self.cluster.client_conf()

        if hasattr(self.cluster, 'sr'):
            conf['schema.registry.url'] = self.cluster.sr.get('url')

        return conf

    @property
    def schema_registry(self):
        return self.cluster.sr.get('url')

    @property
    def topic(self):
        return self._topic

    def produce(self, num_messages, value=None, key=None, headers=None):
        produce_thread = threading.Thread(target=_produce,
                                          args=(self.client_conf, self._topic, num_messages,
                                                value, key, headers))

        produce_thread.daemon = True
        produce_thread.start()


def _produce(conf, topic, num_messages, value="test-value", key=None, headers=None):
    conf.update({'linger.ms': 500})

    p = Producer(conf)

    print('# producing %d messages to topic %s' % (num_messages, topic))
    for i in range(0, num_messages):
        while True:
            try:
                if headers:
                    p.produce(topic, value=value, key=key, headers=headers)
                else:
                    p.produce(topic, value=value, key=key)
                break
            except BufferError:
                # Local queue is full (slow broker connection?)
                p.poll(0.1)
            continue
        p.poll(0.0)

    p.flush(10.0)

    print('# finished producing %d messages to topic %s' % (num_messages, topic))
