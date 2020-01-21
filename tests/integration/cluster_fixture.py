#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
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
# limitations under the License.
#

import copy
import os
import tempfile
import threading
from uuid import uuid4, uuid1

from trivup.clusters.KafkaCluster import KafkaCluster

from confluent_kafka import Producer, Consumer

from confluent_kafka.admin import AdminClient, NewTopic
from tests.integration.java_fixture import JavaProducer, JavaConsumer


class ClusterFixture(object):
    _slots_ = ()
    """
    ClusterFixture factory.
    Provides client configs enabling integration tests to execute against a live cluster.
    """

    def __new__(cls, options):
        conf = {k[6:]: v for (k, v) in os.environ.items() if k.startswith('KAFKA')}

        if bool(conf):
            return ExternalClusterFixture(conf, options)

        return TrivupFixture(options)


class ExternalClusterFixture(object):
    __slots__ = ['_conf', '_topic']

    """
        Configures integration tests clients to communicate with external(developer provided) cluster.
        All client configs are valid and follow the following naming convention.
    """

    OPTIONAL = ['TOPIC', 'SCHEMA_REGISTRY_URL']
    REQUIRED = ['BOOTSTRAP_SERVERS']
    PROPS = OPTIONAL + REQUIRED

    def __init__(self, conf, options=None):

        self._topic = conf.pop('TOPIC', "confluent_kafka_test{}".format(str(uuid4())))

        for r in self.REQUIRED:
            if r not in conf:
                raise ValueError("{} is a required config".format(conf))

        self._conf = {k.lower().replace('_', '.'): v for (k, v) in conf.items()}

    def client_conf(self, options=None):
        conf = copy.deepcopy(self._conf)

        if options is not None:
            conf.update(options)

        return conf

    def producer(self, updates=None):
        return Producer(self.client_conf(updates))

    def consumer(self, updates):
        if 'group.id' not in updates:
            updates.update({'group.id': str(uuid1())})

        return Consumer(self.client_conf(updates))

    @property
    def schema_registry(self):
        return self._conf.get('schema.registry.url')


class TrivupFixture(object):
    __slots__ = ['_cluster', '_admin', '_producer']

    default_options = {
        'broker_cnt': 1,
    }

    def __init__(self, options):
        os.environ['TRIVUP_ROOT'] = tempfile.tempdir
        self._cluster = KafkaCluster(**options)
        self._admin = None
        self._producer = None
        self._cluster.wait_operational()

    def client_conf(self, options=None):
        conf = self._cluster.client_conf()

        if hasattr(self._cluster, 'sr'):
            conf['schema.registry.url'] = self._cluster.sr.get('url')

        if options is not None:
            conf.update(options)

        return conf

    @classmethod
    def _topic_conf(cls, options=None):
        conf = {'num_partitions': 1,
                'replication_factor': 1}

        if options is not None:
            conf.update(options)

        return conf

    def producer(self, options=None):
        """
        Returns a producer bound to this cluster.

        :param dict options: additional producer configuration values.
        :returns: a new producer instance.
        :rtype: confluent_kafka.Producer
        """
        return Producer(self.client_conf(options))

    def java_producer(self, options=None):
        """
        Starts a Java KafkaProducer instance in a subprocess and returns the handle.

        :param dict options: additional producer configuration values.
        :returns: a JavaProducer instance
        :rtype: JavaProducer
        """
        return JavaProducer(self.client_conf(options), self._cluster)

    def consumer(self, options=None):
        """
        Returns a consumer bound to this cluster.

        :param dict options: additional consumer values to be used.
        :returns: a new consumer instance.
        :rtype: confluent_kafka.Consumer
        """
        conf = {
            'group.id': str(uuid1()),
            'auto.offset.reset': 'earliest',
            'enable.partition.eof': True
            }

        if options is not None:
            conf.update(options)

        return Consumer(self.client_conf(conf))

    def java_consumer(self, options=None):
        """
        Starts a Java KafkaConsumer instance in a subprocess and returns the handle.

        :param dict options: additional consumer values to be used.
        :returns: a JavaConsumer instance.
        :rtype: JavaConsumer
        """
        conf = {
            'group.id': str(uuid1()),
            'auto.offset.reset': 'earliest',
            }

        if options is not None:
            conf.update(options)

        return JavaConsumer(self.client_conf(conf), self._cluster)

    def create_topic(self, name, topic_options=None):
        """
        Creates a new topic with this cluster.

        :param str name: topic name
        :param dict topic_options: additions/overrides to topic configuration.
        :returns: The topic's name
        :rtype: str
        """
        if self._admin is None:
            self._admin = AdminClient(self.client_conf())

        future_topic = self._admin.create_topics([NewTopic(name, **self._topic_conf(topic_options))])
        future_topic.get(name).result()

        return name

    def seed_topic(self, topic, value_source=None, key_source=None, header_source=None):
        """
        Populates a topic with data asynchronously.

        If there are fewer keys or headers than values the index will be wrapped and reused with
        each subsequent produce request.

        :param str topic: topic to produce test data to.
        :param list value_source: list or other iterable containing message key data to be produced.
        :param list key_source: list or other iterable containing message value data to be produced.
        :param list(dict) header_source: headers to attach to test data.
        """

        if self._producer is None:
            self._producer = Producer(self.client_conf({'linger.ms': 500}))

        if value_source is None:
            value_source = ['test-data{}'.format(i) for i in range(0, 100)]

        if key_source is None:
            key_source = [None]

        produce_thread = threading.Thread(target=_produce,
                                          args=(self._producer, topic, value_source,
                                                key_source, header_source))

        produce_thread.daemon = True
        produce_thread.start()

    @property
    def schema_registry(self):
        return self._cluster.sr.get('url')


def _produce(producer, topic, value_source, key_source, header_source=None):
    num_messages = len(value_source)
    num_keys = len(key_source)
    num_headers = len(header_source) if header_source else 0

    print('# producing {} messages to topic {}'.format(num_messages, topic))
    for i in range(0, num_messages):
        while True:
            try:
                if header_source is not None:
                    producer.produce(topic,
                                     value=value_source[i],
                                     key=key_source[i % num_keys],
                                     headers=header_source[i % (num_headers + 1)])
                else:
                    producer.produce(topic,
                                     value=value_source[i],
                                     key=key_source[i % num_keys])
                break
            except BufferError:
                producer.poll(0.1)
            continue
        producer.poll(0.0)

    producer.flush(10.0)

    print('# finished producing {} messages to topic {}'.format(num_messages, topic))
