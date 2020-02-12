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
# limitations under the License.
#

import copy
from uuid import uuid4, uuid1

from trivup.clusters.KafkaCluster import KafkaCluster

from confluent_kafka import Producer, Consumer
from confluent_kafka.admin import AdminClient, NewTopic


class KafkaClusterFixture(object):
    __slots__ = ['_cluster', '_admin', '_producer']

    def __init__(self, conf):
        raise NotImplementedError("KafkaCluster should never be instantiated directly")

    @property
    def schema_registry(self):
        raise NotImplementedError("schema_registry has not been implemented")

    def client_conf(self, conf=None):
        raise NotImplementedError("client_conf has not been implemented")

    @classmethod
    def _topic_conf(cls, conf=None):
        topic_conf = {'num_partitions': 1,
                      'replication_factor': 1}

        if conf is not None:
            topic_conf.update(conf)

        return topic_conf

    def producer(self, conf=None):
        """
        Returns a producer bound to this cluster.

        :param dict conf: producer configuration overrides
        :returns: a new producer instance.
        :rtype: confluent_kafka.Producer
        """
        return Producer(self.client_conf(conf))

    def consumer(self, conf=None):
        """
        Returns a consumer bound to this cluster.

        :param dict conf: consumer configuration overrides
        :returns: a new consumer instance.
        :rtype: confluent_kafka.Consumer
        """
        if conf is None or 'group.id' not in conf:
            conf.update({'group.id': uuid1()})

        return Consumer(self.client_conf(conf))

    def create_topic(self, prefix, conf=None):
        """
        Creates a new topic with this cluster.

        :param str prefix: topic name
        :param dict conf: additions/overrides to topic configuration.
        :returns: The topic's name
        :rtype: str
        """
        if self._admin is None:
            self._admin = AdminClient(self.client_conf())

        name = prefix + str(uuid1())
        future_topic = self._admin.create_topics([NewTopic(name,
                                                           **self._topic_conf(conf))])

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

        KafkaClusterFixture._produce(self._producer, topic, value_source, key_source, header_source)

    @staticmethod
    def _produce(producer, topic, value_source, key_source, header_source=None):
        """
        Produces a message for each value in value source to a topic.

        value_source drives the produce loop. If corresponding key and header sources
        are specified they will be applied in round robbin order. In the event there
        are less keys or headers than values the index will be wrapped.

        :param producer: producer instance to use
        :param topic:  topic to produce to
        :param value_source: collection of message values
        :param key_source: optional keys to accompany values_source items
        :param header_source: optional headers to accompany value_source items
        :return:
        """
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

        producer.flush()

        print('# finished producing {} messages to topic {}'.format(num_messages, topic))


class TrivupFixture(KafkaClusterFixture):
    """
    Starts and stops a Kafka Cluster instances for use in testing.

    Optionally provides a Confluent Schema Registry instance for Avro testing.

    """

    default_options = {
        'broker_cnt': 1,
    }

    def __init__(self, conf):
        self._cluster = KafkaCluster(**conf)
        self._admin = None
        self._producer = None
        self._cluster.wait_operational()

    @property
    def schema_registry(self):
        if hasattr(self._cluster, 'sr'):
            return self._cluster.sr.get('url')
        return None

    def client_conf(self, conf=None):
        client_conf = self._cluster.client_conf()

        if self.schema_registry:
            client_conf['schema.registry.url'] = self.schema_registry

        if conf is not None:
            client_conf.update(conf)

        return client_conf


class ExternalClusterFixture(KafkaClusterFixture):
    __slots__ = ['_topic']

    """
    Configures integration tests clients to communicate with external
    (developer provided) cluster.

    All client configuration values are valid after replacing '.', with "_".
    """

    OPTIONAL = ['TOPIC', 'SCHEMA_REGISTRY_URL']
    REQUIRED = ['BOOTSTRAP_SERVERS']
    PROPS = OPTIONAL + REQUIRED

    def __init__(self, conf):

        self._topic = conf.pop('TOPIC', "confluent_kafka_test{}".format(str(uuid4())))

        for r in self.REQUIRED:
            if r not in conf:
                raise ValueError("{} is a required config".format(conf))

        self._cluster = {k.lower().replace('_', '.'): v for (k, v) in conf.items()}

    @property
    def schema_registry(self):
        return self._cluster.get('schema.registry.url', None)

    def client_conf(self, conf=None):
        cluster_conf = copy.deepcopy(self._cluster)

        if conf is not None:
            cluster_conf.update(conf)

        return cluster_conf
