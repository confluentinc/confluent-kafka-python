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

import os
import time
from uuid import uuid1

from trivup.clusters.KafkaCluster import KafkaCluster

from confluent_kafka import Producer, SerializingProducer
from confluent_kafka.admin import (
    AdminClient,
    AlterConfigOpType,
    ConfigEntry,
    ConfigResource,
    NewTopic,
    ResourceType,
)
from confluent_kafka.schema_registry._async.schema_registry_client import AsyncSchemaRegistryClient
from confluent_kafka.schema_registry.schema_registry_client import SchemaRegistryClient
from tests.common import TestConsumer, TestShareConsumer
from tests.common._async.consumer import TestAsyncDeserializingConsumer
from tests.common._async.producer import TestAsyncSerializingProducer
from tests.common.schema_registry import TestDeserializingConsumer


def _apply_rdk_debug(client_conf):
    """Opt-in librdkafka client-side debug via the RDK_DEBUG env var.

    e.g. RDK_DEBUG=security surfaces the SASL/SCRAM handshake in the client
    log (vs. the broker's JVM logs). Honored by both the trivup and BYO
    fixtures. A 'debug' passed explicitly in a client conf still wins.
    """
    rdk_debug = os.environ.get('RDK_DEBUG')
    if rdk_debug:
        client_conf['debug'] = rdk_debug


class KafkaClusterFixture(object):
    __slots__ = ['_cluster', '_admin', '_producer']

    def __init__(self, conf):
        raise NotImplementedError("KafkaCluster should never be instantiated directly")

    def schema_registry(self, conf=None):
        raise NotImplementedError("schema_registry has not been implemented")

    def client_conf(self, conf=None):
        raise NotImplementedError("client_conf has not been implemented")

    @classmethod
    def _topic_conf(cls, conf=None):
        topic_conf = {'num_partitions': 1, 'replication_factor': -1}

        if conf is not None:
            topic_conf.update(conf)

        return topic_conf

    def cimpl_producer(self, conf=None):
        """
        Returns a producer bound to this cluster.

        Args:
            conf (dict): Producer configuration overrides

        Returns:
            Producer: A new Producer instance
        """
        return Producer(self.client_conf(conf))

    def producer(self, conf=None, key_serializer=None, value_serializer=None):
        """
        Returns a producer bound to this cluster.

        Args:
            conf (dict): Producer configuration overrides

            key_serializer (Serializer): serializer to apply to message key

            value_serializer (Deserializer): serializer to apply to
                message value

        Returns:
            Producer: A new SerializingProducer instance

        """
        client_conf = self.client_conf(conf)

        if key_serializer is not None:
            client_conf['key.serializer'] = key_serializer

        if value_serializer is not None:
            client_conf['value.serializer'] = value_serializer

        return SerializingProducer(client_conf)

    def async_producer(self, conf=None, key_serializer=None, value_serializer=None):
        """
        Returns a producer bound to this cluster.

        Args:
            conf (dict): Producer configuration overrides

            key_serializer (Serializer): serializer to apply to message key

            value_serializer (Deserializer): serializer to apply to
                message value

        Returns:
            Producer: A new SerializingProducer instance

        """
        client_conf = self.client_conf(conf)

        if key_serializer is not None:
            client_conf['key.serializer'] = key_serializer

        if value_serializer is not None:
            client_conf['value.serializer'] = value_serializer

        return TestAsyncSerializingProducer(client_conf)

    def cimpl_consumer(self, conf=None):
        """
        Returns a consumer bound to this cluster.

        Args:
            conf (dict): Consumer config overrides

        Returns:
            Consumer: A new Consumer instance

        """
        consumer_conf = self.client_conf({'group.id': str(uuid1()), 'auto.offset.reset': 'earliest'})

        if conf is not None:
            consumer_conf.update(conf)

        return TestConsumer(consumer_conf)

    def share_consumer(self, conf=None):
        """
        Returns a share consumer bound to this cluster.

        Args:
            conf (dict): ShareConsumer config overrides

        Returns:
            ShareConsumer: A new TestShareConsumer instance

        """
        share_conf = self.client_conf({'group.id': str(uuid1())})

        if conf is not None:
            share_conf.update(conf)

        # KIP-932: share.auto.offset.reset is a per-group broker-side config with default as "latest".
        # Set to "earliest" so tests don't have to set it in every call.
        # TODO KIP-932: this shouldn't live in the share_consumer fixture —
        # share.auto.offset.reset is a property of the group, not the consumer.
        # Re-issuing the alter on every consumer construction for the same
        # group.id is unnecessary. Lift this to a per-group setup step.
        reset = share_conf.pop('auto.offset.reset', 'earliest')
        # KIP-932: shorten the acquisition-lock duration per group so the
        # lock-expiry / redelivery tests don't wait on the 30s broker default.
        # Set per-group via AlterConfigs (mirrors librdkafka's
        # set_group_lock_duration) so it works against clusters where the
        # broker-static group.share.record.lock.duration.ms can't be set
        # (e.g. Confluent Cloud).
        lock_duration_ms = str(share_conf.pop('share.record.lock.duration.ms', 1000))
        res = ConfigResource(
            ResourceType.GROUP,
            share_conf['group.id'],
            incremental_configs=[
                ConfigEntry(
                    'share.auto.offset.reset',
                    reset,
                    incremental_operation=AlterConfigOpType.SET,
                ),
                ConfigEntry(
                    'share.record.lock.duration.ms',
                    lock_duration_ms,
                    incremental_operation=AlterConfigOpType.SET,
                ),
            ],
        )
        for f in self.admin().incremental_alter_configs([res]).values():
            f.result()

        return TestShareConsumer(share_conf)

    def consumer(self, conf=None, key_deserializer=None, value_deserializer=None):
        """
        Returns a consumer bound to this cluster.

        Args:
            conf (dict): Consumer config overrides

            key_deserializer (Deserializer): deserializer to apply to
                message key

            value_deserializer (Deserializer): deserializer to apply to
                message value

        Returns:
            Consumer: A new DeserializingConsumer instance

        """
        consumer_conf = self.client_conf({'group.id': str(uuid1()), 'auto.offset.reset': 'earliest'})

        if conf is not None:
            consumer_conf.update(conf)

        if key_deserializer is not None:
            consumer_conf['key.deserializer'] = key_deserializer

        if value_deserializer is not None:
            consumer_conf['value.deserializer'] = value_deserializer

        return TestDeserializingConsumer(consumer_conf)

    def async_consumer(self, conf=None, key_deserializer=None, value_deserializer=None):
        """
        Returns a consumer bound to this cluster.

        Args:
            conf (dict): Consumer config overrides

            key_deserializer (Deserializer): deserializer to apply to
                message key

            value_deserializer (Deserializer): deserializer to apply to
                message value

        Returns:
            Consumer: A new DeserializingConsumer instance

        """
        consumer_conf = self.client_conf({'group.id': str(uuid1()), 'auto.offset.reset': 'earliest'})

        if conf is not None:
            consumer_conf.update(conf)

        if key_deserializer is not None:
            consumer_conf['key.deserializer'] = key_deserializer

        if value_deserializer is not None:
            consumer_conf['value.deserializer'] = value_deserializer

        return TestAsyncDeserializingConsumer(consumer_conf)

    def admin(self, conf=None):
        if conf:
            # When conf is passed create a new AdminClient
            admin_conf = self.client_conf()
            admin_conf.update(conf)
            return AdminClient(admin_conf)

        # Otherwise use a common one
        if self._admin is None:
            self._admin = AdminClient(self.client_conf())
        return self._admin

    def create_topic(self, prefix, conf=None, **create_topic_kwargs):
        """
        Creates a new topic with this cluster.

        :param str prefix: topic name
        :param dict conf: additions/overrides to topic configuration.
        :returns: The topic's name
        :rtype: str
        """
        name = prefix + "-" + str(uuid1())
        future_topic = self.admin().create_topics([NewTopic(name, **self._topic_conf(conf))], **create_topic_kwargs)

        future_topic.get(name).result()
        return name

    def delete_topic(self, topic):
        """
        Deletes a topic with this cluster.

        :param str topic: topic name
        """
        future = self.admin().delete_topics([topic])
        try:
            future.get(topic).result()
            print("Topic {} deleted".format(topic))
        except Exception as e:
            print("Failed to delete topic {}: {}".format(topic, e))

    def create_topic_and_wait_propogation(self, prefix, conf=None, **create_topic_kwargs):
        """
        Creates a new topic with this cluster. Wait for the topic to be propogated to all brokers.

        :param str prefix: topic name
        :param dict conf: additions/overrides to topic configuration.
        :returns: The topic's name
        :rtype: str
        """
        name = self.create_topic(prefix, conf, **create_topic_kwargs)

        # wait for topic propogation across all the brokers.
        # FIXME: find a better way to wait for topic creation
        #        for all languages, given option to send request to
        #        a specific broker isn't present everywhere.
        time.sleep(1)

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
            self._producer = self.producer(self.client_conf({'linger.ms': 500}))

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
                        producer.produce(
                            topic,
                            value=value_source[i],
                            key=key_source[i % num_keys],
                            headers=header_source[i % (num_headers + 1)],
                        )
                    else:
                        producer.produce(topic, value=value_source[i], key=key_source[i % num_keys])
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
        # GSSAPI: trivup generates a krb5.conf + ccache and exposes them in
        # cluster.env, but the client (librdkafka) runs `kinit` and the GSSAPI
        # handshake in THIS process — so the KRB5_* vars must be in os.environ
        # or kinit can't resolve the realm and GSSAPI finds no credentials.
        # (No-op for non-Kerberos mechanisms, which set no KRB5_* env.)
        for key, value in self._cluster.env.items():
            if key.startswith('KRB5'):
                os.environ[key] = value

    def schema_registry(self, conf=None):
        if not hasattr(self._cluster, 'sr'):
            return None

        sr_conf = {'url': self._cluster.sr.get('url')}
        if conf is not None:
            sr_conf.update(conf)
        return SchemaRegistryClient(sr_conf)

    def async_schema_registry(self, conf=None):
        if not hasattr(self._cluster, 'sr'):
            return None

        sr_conf = {'url': self._cluster.sr.get('url')}
        if conf is not None:
            sr_conf.update(conf)
        return AsyncSchemaRegistryClient(sr_conf)

    def client_conf(self, conf=None):
        """
        Default client configuration

        :param dict conf: default client configuration overrides
        :returns: client configuration
        """
        client_conf = self._cluster.client_conf()
        _apply_rdk_debug(client_conf)
        # trivup derives a ~1s Kerberos relogin interval from its short
        # krb_renew_lifetime, so GSSAPI runs re-kinit (and log) every second.
        # Stretch it (default 60s, < the 120s ticket lifetime) to keep logs
        # readable; RDK_KERBEROS_RELOGIN_MS overrides.
        if 'sasl.kerberos.min.time.before.relogin' in client_conf:
            client_conf['sasl.kerberos.min.time.before.relogin'] = \
                int(os.environ.get('RDK_KERBEROS_RELOGIN_MS', '60000'))

        if conf is not None:
            client_conf.update(conf)

        return client_conf

    def stop(self):
        self._cluster.stop(cleanup=True)


class ByoFixture(KafkaClusterFixture):
    """
    A Kafka cluster fixture that assumes an already running cluster as specified
    by bootstrap.servers, and optionally schema.registry.url, in the conf dict.
    """

    def __init__(self, conf):
        if conf.get("bootstrap.servers", "") == "":
            raise ValueError("'bootstrap.servers' must be set in the " "conf dict")
        self._admin = None
        self._producer = None
        self._conf = conf.copy()
        self._sr_url = self._conf.pop("schema.registry.url", None)

    def schema_registry(self, conf=None):
        if self._sr_url is None:
            raise RuntimeError("No Schema-registry available in Byo cluster")
        return SchemaRegistryClient({"url": self._sr_url})

    def client_conf(self, conf=None):
        """
        The client configuration
        """
        client_conf = self._conf.copy()
        _apply_rdk_debug(client_conf)
        if conf is not None:
            client_conf.update(conf)

        return client_conf

    def stop(self):
        pass
