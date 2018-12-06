#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
#
# Copyright 2018 Confluent Inc.
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

from .cimpl import Producer as _impl, PARTITION_UA
from warnings import warn


class Producer(_impl):
    """
        Create a new Kafka Producer instance.

        To avoid spontaneous calls from non-Python threads all callbacks will only be served upon
            calling ```client.poll()``` or ```client.flush()```.

        :param dict conf: Configuration properties. At a minimum ``bootstrap.servers`` **should** be set.
            See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md for more information.
        :param func key_serializer(topic, key): Converts key to bytes.
            **note** serializers are responsible for handling NULL keys
        :param func value_serializer(topic, value): Converts value to bytes.
            **note** serializers are responsible for handling NULL keys
        :param func error_cb(kafka.KafkaError): Callback for generic/global error events.
        :param func stats_cb(json_str): Callback for statistics emitted every ``statistics.interval.ms``.
            See https://github.com/edenhill/librdkafka/wiki/Statistics” for more information.
        :param func throttle_cb(confluent_kafka.ThrottleEvent): Callback for throttled request reporting.
        :param logging.handlers logger: Forwards logs from the Kafka client to the provided handler instance.
            Log messages will only be forwarded when ``client.poll()`` or ``producer.flush()`` are called.
        :raises TypeError: If conf is not a dict object.
    """

    __slots__ = ["_key_serializer", "_value_serializer"]

    # conf must remain optional as long as kwargs are supported
    def __init__(self, conf={}, key_serializer=None, value_serializer=None,
                 error_cb=None, stats_cb=None, throttle_cb=None, logger=None, **kwargs):

        if not isinstance(conf, dict):
            raise TypeError("expected configuration dict")

        if kwargs:
            # Handle kwargs for backwards compatibility
            conf.update(kwargs)
            warn("The use of kwargs is being deprecated. "
                 "In future releases `conf` will be mandatory and "
                 "all keyword arguments must match the constructor signature explicitly.",
                 category=DeprecationWarning, stacklevel=2)

        self._key_serializer = key_serializer
        self._value_serializer = value_serializer

        # Callbacks can be set in the conf dict or *ideally* as parameters.
        # Handle both cases prior to passing along to _impl
        # If callbacks are configured in both places parameter values take precedence.
        if not error_cb:
            error_cb = conf.get('error_cb', None)

        if not stats_cb:
            stats_cb = conf.get('stats_cb', None)

        if not throttle_cb:
            throttle_cb = conf.get('throttle_cb', None)

        if not logger:
            logger = conf.get('logger', None)

        super(Producer, self).__init__(conf, error_cb=error_cb, stats_cb=stats_cb,
                                       throttle_cb=throttle_cb, logger=logger)

    def produce(self, topic, value=None, key=None, partition=PARTITION_UA,
                on_delivery=None, callback=None, timestamp=0, headers=None,
                key_serializer=None, value_serializer=None):
        """
            Produces message to Kafka

            :param str topic: Topic to produce message to.
            :param str|bytes|obj value: Message payload; value_serializer required for non-character values.
            :param str|bytes|obj key: Message key payload; key_serializer required for non-character keys.
            :param int partition: Partition to produce to, else uses the configured built-in partitioner.
                Default value is PARTITION_UA(round-robin over all partitions).
            :param func on_delivery(confluent_kafka.KafkaError, confluent_kafka.Message):
                Delivery report callback to call on successful or failed delivery.
            :param func callback(confluent_kafka.KafkaError, confluent_kafka.Message):
                See on_delivery.
            :param int timestamp: Message timestamp (CreateTime) in milliseconds since epoch UTC
                (requires librdkafka >= v0.9.4, api.version.request=true, and broker >= 0.10.0.0).
                Default value is current time.
            :param dict|list headers: Message headers to set on the message. The header key must be a string while
                the value must be binary, unicode or None. Accepts a list of (key,value) or a dict.
                (Requires librdkafka >= v0.11.4 and broker version >= 0.11.0.0)
            :param func key_serializer(topic, key): Producer key_serializer override;
                Converts message key to bytes. **note** serializers are responsible for handling NULL keys
            :param func value_serializer(topic, value): Producer value_serializer override;
                Converts message value to bytes. **note** serializers are responsible for handling NULL values

        """

        # on_delivery is an alias for callback and take precedence
        if callback and not on_delivery:
            on_delivery = callback

        # parameter overrides take precedence over instance functions
        if not key_serializer:
            key_serializer = self._key_serializer

        if key_serializer:
            key = key_serializer(topic, key)

        if not value_serializer:
            value_serializer = self._value_serializer

        if value_serializer:
            value = value_serializer(topic, value)

        super(Producer, self).produce(topic, value, key, partition, on_delivery=on_delivery,
                                      timestamp=timestamp, headers=headers)
