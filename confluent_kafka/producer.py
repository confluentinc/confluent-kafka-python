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
        Create a new Kafka Producer instance with or without serializer support.

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

    def __new__(cls, *args, **kwargs):
        if 'key_serializer' in kwargs or 'value_serializer' in kwargs:
            return super(Producer, cls).__new__(SerializingProducer, *args, **kwargs)
        return super(Producer, cls).__new__(cls, *args, **kwargs)


class SerializingProducer(Producer):
    """
        SerializingProducer extends Producer with configurable key and value serializer.

        To avoid spontaneous calls from non-Python threads all callbacks will only be served upon
            calling ```client.poll()``` or ```client.flush()```.

        Instances of SerializingProducer cannot be created directly.
        To obtain an instance of this class instantiate a Consumer with a key and/or value deserializer.

        Duplicate params have been omitted for brevity. See Consumer for class documentation.

        :raises TypeError: If conf is not a dict.
        :raises TypeError: If instantiated directly.
    """

    __slots__ = ["_key_serializer", "_value_serializer"]

    def __new__(cls, *args, **kwargs):
        raise TypeError("SerializingProducer is a non user-instantiable class")

    @staticmethod
    def byteSerializer(topic, data):
        """ Pass-through serializer """
        return data

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

        # Ensure the default serializer cannot be overwritten with None on instantiation
        if key_serializer is None:
            key_serializer = SerializingProducer.byteSerializer

        if value_serializer is None:
            value_serializer = SerializingProducer.byteSerializer

        self._key_serializer = key_serializer
        self._value_serializer = value_serializer

        # Callbacks can be set in the conf dict or *ideally* as parameters.
        # Raise a SyntaxError if a callback is set in both places.
        for var, name in [(logger, 'logger'), (error_cb, 'error_cb'),
                          (stats_cb, 'stats_cb'), (throttle_cb, 'throttle_cb')]:
            if all([var, conf.get(name, None)]):
                raise SyntaxError("{} parameter repeated".format(name))
            if var is None:
                var = conf.get(name, None)

        super(SerializingProducer, self).__init__(conf, error_cb=error_cb, stats_cb=stats_cb,
                                                  throttle_cb=throttle_cb, logger=logger)

    def produce(self, topic, value=None, key=None, partition=PARTITION_UA,
                on_delivery=None, callback=None, timestamp=0, headers=None):
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

        super(SerializingProducer, self).produce(topic, self._value_serializer(topic, value), self._key_serializer(topic, key), partition, on_delivery=on_delivery,
                                                 callback=callback, timestamp=timestamp, headers=headers)
