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

from .cimpl import Consumer as _impl
from warnings import warn


class Consumer(_impl):
    """
        Create a new Kafka Consumer instance.

        To avoid spontaneous calls from non-Python threads all callbacks will only be served upon
        calling ```client.poll()``` or ```client.flush()```.

        :param dict conf: Configuration properties.
            See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md for more information.
        :param func key_deserializer(topic, key): Converts message key bytes to object.
            **note** deserializers are responsible for handling NULL keys
        :param func value_deserializer(topic, value): Converts message value bytes to object.
            **note** deserializers are responsible for handling NULL values
        :param func on_commit(err, [partitions]): Callback used to indicate success or failure
            of an offset commit.
        :param func stats_cb(json_str): Callback for statistics emitted every ``statistics.interval.ms``.
            See https://github.com/edenhill/librdkafka/wiki/Statistics” for more information.
        :param func throttle_cb(confluent_kafka.ThrottleEvent): Callback for throttled request reporting.
        :param logging.handlers logger: Forwards logs from the Kafka client to the provided handler instance.
            Log messages will only be forwarded when ``client.poll()`` or ``producer.flush()`` are called.
        :raises TypeError: If conf is not a dict.
    """
    def __new__(cls, *args, **kwargs):
        if 'key_deserializer' in kwargs or 'value_deserializer' in kwargs:
            return super(Consumer, cls).__new__(SerializingConsumer, *args, **kwargs)
        return super(Consumer, cls).__new__(cls, *args, **kwargs)


class SerializingConsumer(Consumer):
    """
        SerializingConsumer extends Consumer with configurable key and value deserializer.

        Instances of SerializingConsumer cannot be created directly.
        To obtain an instance of this class instantiate a Consumer with a key and/or value deserializer.

        Duplicate params have been omitted for brevity. See Consumer for class documentation.

        :raises TypeError: If conf is not a dict.
        :raises TypeError: If instantiated directly.
    """

    __slots__ = ["_key_deserializer", "_value_deserializer"]

    def __new__(cls, *args, **kwargs):
        raise TypeError("SerializingConsumer is a non user-instantiable class")

    @staticmethod
    def byteSerializer(topic, data):
        """ Pass-through serializer """
        return data

    # conf must remain optional as long as kwargs are supported
    def __init__(self, conf={}, key_deserializer=None, value_deserializer=None,
                 on_commit=None, stats_cb=None, throttle_cb=None, logger=None, **kwargs):

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
        if key_deserializer is None:
            key_deserializer = SerializingConsumer.byteSerializer

        if value_deserializer is None:
            value_deserializer = SerializingConsumer.byteSerializer

        self._key_deserializer = key_deserializer
        self._value_deserializer = value_deserializer

        # Callbacks can be set in the conf dict or *ideally* as parameters.
        # Raise a SyntaxError if a callback is set in both places.
        for var, name in [(logger, 'logger'), (on_commit, 'on_commit'),
                          (stats_cb, 'stats_cb'), (throttle_cb, 'throttle_cb')]:
            if all([var, conf.get(name, None)]):
                raise SyntaxError("{} parameter repeated".format(name))
            if var is None:
                var = conf.get(name, None)

        super(SerializingConsumer, self).__init__(conf, on_commit=on_commit, stats_cb=stats_cb,
                                                  throttle_cb=throttle_cb, logger=logger)

    def poll(self, timeout=-1.0):
        """
            Consumes a message, triggers callbacks, returns an event.

            The application must check the returned Message object’s Message.error() method to distinguish
            between proper messages, an error(see error().code() for specifics), or an event.

            :param float timeout:  Maximum time in seconds to block waiting for message, event or callback.
            :param func key_deserializer(topic, key): Converts message key bytes to object.
                **note** deserializers are responsible for handling NULL keys
            :param func value_deserializer(topic, value): Converts message value bytes to object.
                **note** deserializers are responsible for handling NULL values
            :returns: A confluent_kafka.Message or None on timeout.
            :raises RuntimeError: If called on a closed consumer.
        """

        msg = super(SerializingConsumer, self).poll(timeout)

        if msg is None or msg.error():
            return msg

        topic = msg.topic()

        msg.set_key(self._key_deserializer(topic, msg.key()))
        msg.set_value(self._value_deserializer(topic, msg.value()))

        return msg

    def consume(self, num_messages=1, timeout=-1):
        """
            Consume messages, calls callbacks and returns a list of messages. (possibly empty on timeout)

            The application must check Message.error() to distinguish between
                proper messages, an error(see error().code() for specifics), or an event for each
                Message in the list.

            :param int num_messages: Maximum number of messages to return (default: 1)
            :param float timeout: Maximum time in seconds to block waiting for message, event or callback.
                (default: infinite (-1))
            :returns: A list of Message objects (possibly empty on timeout)
            :rtype: list(Message)
            :raises RuntimeError: If called on a closed consumer.
            :raises KafkaError: In case of internal error.
            :raises ValueError: If num_messages > 1M.
        """

        msgset = super(SerializingConsumer, self).consume(num_messages, timeout)
        for msg in msgset:
            if msg.error():
                continue

            msg.set_key(self._key_deserializer(msg.topic(), msg.key()))
            msg.set_value(self._value_deserializer(msg.topic(), msg.value()))

        return msgset
