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

    __slots__ = ["_key_deserializer", "_value_deserializer"]

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

        self._key_deserializer = key_deserializer
        self._value_deserializer = value_deserializer

        # Callbacks can be set in the conf dict or *ideally* as parameters.
        # Handle both cases prior to passing along to _impl
        # If callbacks are configured in both places parameter values take precedence.
        if not on_commit:
            on_commit = conf.get('on_commit', None)

        if not stats_cb:
            stats_cb = conf.get('stats_cb', None)

        if not throttle_cb:
            throttle_cb = conf.get('throttle_cb', None)

        if not logger:
            logger = conf.get('logger', None)
        super(Consumer, self).__init__(conf, on_commit=on_commit, stats_cb=stats_cb,
                                       throttle_cb=throttle_cb, logger=logger)

    def poll(self, timeout=-1.0, key_deserializer=None, value_deserializer=None):
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

        msg = super(Consumer, self).poll(timeout)

        if not msg or msg.error():
            return msg

        topic = msg.topic()

        # parameter overrides take precedence over instance functions
        if not key_deserializer:
            key_deserializer = self._key_deserializer

        if key_deserializer:
            msg.set_key(key_deserializer(topic, msg.key()))

        if not value_deserializer:
            value_deserializer = self._value_deserializer

        if value_deserializer:
            msg.set_value(value_deserializer(topic, msg.value()))

        return msg

    def consume(self, num_messages=1, timeout=-1):
        """
            Consume messages, calls callbacks and returns a list of messages. (possibly empty on timeout)

            The application must check Message.error() to distinguish between
                proper messages, an error(see error().code() for specifics), or an event. for each
                Message in the list.

            :param int num_messages: Maximum number of messages to return (default: 1)
            :param float timeout: Maximum time in seconds to block waiting for message, event or callback.
                (default: infinite (-1))
            :returns: A list of Message objects (possibly empty on timeout)
            :rtype: list(Message)
            :raises NotImplementedError: If used with key/value serializers.
            :raises RuntimeError: If called on a closed consumer.
            :raises KafkaError: In case of internal error.
            :raises ValueError: If num_messages > 1M.
        """

        # Disable consume() method when serializers are in use.
        if self._key_deserializer or self._value_deserializer:
            raise(NotImplementedError, "Batch consumption does not support the use of deserializers")

        return super(Consumer, self).consume(num_messages, timeout)
