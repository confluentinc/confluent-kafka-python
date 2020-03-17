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

from confluent_kafka.cimpl import KafkaError

from .cimpl import Consumer as _cConsumer
from .error import ConsumeException
from .serialization import SerializationError


class SerializingConsumer(_cConsumer):
    """
    A client that consumes records from a Kafka cluster. With Serialization
    capabilities.

    Keyword Args:
        conf (ClientConfig): Client configuration
            The following configurations are supported in addition to the ones
            described in Client Configurations(linked below).

            - key.deserializer (Serializer): Serde instance to deserialize message
                key contents.
            - value.deserializer (Serializer): Serde instance to deserailize message
                value contents.
            - error_cb(callable(KafkaError), optional): Callback for generic/global
                error events. These errors are typically to be considered
                informational since the client will automatically try to recover.
                This callback is served upon calling :py:func:`Consumer.poll()`
            - log_cb (logging.Handler, optional): logging handle to forward logs to.
                To avoid spontaneous calls from non-Python threads the log messages
                will only be forwarded when :py:func:`Consumer.poll()` is called.
            - stats_cb (callable(str), optional): Callback for statistics data. This
                callback is triggered by :py:func:`Consumer.poll()` every
                ``statistics.interval.ms`` (needs to be configured separately).
                The str function argument is a str instance of a JSON document
                containing statistics data. This callback is served upon calling
                :py:func:`Consumer.poll()`.
            - throttle_cb (callable(ThrottleEvent), optional): Callback for throttled
                request reporting. This callback is served upon calling
                :py:func:`Consumer.poll()`.

    Raises:
        ValueError if configured incorrectly

    .. _Client Configurations not listed above:
        https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md

    .. _Statistics:
        https://github.com/edenhill/librdkafka/wiki/Statistics
    """

    def __init__(self, conf):
        self._key_deserializer = conf.KeyDeserializer
        self._value_deserializer = conf.ValueDeserializer

        super(SerializingConsumer, self).__init__(conf.Config)

    def poll(self, timeout=-1):
        """
        Consume messages, calls callbacks and returns events.

        The application must check the returned :py:class:`Message` object's
        :py:func:`Message.error()` method to distinguish between proper messages
        (error() returns None), or an event or error (see error().code() for
        specifics)

        .. note: Callbacks may be called from this method, such as
            ``on_assign``, ``on_revoke``, et.al

        Args:
            timeout (float): Maximum time to block waiting for message, event
                or callback. (Seconds)

        Returns:
            :py:class:`Message` or None on timeout

        Raises:
            ConsumeException if an error occurred
        """

        msg = super(SerializingConsumer, self).poll(timeout)

        if msg is None:
            return None

        if msg.error() is not None:
            raise ConsumeException(msg.error(), message=msg)

        try:
            if self._value_deserializer:
                msg.set_value(self._value_deserializer(msg.value(), None))
        except SerializationError as se:
            raise ConsumeException(KafkaError._VALUE_DESERIALIZATION,
                                   error_message=se.message,
                                   message=msg)
        try:
            if self._key_deserializer:
                msg.set_key(self._key_deserializer(msg.key(), None))
        except SerializationError as se:
            raise ConsumeException(KafkaError._KEY_DESERIALIZATION,
                                   error_message=se.message,
                                   message=msg)

        return msg

    def consume(self, num_messages=1, timeout=-1):
        """
        Consumer.consume is not supported by the SerializingConsumer.

        Args:
            num_messages (int): Maximum number of messages to return
            timeout (float):  Maximum time to block waiting for message, event
                or callback. (Seconds)

        Returns:
            list(Message), possibly None if  timeout is exceeded.
        """
        raise NotImplementedError
