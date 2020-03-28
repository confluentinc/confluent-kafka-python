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

from confluent_kafka.cimpl import (KafkaError,
                                   Consumer as _ConsumerImpl)
from .error import ConsumeError
from .serialization import (SerializationError,
                            SerializationContext,
                            MessageField)


class DeserializingConsumer(_ConsumerImpl):
    """
    A client that consumes records from a Kafka cluster. With deserialization
    capabilities.

    Note:

        The DeserializingConsumer is an experimental API and subject to change.

    .. versionadded:: 1.4.0

        The ``key.deserializer`` and ``value.deserializer`` classes instruct the
        DeserializingConsumer on how to convert the message payload bytes to objects.

    Note:

        All configured callbacks are served from the application queue upon
        calling :py:func:`DeserializingConsumer.poll`

    DeserializingConsumer configuration properties(* indicates required field)

    +--------------------+-----------------+-----------------------------------------------------+
    | Property Name      | Type            | Description                                         |
    +====================+=================+=====================================================+
    | bootstrap.servers* | str             | Comma-separated list of brokers.                    |
    +--------------------+-----------------+-----------------------------------------------------+
    |                    |                 | Client group id string.                             |
    | group.id*          | str             | All clients sharing the same group.id belong to the |
    |                    |                 | same group.                                         |
    +--------------------+-----------------+-----------------------------------------------------+
    |                    |                 | Callable(SerializationContext, bytes) -> obj        |
    | key.deserializer   | callable        |                                                     |
    |                    |                 | Serializer used for message keys.                   |
    +--------------------+-----------------+-----------------------------------------------------+
    |                    |                 | Callable(SerializationContext, bytes) -> obj        |
    | value.deserializer | callable        |                                                     |
    |                    |                 | Serializer used for message values.                 |
    +--------------------+-----------------+-----------------------------------------------------+
    |                    |                 | Callable(KafkaError)                                |
    |                    |                 |                                                     |
    | error_cb           | callable        | Callback for generic/global error events. These     |
    |                    |                 | errors are typically to be considered informational |
    |                    |                 | since the client will automatically try to recover. |
    +--------------------+-----------------+-----------------------------------------------------+
    | log_cb             | logging.Handler | Logging handler to forward logs                     |
    +--------------------+-----------------+-----------------------------------------------------+
    |                    |                 | Callable(str)                                       |
    |                    |                 |                                                     |
    |                    |                 | Callback for statistics. This callback is           |
    | stats_cb           | callable        | added to the application queue every                |
    |                    |                 | ``statistics.interval.ms`` (configured separately). |
    |                    |                 | The function argument is a JSON formatted str       |
    |                    |                 | containing statistics data.                         |
    +--------------------+-----------------+-----------------------------------------------------+
    |                    |                 | Callable(ThrottleEvent)                             |
    | throttle_cb        | callable        |                                                     |
    |                    |                 | Callback for throttled request reporting.           |
    +--------------------+-----------------+-----------------------------------------------------+

    .. _See Client CONFIGURATION.md for a complete list of configuration properties:
        https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md

    Args:
        conf (dict): DeserializingConsumer configuration.

    Raises:
        ValueError: if configuration validation fails

    .. _Statistics:
        https://github.com/edenhill/librdkafka/blob/master/STATISTICS.md

    """

    def __init__(self, conf):
        conf_copy = conf.copy()
        self._key_deserializer = conf_copy.pop('key.deserializer', None)
        self._value_deserializer = conf_copy.pop('value.deserializer', None)

        super(DeserializingConsumer, self).__init__(conf_copy)

    def poll(self, timeout=-1):
        """
        Consume messages and calls callbacks.

        Args:
            timeout (float): Maximum time to block waiting for message(Seconds).

        Returns:
            :py:class:`Message` or None on timeout

        Raises:
            ConsumeError if an error was encountered while polling.

        """
        msg = super(DeserializingConsumer, self).poll(timeout)

        if msg is None:
            return None

        if msg.error() is not None:
            raise ConsumeError(msg.error(), message=msg)

        ctx = SerializationContext(msg.topic(), MessageField.VALUE)
        value = msg.value()
        if self._value_deserializer is not None:
            try:
                value = self._value_deserializer(ctx, value)
            except SerializationError as se:
                raise ConsumeError(KafkaError._VALUE_DESERIALIZATION,
                                   reason=se.message,
                                   message=msg)
        key = msg.key()
        if self._key_deserializer is not None:
            try:
                ctx.field = MessageField.KEY
                key = self._key_deserializer(ctx, key)
            except SerializationError as se:
                raise ConsumeError(KafkaError._KEY_DESERIALIZATION,
                                   reason=se.message,
                                   message=msg)

        msg.set_key(key)
        msg.set_value(value)
        return msg

    def consume(self, num_messages=1, timeout=-1):
        """
        :py:func:`Consumer.consume` not implemented,
        :py:func:`DeserializingConsumer.poll` instead
        """
        raise NotImplementedError
