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

from confluent_kafka.cimpl import Consumer as _ConsumerImpl
from .error import (ConsumeError,
                    KeyDeserializationError,
                    ValueDeserializationError)
from .serialization import (SerializationContext,
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

    Notable DeserializingConsumer configuration properties(* indicates required field)

    +-------------------------+---------------------+-----------------------------------------------------+
    | Property Name           | Type                | Description                                         |
    +=========================+=====================+=====================================================+
    | ``bootstrap.servers`` * | str                 | Comma-separated list of brokers.                    |
    +-------------------------+---------------------+-----------------------------------------------------+
    |                         |                     | Client group id string.                             |
    | ``group.id`` *          | str                 | All clients sharing the same group.id belong to the |
    |                         |                     | same group.                                         |
    +-------------------------+---------------------+-----------------------------------------------------+
    |                         |                     | Callable(SerializationContext, bytes) -> obj        |
    | ``key.deserializer``    | callable            |                                                     |
    |                         |                     | Deserializer used for message keys.                 |
    +-------------------------+---------------------+-----------------------------------------------------+
    |                         |                     | Callable(SerializationContext, bytes) -> obj        |
    | ``value.deserializer``  | callable            |                                                     |
    |                         |                     | Deserializer used for message values.               |
    +-------------------------+---------------------+-----------------------------------------------------+
    |                         |                     | Callable(KafkaError)                                |
    |                         |                     |                                                     |
    | ``error_cb``            | callable            | Callback for generic/global error events. These     |
    |                         |                     | errors are typically to be considered informational |
    |                         |                     | since the client will automatically try to recover. |
    +-------------------------+---------------------+-----------------------------------------------------+
    | ``log_cb``              | ``logging.Handler`` | Logging handler to forward logs                     |
    +-------------------------+---------------------+-----------------------------------------------------+
    |                         |                     | Callable(str)                                       |
    |                         |                     |                                                     |
    |                         |                     | Callback for statistics. This callback is           |
    | ``stats_cb``            | callable            | added to the application queue every                |
    |                         |                     | ``statistics.interval.ms`` (configured separately). |
    |                         |                     | The function argument is a JSON formatted str       |
    |                         |                     | containing statistics data.                         |
    +-------------------------+---------------------+-----------------------------------------------------+
    |                         |                     | Callable(ThrottleEvent)                             |
    | ``throttle_cb``         | callable            |                                                     |
    |                         |                     | Callback for throttled request reporting.           |
    +-------------------------+---------------------+-----------------------------------------------------+

    See Also:
        - `CONFIGURATION.md <https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md>`_ for additional configuration property details.
        - `STATISTICS.md <https://github.com/edenhill/librdkafka/blob/master/STATISTICS.md>`_ for detailed information about the statistics handled by stats_cb

    Args:
        conf (dict): DeserializingConsumer configuration.

    Raises:
        ValueError: if configuration validation fails

    """  # noqa: E501

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
            KeyDeserializationError: If an error occurs during key
            deserialization.

            ValueDeserializationError: If an error occurs during value
            deserialization.

            ConsumeError if an error was encountered while polling.

        """
        msg = super(DeserializingConsumer, self).poll(timeout)

        if msg is None:
            return None

        if msg.error() is not None:
            raise ConsumeError(msg.error(), kafka_message=msg)

        return self._parse_deserialize_message(msg)

    def consume(self, num_messages=1, timeout=-1):
        """
        Consume up to the number of messages specified with a timeout for each request

        Args:
            num_messages (int): The maximum number of messages to wait for.
            timeout (float): Maximum time to block waiting for message(Seconds).

        Returns:
            :py:class:`Message` or None on timeout

        Raises:
            KeyDeserializationError: If an error occurs during key
            deserialization.

            ValueDeserializationError: If an error occurs during value
            deserialization.

            ConsumeError if an error was encountered while polling.

            RuntimeError if the number of messages is less than 1
        """
        if num_messages < 1:
            raise RuntimeError("The maximum number of messages must be greater than or equal to 1.")

        messages = super(DeserializingConsumer, self).consume(num_messages, timeout)

        if messages is None:
            return []

        deserialized_messages = []

        for message in messages:
            if msg.error() is not None:
                raise ConsumeError(msg.error(), kafka_message=msg)

            deserialized_messages.append(self._parse_deserialize_message(message))

        return deserialized_messages


    def _parse_deserialize_message(message):
        """
        Internal class method for deserializing and maintaining consistency between poll and consume classes.

        This function will take in a raw serialized message (from cimpl) and return a deserialized message back.

        Args:
            message (cimpl.Message): The serialized message returned from the base consumer class.

        Returns:
            :py:class:`Message` on sucessful deserialization

        Raises:
            KeyDeserializationError: If an error occurs during key
            deserialization.

            ValueDeserializationError: If an error occurs during value
            deserialization.
        """
        ctx = SerializationContext(message.topic(), MessageField.VALUE)
        value = message.value()
        if self._value_deserializer is not None:
            try:
                value = self._value_deserializer(value, ctx)
            except Exception as se:
                raise ValueDeserializationError(exception=se, kafka_message=message)

        key = message.key()
        ctx.field = MessageField.KEY
        if self._key_deserializer is not None:
            try:
                key = self._key_deserializer(key, ctx)
            except Exception as se:
                raise KeyDeserializationError(exception=se, kafka_message=message)

        message.set_key(key)
        message.set_value(value)
        return message
