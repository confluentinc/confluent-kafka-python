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

    At a minimum both ``bootstrap.servers`` and ``group.id`` must be set.

    For detailed information about these settings and others see

    .. _Client Configurations not listed above:
        https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md

    Args:
        conf (dict): Client configuration
            The following configurations are supported in addition to the ones
            described in Client Configurations(linked below).

            key.deserializer (Deserializer): The deserializer for message keys.

            value.deserializer (Deserializer): The deserializer for message
                values.

            error_cb(callable(KafkaError), optional): Callback for
                generic/global error events. These errors are typically to be
                considered informational since the client will automatically try
                to recover. This callback is served upon calling
                :py:func:`DeserializingConsumer.poll()`

            log_cb (logging.Handler, optional): logging handler to forward logs
                to. To avoid spontaneous calls from non-Python threads the log
                messages will only be forwarded when
                :py:func:`DeserializingConsumer.poll()` is called.

            stats_cb (callable(str), optional): Callback for statistics data.
                This callback is triggered by
                :py:func:`DeserialializingConsumer.poll()` every
                ``statistics.interval.ms`` (needs to be configured separately).
                The str function argument is a string of a JSON formatted
                statistics data. This callback is served upon calling
                :py:func:`DeserializingConsumer.poll()`

            throttle_cb (callable(ThrottleEvent), optional): Callback for
                throttled request reporting. This callback is served upon
                calling :py:func:`DeserializingConsumer.poll()`.

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
                value = self._value_deserializer(value, ctx)
            except SerializationError as se:
                raise ConsumeError(KafkaError._VALUE_DESERIALIZATION,
                                   reason=se.message,
                                   message=msg)
        key = msg.key()
        if self._key_deserializer is not None:
            try:
                ctx.field = MessageField.KEY
                key = self._key_deserializer(key, ctx)
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
        :py:func:`DeserializingConsumer.poll()` instead
        """
        raise NotImplementedError
