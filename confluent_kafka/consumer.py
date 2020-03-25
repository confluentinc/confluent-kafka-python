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
                                   Consumer as _cConsumer)
from .error import ConsumeException
from .serialization import (SerializationError,
                            SerializationContext,
                            MessageField)


class DeserializingConsumer(_cConsumer):
    """
    A client that consumes records from a Kafka cluster. With Serialization
    capabilities.

    Args:
        conf (ClientConfig): Client configuration
            The following configurations are supported in addition to the ones
            described in Client Configurations(linked below).

            key.deserializer (Deserializer): Deserializes
                :py:func:`Message.key()` return value.

            value.deserializer (Deserializer): Deserializes
                :py:func:`Message.value()` return value.

            error_cb(callable(KafkaError), optional): Callback for
                generic/global error events. These errors are typically to be
                considered informational since the client will automatically try
                to recover. This callback is served upon calling
                :py:func:`Consumer.poll()`

            log_cb (logging.Handler, optional): logging handle to forward logs
                to. To avoid spontaneous calls from non-Python threads the log
                messages will only be forwarded when :py:func:`Consumer.poll()`
                is called.

            stats_cb (callable(str), optional): Callback for statistics data.
                This callback is triggered by :py:func:`Consumer.poll()` every
                ``statistics.interval.ms`` (needs to be configured separately).
                The str function argument is a str instance of a JSON document
                containing statistics data. This callback is served upon calling
                :py:func:`Consumer.poll()`.

            throttle_cb (callable(ThrottleEvent), optional): Callback for
                throttled request reporting. This callback is served upon
                calling :py:func:`Consumer.poll()`.

    Raises:
        ValueError: if configuration validation fails

    .. _Client Configurations not listed above:
        https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md

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

        Note:
            Callbacks may be called from this method, such as `on_assign``,
            ``on_revoke``, et.al. This will unblock
            :py:function:`Consumer.poll()` with a return value of None.

        Args:
            timeout (float): Maximum time to block waiting for message, event
                or callback. (Seconds)

        Returns:
            :py:class:`Message` or None on timeout

        Raises:
            ConsumeException if an error was encountered while polling.

        """
        msg = super(DeserializingConsumer, self).poll(timeout)

        if msg is None:
            return None

        if msg.error() is not None:
            raise ConsumeException(msg.error(), message=msg)

        ctx = SerializationContext(msg.topic(), MessageField.VALUE)
        if self._value_deserializer:
            try:
                msg.set_value(self._value_deserializer(msg.value(), ctx))
            except SerializationError as se:
                raise ConsumeException(KafkaError._VALUE_DESERIALIZATION,
                                       reason=se.message,
                                       message=msg)
        if self._key_deserializer:
            try:
                ctx.field = MessageField.KEY
                msg.set_key(self._key_deserializer(msg.key(), ctx))
            except SerializationError as se:
                raise ConsumeException(KafkaError._KEY_DESERIALIZATION,
                                       reason=se.message,
                                       message=msg)

        return msg

    def consume(self, num_messages=1, timeout=-1):
        """:py:func:`Consumer.consume` not implemented"""
        raise NotImplementedError
