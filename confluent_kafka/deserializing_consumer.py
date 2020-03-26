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
from .error import ConsumeError
from .serialization import (SerializationError,
                            SerializationContext,
                            MessageField)


class DeserializingConsumer(_cConsumer):
    """
    A client that consumes records from a Kafka cluster. With deserialization
    capabilities.

    Note:
        The DeserializingConsumer is an experimental API and subject to change
        between now and its eventual promotion to GA.

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
                :py:func:`Consumer.poll()`

            log_cb (logging.Handler, optional): logging handle to forward logs
                to. To avoid spontaneous calls from non-Python threads the log
                messages will only be forwarded when :py:func:`Consumer.poll()`
                is called.

            stats_cb (callable(str), optional): Callback for statistics data.
                This callback is triggered by :py:func:`Consumer.poll()` every
                ``statistics.interval.ms`` (needs to be configured separately).
                The str function argument is a str instance of a JSON formatted
                string containing statistics data. This callback is served upon
                calling :py:func:`Producer.poll()` or
                :py:func:`Producer.flush()`

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
            raise ConsumeError(msg.error(), message=msg)

        ctx = SerializationContext(msg.topic(), MessageField.VALUE)
        value = None
        if self._value_deserializer:
            try:
                value = self._value_deserializer(msg.value(), ctx)
            except SerializationError as se:
                raise ConsumeError(KafkaError._VALUE_DESERIALIZATION,
                                   reason=se.message,
                                   message=msg)
        key = None
        if self._key_deserializer:
            try:
                ctx.field = MessageField.KEY
                key = self._key_deserializer(msg.key(), ctx)
            except SerializationError as se:
                raise ConsumeError(KafkaError._KEY_DESERIALIZATION,
                                   reason=se.message,
                                   message=msg)

        msg.set_key(key)
        msg.set_value(value)
        return msg

    def consume(self, num_messages=1, timeout=-1):
        """:py:func:`Consumer.consume` not implemented"""
        raise NotImplementedError
