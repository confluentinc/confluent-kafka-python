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

from confluent_kafka.cimpl import Producer as _ProducerImpl
from .serialization import (MessageField,
                            SerializationContext)
from .error import (KeySerializationError,
                    ValueSerializationError)


class SerializingProducer(_ProducerImpl):
    """
    A high level Kafka Producer with serialization capabilities.

    Note:

        The SerializingProducer is an experimental API and subject to change.

    The SerializingProducer is thread safe and sharing a single instance across
    threads will generally be more efficient than having multiple instances.

    The :py:func:`SerializingProducer.produce` method is asynchronous.
    When called it adds the message to a queue of pending messages and
    immediately returns. This allows the Producer to batch together individual
    messages for efficiency.

    The Producer will automatically retry failed produce requests up to
    ``message.timeout.ms`` .

    .. versionadded:: 1.4.0

        The Transactional Producer allows an application to send messages to
        multiple partitions (and topics) atomically.

        The ``key.serializer`` and ``value.serializer`` classes instruct the
        SerializingProducer on how to convert the message payload to bytes.

    Note:

        All configured callbacks are served from the application queue upon
        calling :py:func:`SerializingProducer.poll` or :py:func:`SerializingProducer.flush`

    Notable SerializingProducer configuration properties(* indicates required field)

    +-------------------------+---------------------+-----------------------------------------------------+
    | Property Name           | Type                | Description                                         |
    +=========================+=====================+=====================================================+
    | ``bootstrap.servers`` * | str                 | Comma-separated list of brokers.                    |
    +-------------------------+---------------------+-----------------------------------------------------+
    |                         |                     | Callable(obj, SerializationContext) -> bytes        |
    | ``key.serializer``      | callable            |                                                     |
    |                         |                     | Serializer used for message keys.                   |
    +-------------------------+---------------------+-----------------------------------------------------+
    |                         |                     | Callable(obj, SerializationContext) -> bytes        |
    | ``value.serializer``    | callable            |                                                     |
    |                         |                     | Serializer used for message values.                 |
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
    |                         |                     | Callback for throttled request reporting.           |
    +-------------------------+---------------------+-----------------------------------------------------+

    See Also:
        - `CONFIGURATION.md <https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md>`_ for additional configuration property details.
        - `STATISTICS.md <https://github.com/edenhill/librdkafka/blob/master/STATISTICS.md>`_ for detailed information about the statistics handled by stats_cb

    Args:
        conf (producer): SerializingProducer configuration.

    """  # noqa E501
    def __init__(self, conf):
        conf_copy = conf.copy()

        self._key_serializer = conf_copy.pop('key.serializer', None)
        self._value_serializer = conf_copy.pop('value.serializer', None)

        super(SerializingProducer, self).__init__(conf_copy)

    def produce(self, topic, key=None, value=None, partition=-1,
                on_delivery=None, timestamp=0, headers=None):
        """
        Produce message to topic.

        This is an asynchronous operation, an application may use the
        ``on_delivery`` argument to pass a function (or lambda) that will be
        called from :py:func:`SerializingProducer.poll` when the message has
        been successfully delivered or permanently fails delivery.

        Currently message headers are not supported on the message returned to
        the callback. The ``msg.headers()`` will return None even if the
        original message had headers set.

        Args:
            topic (str): Topic to produce message to.

            key (object, optional): Message key.

            value (object, optional): Message payload.

            partition (int, optional): Partition to produce to, else uses the
                configured built-in partitioner.

            on_delivery (callable(KafkaError, Message), optional): Delivery
                report callback to call (from
                :py:func:`SerializingProducer.poll` or
                :py:func:`SerializingProducer.flush` on successful or
                failed delivery.

            timestamp (float, optional): Message timestamp (CreateTime) in ms
                since epoch UTC (requires broker >= 0.10.0.0). Default value
                is current time.

            headers (dict, optional): Message headers to set on the message.
                The header key must be a str while the value must be binary,
                unicode or None. (Requires broker version >= 0.11.0.0)

        Raises:
            BufferError: if the internal producer message queue is full.
                ( ``queue.buffering.max.messages`` exceeded). If this happens
                the application should call :py:func:`SerializingProducer.Poll`
                and try again.

            KeySerializationError: If an error occurs during key serialization.

            ValueSerializationError: If an error occurs during value
            serialization.

            KafkaException: For all other errors

        """
        ctx = SerializationContext(topic, MessageField.KEY)
        if self._key_serializer is not None:
            try:
                key = self._key_serializer(key, ctx)
            except Exception as se:
                raise KeySerializationError(se)
        ctx.field = MessageField.VALUE
        if self._value_serializer is not None:
            try:
                value = self._value_serializer(value, ctx)
            except Exception as se:
                raise ValueSerializationError(se)

        super(SerializingProducer, self).produce(topic, value, key,
                                                 headers=headers,
                                                 partition=partition,
                                                 timestamp=timestamp,
                                                 on_delivery=on_delivery)
