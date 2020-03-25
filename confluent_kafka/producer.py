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

from .cimpl import Producer as _cProducer
from .serialization import (MessageField,
                            SerializationContext)


class SerializingProducer(_cProducer):
    """
    A high level Kafka Producer with serialization capabilities.

    The SerializingProducer is thread safe and sharing a single instance across
    threads will generally be faster than having multiple instances.

    The :py:func:`SerializingProducer.produce()` method is asynchronous.
    When called it adds the message to a queue of pending messages and
    immediately returns. This allows the Producer to batch together individual
    messages for efficiency.

    The Producer will automatically retry failed produce requests up to
    ``message.timeout.ms``.

    .. versionadded:: 1.0.0

        Setting ``enable.idempotence: True`` enables Idempotent Producer which
        provides guaranteed ordering and exactly-once producing.

    .. versionadded:: 1.4.0

        The Transactional Producer allows an application to send messages to
        multiple partitions (and topics) atomically.

        The ``key_serializer`` and ``value_serializer`` classes instruct the
        producer on how to convert the message payload to bytes.

    Args:
        conf (Config): Producer configuration

        conf (ClientConfig): Client configuration
            The following configurations are supported in addition to the ones
            described in Client Configurations(linked below).

            key.serializer (Serializer, optional): The serializer for key that
                implements Serializer

            value.serializer (Serializer, optional): The serializer for value
                that implements Serializer

            error_cb callable(KafkaError, optional): Callback for generic/global
                error events. These errors are typically to be considered
                informational since the client will automatically try to recover.
                This callback is served upon calling :py:func:`Producer.poll()`
                or :py:func:`Producer.flush()`

            log_cb (logging.Handler, optional): logging handle to forward logs to.
                To avoid spontaneous calls from non-Python threads the log messages
                will only be forwarded when :py:func:`Producer.poll()` or
                :py:func:`Producer.flush()` is called

            stats_cb (callable(str), optional): Callback for statistics data.
                This callback is triggered by :py:func:`Consumer.poll()` every
                ``statistics.interval.ms`` (needs to be configured separately).
                The str function argument is a str instance of a JSON document
                containing statistics data. This callback is served upon calling
                :py:func:`Producer.poll()` or :py:func:`Producer.flush()`

            throttle_cb (callable(ThrottleEvent), optional): Callback for
                throttled request reporting. This callback is served upon
                calling :py:func:`SerializingProducer.poll()` or
                :py:func:`SerializingProducer.flush()`

    .. _Client Configurations not listed above:
        https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md

    .. _Statistics:
        https://github.com/edenhill/librdkafka/blob/master/STATISTICS.md

    """
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
        called from :py:func:`SerializingProducer.poll()` when the message has
        been successfully delivered or permanently fails delivery.

        Currently message headers are not supported on the message returned to
        the callback. The ``msg.headers()`` will return None even if the
        original message had headers set.

        Args:
            topic (str): topic to produce message to

            key (object, optional): Message key

            value (object, optional): Message payload

            partition (int, optional): Partition to produce to, else uses the
                configured built-in partitioner

            on_delivery (callable(KafkaError, Message), optional): Delivery
                report callback to call (from
                :py:func:`SerializingProducer.poll()` or
                :py:func:`SerializingProducer.flush()` ) on successful or
                failed delivery

            timestamp (float, optional): Message timestamp (CreateTime) in ms
                since epoch UTC (requires broker >= 0.10.0.0). Default value
                is current time.

            headers (dict, optional): Message headers to set on the message.
                The header key must be a str while the value must be binary,
                unicode or None. (Requires broker version >= 0.11.0.0)

        Raises:
            BufferError: if the internal producer message queue is full.
                (``queue.buffering.max.messages`` exceeded). If this happens
                the application should call :py:func:`SerializingProducer.Poll`
                and try again.

             KafkaException: for other errors, see exception code

        """
        ctx = SerializationContext(topic, MessageField.KEY)
        if self._key_serializer:
            key = self._key_serializer(key, ctx)

        ctx.field = MessageField.VALUE
        if self._value_serializer:
            value = self._value_serializer(value, ctx)

        if headers is not None:
            super(SerializingProducer, self).produce(topic, value, key,
                                                     partition=partition,
                                                     timestamp=timestamp,
                                                     on_delivery=on_delivery)
        else:
            super(SerializingProducer, self).produce(topic, value, key,
                                                     partition=partition,
                                                     timestamp=timestamp,
                                                     on_delivery=on_delivery)
