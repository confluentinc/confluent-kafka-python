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

from typing import TYPE_CHECKING, Any, Dict, Generic, List, Optional

if TYPE_CHECKING:
    # PEP 696 defaults, so an unparameterized DeserializingConsumer(conf) still
    # infers as before instead of erroring with "Need type annotation". They
    # only mean anything to a type checker, and type checkers always have
    # typing_extensions available, so the runtime below keeps plain TypeVars
    # and this costs no dependency on any Python version.
    from typing_extensions import TypeVar

    K = TypeVar("K", default=Any)
    V = TypeVar("V", default=Any)
else:
    from typing import TypeVar

    K = TypeVar("K")
    V = TypeVar("V")

from confluent_kafka.cimpl import Consumer as _ConsumerImpl
from confluent_kafka.cimpl import Message

from ._serde_builder import pop_serdes, propagate_cluster_id
from .error import ConsumeError, KeyDeserializationError, ValueDeserializationError
from .serialization import MessageField, SerializationContext


class DeserializingConsumer(_ConsumerImpl, Generic[K, V]):
    """
    A high level Kafka consumer with deserialization capabilities.

    `This class is experimental and likely to be removed, or subject to incompatible API
    changes in future versions of the library. To avoid breaking changes on upgrading, we
    recommend using deserializers directly.`

    Derived from the :py:class:`Consumer` class, overriding the :py:func:`Consumer.poll`
    method to add deserialization capabilities.

    Additional configuration properties:

    +--------------------------------+---------------------+--------------------------------------------+
    | Property Name                  | Type                | Description                                |
    +================================+=====================+============================================+
    |                                |                     | Callable(bytes, SerializationContext)      |
    | ``key.deserializer``           | callable            | -> obj                                     |
    |                                |                     | Deserializer used for message keys.        |
    +--------------------------------+---------------------+--------------------------------------------+
    |                                |                     | Callable(bytes, SerializationContext)      |
    | ``value.deserializer``         | callable            | -> obj                                     |
    |                                |                     | Deserializer used for message values.      |
    +--------------------------------+---------------------+--------------------------------------------+
    |                                |                     | DeserializerBuilder building the key       |
    | ``key.deserializer.builder``   | DeserializerBuilder | deserializer, as an alternative to passing |
    |                                |                     | a ready-made one in ``key.deserializer``.  |
    +--------------------------------+---------------------+--------------------------------------------+
    |                                |                     | DeserializerBuilder building the value     |
    | ``value.deserializer.builder`` | DeserializerBuilder | deserializer, as an alternative to passing |
    |                                |                     | one in ``value.deserializer``.             |
    +--------------------------------+---------------------+--------------------------------------------+

    Deserializers for string, integer and double (:py:class:`StringDeserializer`, :py:class:`IntegerDeserializer`
    and :py:class:`DoubleDeserializer`) are supplied out-of-the-box in the ``confluent_kafka.serialization``
    namespace.

    Deserializers for Protobuf, JSON Schema and Avro (:py:class:`ProtobufDeserializer`, :py:class:`JSONDeserializer`
    and :py:class:`AvroDeserializer`) with Confluent Schema Registry integration are supplied out-of-the-box
    in the ``confluent_kafka.schema_registry`` namespace, each with a matching builder
    (:py:class:`AvroDeserializerBuilder` and friends) that constructs the Schema Registry client for you::

        consumer = DeserializingConsumer[str, User]({
            'bootstrap.servers': brokers,
            'group.id': group,
            'value.deserializer.builder': AvroDeserializerBuilder()
                .set_schema_registry_config({'url': schema_registry_url})
                .set_from_dict(dict_to_user),
        })

    A builder is also what allows a deserializer to be given the Kafka cluster id: deserializers that
    need it (those resolving subjects through the Schema Registry associated subject name strategy
    without an explicit ``subject.name.strategy.kafka.cluster.id``) have it fetched from the broker and
    supplied during construction.

    The class is generic in the deserialized key and value types, which parameterize the messages it
    yields: on a ``DeserializingConsumer[str, User]``, :py:func:`Message.deserialized_value` is typed
    as ``Optional[User]``. Left unparameterized both are Any.

    See Also:
        - The :ref:`Configuration Guide <pythonclient_configuration>` for in depth information on how to configure the client.
        - `CONFIGURATION.md <https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md>`_ for a comprehensive set of configuration properties.
        - `STATISTICS.md <https://github.com/edenhill/librdkafka/blob/master/STATISTICS.md>`_ for detailed information on the statistics provided by stats_cb
        - The :py:class:`Consumer` class for inherited methods.

    Args:
        conf (dict): DeserializingConsumer configuration.

    Raises:
        ValueError: if configuration validation fails, or if a deserializer and its
            builder are both configured.
    """  # noqa: E501

    def __init__(self, conf: Dict[str, Any]) -> None:
        self._key_deserializer, self._value_deserializer, conf_copy = pop_serdes(
            conf, 'key.deserializer', 'value.deserializer'
        )

        super(DeserializingConsumer, self).__init__(conf_copy)

        propagate_cluster_id(self, [self._key_deserializer, self._value_deserializer])

    # Narrows Consumer.poll()'s Message[bytes, bytes] to this consumer's
    # deserialized types, which is the whole point of the class.
    def poll(self, timeout: float = -1) -> Optional["Message[K, V]"]:  # type: ignore[override]
        """
        Consume messages and calls callbacks.

        The deserialized key and value are readable both through
        :py:func:`Message.key` / :py:func:`Message.value` and through
        :py:func:`Message.deserialized_key` / :py:func:`Message.deserialized_value`,
        which return the very same objects. Prefer the latter pair: they are typed
        with this consumer's key and value types, whereas ``key()`` and ``value()``
        remain typed as bytes for the benefit of the raw :py:class:`Consumer`.

        Args:
            timeout (float): Maximum time to block waiting for message(Seconds).

        Returns:
            :py:class:`Message` or None on timeout

        Raises:
            KeyDeserializationError: If an error occurs during key deserialization.

            ValueDeserializationError: If an error occurs during value deserialization.

            ConsumeError: If an error was encountered while polling.
        """

        msg = super(DeserializingConsumer, self).poll(timeout)

        if msg is None:
            return None

        error = msg.error()
        if error is not None:
            raise ConsumeError(error, kafka_message=msg)

        return self._deserialize(msg)

    def _deserialize(self, msg: "Message[Any, Any]") -> "Message[K, V]":
        """
        Deserialize a message's key and value in place and return it.

        The key is deserialized before the value so a key deserializer can stash
        state for the value deserializer (e.g. the Schema Registry DLQ action),
        matching ``SerializingProducer``.

        The deserialized objects are stored in the message's key and value slots,
        which is what makes them readable through both ``key()``/``value()`` and
        ``deserialized_key()``/``deserialized_value()``.

        Raises:
            KeyDeserializationError: If an error occurs during key deserialization.

            ValueDeserializationError: If an error occurs during value deserialization.
        """
        topic = msg.topic()
        if topic is None:
            raise TypeError("Message topic is None")
        ctx = SerializationContext(topic, MessageField.KEY, msg.headers())

        key: Any = msg.key()
        if self._key_deserializer is not None:
            try:
                key = self._key_deserializer(key, ctx)
            except Exception as se:
                raise KeyDeserializationError(exception=se, kafka_message=msg)

        value: Any = msg.value()
        ctx.field = MessageField.VALUE
        if self._value_deserializer is not None:
            try:
                value = self._value_deserializer(value, ctx)
            except Exception as se:
                raise ValueDeserializationError(exception=se, kafka_message=msg)

        msg.set_key(key)
        msg.set_value(value)
        return msg

    def consume(  # type: ignore[override]
        self, num_messages: int = 1, timeout: float = -1
    ) -> List["Message[K, V]"]:
        """
        :py:func:`Consumer.consume` not implemented, use
        :py:func:`DeserializingConsumer.poll` instead
        """

        raise NotImplementedError
