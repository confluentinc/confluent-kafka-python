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

from typing import TYPE_CHECKING, Any, Dict, Generic, Optional

if TYPE_CHECKING:
    # PEP 696 defaults, so an unparameterized SerializingProducer(conf) still
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

from confluent_kafka.cimpl import Producer as _ProducerImpl

from ._serde_builder import (
    CLUSTER_ID_TIMEOUT,
    build_serdes,
    close_serdes,
    pop_serde_props,
    propagate_cluster_id_resolver,
)
from ._types import DeliveryCallback, HeadersType
from .error import KeySerializationError, ValueSerializationError
from .serialization import MessageField, SerializationContext


class SerializingProducer(_ProducerImpl, Generic[K, V]):
    """
    A high level Kafka producer with serialization capabilities.

    `This class is experimental and likely to be removed, or subject to incompatible API
    changes in future versions of the library. To avoid breaking changes on upgrading, we
    recommend using serializers directly.`

    Derived from the :py:class:`Producer` class, overriding the :py:func:`Producer.produce`
    method to add serialization capabilities.

    Additional configuration properties:

    +------------------------------+---------------------+------------------------------------------------+
    | Property Name                | Type                | Description                                    |
    +==============================+=====================+================================================+
    |                              |                     | Callable(obj, SerializationContext) -> bytes   |
    | ``key.serializer``           | callable            |                                                |
    |                              |                     | Serializer used for message keys.              |
    +------------------------------+---------------------+------------------------------------------------+
    |                              |                     | Callable(obj, SerializationContext) -> bytes   |
    | ``value.serializer``         | callable            |                                                |
    |                              |                     | Serializer used for message values.            |
    +------------------------------+---------------------+------------------------------------------------+
    |                              |                     | SerializerBuilder building the key serializer, |
    | ``key.serializer.builder``   | SerializerBuilder   | as an alternative to passing a ready-made one  |
    |                              |                     | in ``key.serializer``.                         |
    +------------------------------+---------------------+------------------------------------------------+
    |                              |                     | SerializerBuilder building the value           |
    | ``value.serializer.builder`` | SerializerBuilder   | serializer, as an alternative to passing a     |
    |                              |                     | ready-made one in ``value.serializer``.        |
    +------------------------------+---------------------+------------------------------------------------+

    Serializers for string, integer and double (:py:class:`StringSerializer`, :py:class:`IntegerSerializer`
    and :py:class:`DoubleSerializer`) are supplied out-of-the-box in the ``confluent_kafka.serialization``
    namespace.

    Serializers for Protobuf, JSON Schema and Avro (:py:class:`ProtobufSerializer`, :py:class:`JSONSerializer`
    and :py:class:`AvroSerializer`) with Confluent Schema Registry integration are supplied out-of-the-box
    in the ``confluent_kafka.schema_registry`` namespace, each with a matching builder
    (:py:class:`AvroSerializerBuilder` and friends) that constructs the Schema Registry client for you::

        producer = SerializingProducer({
            'bootstrap.servers': brokers,
            'value.serializer.builder': AvroSerializerBuilder()
                .set_schema_registry_config({'url': schema_registry_url})
                .set_schema(schema_str),
        })

    Serializers are also handed a way to obtain the Kafka cluster id: those that need it (serializers
    resolving subjects through the Schema Registry associated subject name strategy without an explicit
    ``subject.name.strategy.kafka.cluster.id``) fetch it from the broker on their first lookup, so
    constructing the producer never waits on a broker. Until a broker has been reached that lookup
    raises a :py:class:`SerializationError` and is retried on the next message.

    Serializers built here from a ``.builder`` property are owned by the producer and closed by
    :py:func:`close`, together with any Schema Registry client the builder created for them. Ready-made
    serializers passed in ``key.serializer`` / ``value.serializer`` remain the application's to close.

    The class is generic in the key and value types accepted by :py:func:`produce`, so
    ``SerializingProducer[str, User]`` type checks the objects handed to it. Left unparameterized both
    are Any, as before.

    See Also:
        - The :ref:`Configuration Guide <pythonclient_configuration>` for in depth information on how to configure the client.
        - `CONFIGURATION.md <https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md>`_ for a comprehensive set of configuration properties.
        - `STATISTICS.md <https://github.com/edenhill/librdkafka/blob/master/STATISTICS.md>`_ for detailed information on the statistics provided by stats_cb
        - The :py:class:`Producer` class for inherited methods.

    Args:
        conf (producer): SerializingProducer configuration.

    Raises:
        ValueError: If a serializer and its builder are both configured.
    """  # noqa E501

    def __init__(self, conf: Dict[str, Any]) -> None:
        specs, conf_copy = pop_serde_props(conf, 'key.serializer', 'value.serializer')
        serdes, self._owned_serdes, conf_copy = build_serdes(specs, conf_copy)
        self._key_serializer, self._value_serializer = serdes
        self._closed = False

        try:
            super(SerializingProducer, self).__init__(conf_copy)

            propagate_cluster_id_resolver(lambda: self.cluster_id(timeout=CLUSTER_ID_TIMEOUT), serdes)
        except BaseException:
            owned, self._owned_serdes = self._owned_serdes, []
            try:
                close_serdes(owned)
            except Exception:
                pass
            raise

    def close(self) -> bool:
        """
        Close the producer, then the serializers it built.

        Flushes and destroys the underlying :py:class:`Producer` first, then
        closes the serializers built from ``key.serializer.builder`` /
        ``value.serializer.builder`` along with any Schema Registry client
        they own. Serializers supplied ready-made are left untouched.

        Safe to call more than once: later calls do nothing.

        Returns:
            bool: What :py:func:`Producer.close` returned.
        """
        try:
            return super(SerializingProducer, self).close()
        finally:
            # Only now: the flush inside Producer.close() dispatches delivery
            # callbacks, which may legitimately produce() again on this same
            # thread, and Producer itself already rejects any other caller
            # while it is closing.
            self._closed = True
            # released even when flushing/destroying the producer raised
            owned, self._owned_serdes = self._owned_serdes, []
            close_serdes(owned)

    def __exit__(self, exc_type: Any, exc_value: Any, exc_traceback: Any) -> Optional[bool]:
        # Producer.__exit__ is implemented in C and calls the C close directly,
        # which would skip the serializer release above.
        self.close()
        return None

    def produce(  # type: ignore[override]
        self,
        topic: str,
        key: Optional[K] = None,
        value: Optional[V] = None,
        partition: int = -1,
        on_delivery: Optional[DeliveryCallback] = None,
        timestamp: int = 0,
        headers: Optional[HeadersType] = None,
    ) -> None:
        """
        Produce a message.

        This is an asynchronous operation. An application may use the
        ``on_delivery`` argument to pass a function (or lambda) that will be
        called from :py:func:`SerializingProducer.poll` when the message has
        been successfully delivered or permanently fails delivery.

        Note:
            Currently message headers are not supported on the message returned to
            the callback. The ``msg.headers()`` will return None even if the
            original message had headers set.

        Args:
            topic (str): Topic to produce message to.

            key (object, optional): Message payload key.

            value (object, optional): Message payload value.

            partition (int, optional): Partition to produce to, else the
                configured built-in partitioner will be used.

            on_delivery (callable(KafkaError, Message), optional): Delivery
                report callback. Called as a side effect of
                :py:func:`SerializingProducer.poll` or
                :py:func:`SerializingProducer.flush` on successful or
                failed delivery.

            timestamp (int, optional): Message timestamp (CreateTime) in
                milliseconds since Unix epoch UTC (requires broker >= 0.10.0.0).
                Default value is current time.

            headers (dict, optional): Message headers. The header key must be
                a str while the value must be binary, unicode or None. (Requires
                broker version >= 0.11.0.0)

        Raises:
            BufferError: if the internal producer message queue is full.
                (``queue.buffering.max.messages`` exceeded). If this happens
                the application should call :py:func:`SerializingProducer.Poll`
                and try again.

            KeySerializationError: If an error occurs during key serialization.

            ValueSerializationError: If an error occurs during value serialization.

            RuntimeError: If the producer has been closed. Checked before the
                serializers run, so they never see a message that cannot be produced.

            TypeError: If ``topic`` is not a str, likewise checked before the
                serializers run.

            KafkaException: For all other errors
        """

        key_bytes: Any = key
        value_bytes: Any = value

        # Fail before the serializers run, as they may have side effects
        # (schema registration) for a message that can no longer be produced.
        if self._closed:
            raise RuntimeError("Producer has been closed")
        if not isinstance(topic, str):
            raise TypeError("topic must be a str, not {}".format(type(topic).__name__))

        ctx = SerializationContext(topic, MessageField.KEY, headers)
        if self._key_serializer is not None:
            try:
                key_bytes = self._key_serializer(key, ctx)
            except Exception as se:
                raise KeySerializationError(se)
        ctx.field = MessageField.VALUE
        if self._value_serializer is not None:
            try:
                value_bytes = self._value_serializer(value, ctx)
            except Exception as se:
                raise ValueSerializationError(se)

        super(SerializingProducer, self).produce(
            topic,
            value_bytes,
            key_bytes,
            headers=headers,
            partition=partition,
            timestamp=timestamp,
            on_delivery=on_delivery,
        )
