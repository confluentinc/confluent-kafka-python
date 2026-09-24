# Copyright 2026 Confluent Inc.
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

import asyncio
from typing import TYPE_CHECKING, Any, Dict, Generic, Optional

if TYPE_CHECKING:
    # PEP 696 defaults, so an unparameterized AsyncSerializingProducer(conf)
    # still infers as before instead of erroring with "Need type annotation".
    from typing_extensions import TypeVar

    K = TypeVar("K", default=Any)
    V = TypeVar("V", default=Any)
else:
    from typing import TypeVar

    K = TypeVar("K")
    V = TypeVar("V")

from .._serde_builder import (
    AsyncSharedClusterIdResolver,
    async_build_serdes,
    async_close_serdes,
    maybe_await,
    pop_serde_props,
    propagate_cluster_id_resolver,
)
from .._types import HeadersType
from .._util.asyncinit import asyncinit
from ..error import KeySerializationError, ValueSerializationError
from ..serialization import MessageField, SerializationContext
from .producer import AIOProducer


@asyncinit
class AsyncSerializingProducer(AIOProducer, Generic[K, V]):
    """
    An asyncio Kafka producer with serialization capabilities.

    `This class is experimental and likely to be removed, or subject to incompatible API
    changes in future versions of the library.`

    The asyncio counterpart of :py:class:`SerializingProducer`: derived from
    :py:class:`AIOProducer`, overriding :py:func:`AIOProducer.produce` to serialize
    the key and value first. Because the asyncio serializers and their builders are
    themselves asynchronous, so is construction::

        producer = await AsyncSerializingProducer({
            'bootstrap.servers': brokers,
            'value.serializer.builder': AsyncAvroSerializerBuilder()
                .set_schema_registry_config({'url': schema_registry_url})
                .set_schema(schema_str),
        })

    Additional configuration properties:

    +------------------------------+---------------------+------------------------------------------------+
    | Property Name                | Type                | Description                                    |
    +==============================+=====================+================================================+
    |                              |                     | Callable(obj, SerializationContext) -> bytes,  |
    | ``key.serializer``           | callable            | or an asyncio serializer such as               |
    |                              |                     | :py:class:`AsyncAvroSerializer`, for keys.     |
    +------------------------------+---------------------+------------------------------------------------+
    |                              |                     | Callable(obj, SerializationContext) -> bytes,  |
    | ``value.serializer``         | callable            | or an asyncio serializer, for values.          |
    |                              |                     |                                                |
    +------------------------------+---------------------+------------------------------------------------+
    |                              |                     | SerializerBuilder building the key serializer, |
    | ``key.serializer.builder``   | SerializerBuilder   | as an alternative to passing a ready-made one  |
    |                              |                     | in ``key.serializer``.                         |
    +------------------------------+---------------------+------------------------------------------------+
    |                              |                     | SerializerBuilder building the value           |
    | ``value.serializer.builder`` | SerializerBuilder   | serializer, as an alternative to passing a     |
    |                              |                     | ready-made one in ``value.serializer``.        |
    +------------------------------+---------------------+------------------------------------------------+

    Both the asyncio serializers (:py:class:`AsyncAvroSerializer` and friends, with their
    builders :py:class:`AsyncAvroSerializerBuilder` and friends) and the blocking ones
    (:py:class:`StringSerializer`, ...) are accepted; blocking serializers run on the
    event loop, so keep them to the cheap, non-blocking kind.

    Serializers are handed a way to obtain the Kafka cluster id: those that need it
    (serializers resolving subjects through the Schema Registry associated subject name
    strategy without an explicit ``subject.name.strategy.kafka.cluster.id``) fetch it from
    the broker on their first lookup, so constructing the producer never waits on a broker.

    Serializers built here from a ``.builder`` property are owned by the producer and closed
    by :py:func:`close`, together with any Schema Registry client the builder created for
    them. Ready-made serializers remain the application's to close.

    Args:
        conf (dict): AsyncSerializingProducer configuration; the serializer properties
            above plus everything :py:class:`AIOProducer` accepts.

        **kwargs: Passed on to :py:class:`AIOProducer` (``max_workers``, ``executor``,
            ``batch_size``, ``buffer_timeout``).

    Raises:
        ValueError: If a serializer and its builder are both configured.
    """  # noqa: E501

    async def __init_impl(self, conf: Dict[str, Any], **kwargs: Any) -> None:
        specs, conf_copy = pop_serde_props(conf, 'key.serializer', 'value.serializer')
        serdes, self._owned_serdes, conf_copy = await async_build_serdes(specs, conf_copy)
        self._key_serializer, self._value_serializer = serdes

        try:
            AIOProducer.__init__(self, conf_copy, **kwargs)

            propagate_cluster_id_resolver(AsyncSharedClusterIdResolver(self.cluster_id), serdes)
        except BaseException:
            owned, self._owned_serdes = self._owned_serdes, []
            try:
                await async_close_serdes(owned)
            except Exception:
                pass
            raise

    # asyncinit awaits __init__, so it is a coroutine function; assigning it
    # keeps type checkers from objecting to an async __init__.
    __init__ = __init_impl

    async def close(self) -> None:
        """
        Close the producer, then the serializers it built.

        Flushes and shuts down the underlying :py:class:`AIOProducer` first,
        then closes the serializers built from ``key.serializer.builder`` /
        ``value.serializer.builder`` along with any Schema Registry client they
        own. Serializers supplied ready-made are left untouched.
        """
        try:
            # AIOProducer.close() shuts its executor down and cannot run twice
            if not self._is_closed:
                await super().close()
        finally:
            # released even when flushing raised
            owned, self._owned_serdes = self._owned_serdes, []
            await async_close_serdes(owned)

    async def produce(  # type: ignore[override]
        self,
        topic: str,
        key: Optional[K] = None,
        value: Optional[V] = None,
        partition: int = -1,
        timestamp: int = 0,
        headers: Optional[HeadersType] = None,
    ) -> "asyncio.Future[Any]":
        """
        Serialize the key and value, then produce the message.

        Args:
            topic (str): Topic to produce message to.

            key (object, optional): Message payload key.

            value (object, optional): Message payload value.

            partition (int, optional): Partition to produce to, else the
                configured built-in partitioner will be used.

            timestamp (int, optional): Message timestamp (CreateTime) in
                milliseconds since Unix epoch UTC. Default value is current
                time.

            headers (dict, optional): Message headers. Passed to the
                serializers through the :py:class:`SerializationContext`;
                :py:class:`AIOProducer` does not yet support producing them.

        Returns:
            asyncio.Future: Resolves to the delivered :py:class:`Message`, or
            raises on delivery failure.

        Raises:
            KeySerializationError: If an error occurs during key serialization.

            ValueSerializationError: If an error occurs during value serialization.

            RuntimeError: If the producer has been closed. Checked before the
                serializers run, so they never see a message that cannot be produced.

            TypeError: If ``topic`` is not a str, likewise checked before the
                serializers run.

            NotImplementedError: If headers are given, see :py:func:`AIOProducer.produce`.
        """
        # Fail before the serializers run, as they may have side effects
        # (schema registration) for a message that can no longer be produced.
        if self._is_closed:
            raise RuntimeError("Producer has been closed")
        if not isinstance(topic, str):
            raise TypeError("topic must be a str, not {}".format(type(topic).__name__))

        key_bytes: Any = key
        value_bytes: Any = value

        ctx = SerializationContext(topic, MessageField.KEY, headers)
        if self._key_serializer is not None:
            try:
                key_bytes = await maybe_await(self._key_serializer(key, ctx))
            except Exception as se:
                raise KeySerializationError(se)
        ctx.field = MessageField.VALUE
        if self._value_serializer is not None:
            try:
                value_bytes = await maybe_await(self._value_serializer(value, ctx))
            except Exception as se:
                raise ValueSerializationError(se)

        kwargs: Dict[str, Any] = {}
        if partition != -1:
            kwargs['partition'] = partition
        if timestamp != 0:
            kwargs['timestamp'] = timestamp
        if headers is not None:
            kwargs['headers'] = headers

        return await super().produce(topic, value_bytes, key_bytes, **kwargs)
