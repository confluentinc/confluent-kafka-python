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

from typing import TYPE_CHECKING, Any, Dict, Generic, List, Optional

if TYPE_CHECKING:
    # PEP 696 defaults, so an unparameterized AsyncDeserializingConsumer(conf)
    # still infers as before instead of erroring with "Need type annotation".
    from typing_extensions import TypeVar

    K = TypeVar("K", default=Any)
    V = TypeVar("V", default=Any)
else:
    from typing import TypeVar

    K = TypeVar("K")
    V = TypeVar("V")

from ..cimpl import Message
from .._serde_builder import (
    CLUSTER_ID_TIMEOUT,
    async_build_serdes,
    async_close_serdes,
    maybe_await,
    pop_serde_props,
    propagate_cluster_id_resolver,
)
from .._util.asyncinit import asyncinit
from ..error import ConsumeError, KeyDeserializationError, ValueDeserializationError
from ..serialization import MessageField, SerializationContext
from ._AIOConsumer import AIOConsumer


@asyncinit
class AsyncDeserializingConsumer(AIOConsumer, Generic[K, V]):
    """
    An asyncio Kafka consumer with deserialization capabilities.

    `This class is experimental and likely to be removed, or subject to incompatible API
    changes in future versions of the library.`

    The asyncio counterpart of :py:class:`DeserializingConsumer`: derived from
    :py:class:`AIOConsumer`, overriding :py:func:`AIOConsumer.poll` and
    :py:func:`AIOConsumer.consume` to deserialize keys and values. Because the asyncio
    deserializers and their builders are themselves asynchronous, so is construction::

        consumer = await AsyncDeserializingConsumer({
            'bootstrap.servers': brokers,
            'group.id': group,
            'value.deserializer.builder': AsyncAvroDeserializerBuilder()
                .set_schema_registry_config({'url': schema_registry_url})
                .set_from_dict(dict_to_user),
        })

    Additional configuration properties:

    +--------------------------------+---------------------+--------------------------------------------+
    | Property Name                  | Type                | Description                                |
    +================================+=====================+============================================+
    |                                |                     | Callable(bytes, SerializationContext)      |
    | ``key.deserializer``           | callable            | -> obj, or an asyncio deserializer such as |
    |                                |                     | :py:class:`AsyncAvroDeserializer`, for keys|
    +--------------------------------+---------------------+--------------------------------------------+
    |                                |                     | Callable(bytes, SerializationContext)      |
    | ``value.deserializer``         | callable            | -> obj, or an asyncio deserializer, for    |
    |                                |                     | values.                                    |
    +--------------------------------+---------------------+--------------------------------------------+
    |                                |                     | DeserializerBuilder building the key       |
    | ``key.deserializer.builder``   | DeserializerBuilder | deserializer, as an alternative to passing |
    |                                |                     | a ready-made one in ``key.deserializer``.  |
    +--------------------------------+---------------------+--------------------------------------------+
    |                                |                     | DeserializerBuilder building the value     |
    | ``value.deserializer.builder`` | DeserializerBuilder | deserializer, as an alternative to passing |
    |                                |                     | one in ``value.deserializer``.             |
    +--------------------------------+---------------------+--------------------------------------------+

    Both the asyncio deserializers (:py:class:`AsyncAvroDeserializer` and friends, with
    their builders :py:class:`AsyncAvroDeserializerBuilder` and friends) and the blocking
    ones (:py:class:`StringDeserializer`, ...) are accepted; blocking deserializers run on
    the event loop, so keep them to the cheap, non-blocking kind.

    Deserializers are handed a way to obtain the Kafka cluster id: those that need it
    (deserializers resolving subjects through the Schema Registry associated subject name
    strategy without an explicit ``subject.name.strategy.kafka.cluster.id``) fetch it from
    the broker on their first lookup, so constructing the consumer never waits on a broker.

    Deserializers built here from a ``.builder`` property are owned by the consumer and
    closed by :py:func:`close`, together with any Schema Registry client the builder created
    for them. Ready-made deserializers remain the application's to close.

    The class is generic in the deserialized key and value types, which parameterize the
    messages it yields: on an ``AsyncDeserializingConsumer[str, User]``,
    :py:func:`Message.deserialized_value` is typed as ``Optional[User]``.

    Args:
        conf (dict): AsyncDeserializingConsumer configuration; the deserializer properties
            above plus everything :py:class:`AIOConsumer` accepts.

        **kwargs: Passed on to :py:class:`AIOConsumer` (``max_workers``, ``executor``).

    Raises:
        ValueError: If a deserializer and its builder are both configured.
    """  # noqa: E501

    async def __init_impl(self, conf: Dict[str, Any], **kwargs: Any) -> None:
        specs, conf_copy = pop_serde_props(conf, 'key.deserializer', 'value.deserializer')
        serdes, self._owned_serdes, conf_copy = await async_build_serdes(specs, conf_copy)
        self._key_deserializer, self._value_deserializer = serdes

        try:
            AIOConsumer.__init__(self, conf_copy, **kwargs)

            propagate_cluster_id_resolver(self._resolve_cluster_id, serdes)
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

    async def _resolve_cluster_id(self) -> str:
        return await self.cluster_id(timeout=CLUSTER_ID_TIMEOUT)

    async def close(self, *args: Any, **kwargs: Any) -> Any:
        """
        Close the consumer, then the deserializers it built.

        Leaves the group and closes the underlying :py:class:`AIOConsumer`
        first, then closes the deserializers built from
        ``key.deserializer.builder`` / ``value.deserializer.builder`` along with
        any Schema Registry client they own. Deserializers supplied ready-made
        are left untouched.
        """
        result = await super().close(*args, **kwargs)

        owned, self._owned_serdes = self._owned_serdes, []
        await async_close_serdes(owned)

        return result

    async def poll(self, timeout: float = -1) -> Optional["Message[K, V]"]:  # type: ignore[override]
        """
        Consume a message and deserialize its key and value.

        The deserialized key and value are readable both through
        :py:func:`Message.key` / :py:func:`Message.value` and through
        :py:func:`Message.deserialized_key` / :py:func:`Message.deserialized_value`,
        which return the very same objects. Prefer the latter pair: they are typed
        with this consumer's key and value types.

        Args:
            timeout (float): Maximum time to block waiting for message (seconds).

        Returns:
            :py:class:`Message` or None on timeout

        Raises:
            KeyDeserializationError: If an error occurs during key deserialization.

            ValueDeserializationError: If an error occurs during value deserialization.

            ConsumeError: If an error was encountered while polling.
        """
        msg = await super().poll(timeout)

        if msg is None:
            return None

        error = msg.error()
        if error is not None:
            raise ConsumeError(error, kafka_message=msg)

        return await self._deserialize(msg)

    async def consume(  # type: ignore[override]
        self, num_messages: int = 1, timeout: float = -1
    ) -> List["Message[K, V]"]:
        """
        Consume a batch of messages and deserialize their keys and values.

        Messages carrying an error (``msg.error()`` is not None) are returned
        as they are, without deserialization, so that the rest of the batch is
        not lost; check :py:func:`Message.error` on each message.

        Args:
            num_messages (int): Maximum number of messages to return.

            timeout (float): Maximum time to block waiting for messages (seconds).

        Returns:
            list(Message): The consumed messages, possibly empty on timeout.

        Raises:
            KeyDeserializationError: If an error occurs during key deserialization.

            ValueDeserializationError: If an error occurs during value deserialization.
        """
        msgs = await super().consume(num_messages, timeout)

        result: List["Message[K, V]"] = []
        for msg in msgs:
            if msg.error() is not None:
                result.append(msg)
            else:
                result.append(await self._deserialize(msg))
        return result

    async def _deserialize(self, msg: "Message[Any, Any]") -> "Message[K, V]":
        """
        Deserialize a message's key and value in place and return it.

        The key is deserialized before the value so a key deserializer can stash
        state for the value deserializer (e.g. the Schema Registry DLQ action),
        matching :py:class:`DeserializingConsumer`.
        """
        topic = msg.topic()
        if topic is None:
            raise TypeError("Message topic is None")
        ctx = SerializationContext(topic, MessageField.KEY, msg.headers())

        key: Any = msg.key()
        if self._key_deserializer is not None:
            try:
                key = await maybe_await(self._key_deserializer(key, ctx))
            except Exception as se:
                raise KeyDeserializationError(exception=se, kafka_message=msg)

        value: Any = msg.value()
        ctx.field = MessageField.VALUE
        if self._value_deserializer is not None:
            try:
                value = await maybe_await(self._value_deserializer(value, ctx))
            except Exception as se:
                raise ValueDeserializationError(exception=se, kafka_message=msg)

        msg.set_key(key)
        msg.set_value(value)
        return msg
