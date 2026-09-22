#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
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
#

"""
Shared plumbing for the serde-building clients.

:py:class:`SerializingProducer`, :py:class:`DeserializingConsumer` and their
asyncio counterparts all accept either a ready-made serde or a builder that
produces one, own the serdes they built, and hand the serdes a resolver for
the Kafka cluster id. That handling lives here so the clients stay in step.
"""

import inspect
from typing import Any, Callable, Dict, List, NamedTuple, Optional, Tuple

#: Time to wait for the cluster id, in seconds. Matches the default
#: ``max.block.ms`` the Java client allows for metadata retrieval.
CLUSTER_ID_TIMEOUT = 60.0


class SerdeSpec(NamedTuple):
    """What a client configuration says about one of its serdes."""

    prop: str
    serde: Any
    builder: Any
    is_key: bool


def pop_serde_props(conf: Dict[str, Any], key_prop: str, value_prop: str) -> Tuple[List[SerdeSpec], Dict[str, Any]]:
    """
    Pop the key and value serde properties from a client configuration.

    Pops ``<key_prop>`` / ``<value_prop>`` and their ``.builder`` counterparts
    without running the builders, so that the same configuration handling
    serves both the blocking and the asyncio clients.

    Args:
        conf (dict): Client configuration. Not modified; a copy is returned.

        key_prop (str): Property holding the key serde, e.g. ``key.serializer``.

        value_prop (str): Property holding the value serde.

    Returns:
        tuple: The key and value :py:class:`SerdeSpec`, in that order, and the
        remaining configuration.

    Raises:
        ValueError: If a property and its ``.builder`` counterpart are both
            configured.
    """

    conf_copy = conf.copy()
    specs = []

    for prop, is_key in ((key_prop, True), (value_prop, False)):
        builder_prop = prop + '.builder'
        serde = conf_copy.pop(prop, None)
        builder = conf_copy.pop(builder_prop, None)

        if serde is not None and builder is not None:
            raise ValueError("Cannot configure both {} and {}; use one or the other".format(prop, builder_prop))

        specs.append(SerdeSpec(prop, serde, builder, is_key))

    return specs, conf_copy


def validate_build_result(builder_prop: str, result: Any) -> Tuple[Any, Dict[str, Any]]:
    """
    Check what a builder handed back and split it into serde and configuration.

    Raises:
        ValueError: If the result is not a ``(serde, dict)`` pair, naming the
            property whose builder is at fault.
    """

    try:
        serde, remaining_conf = result
    except (TypeError, ValueError):
        raise ValueError(
            "{}.builder must return a (serde, configuration) tuple from build(), got {}".format(
                builder_prop, type(result).__name__
            )
        )

    if not isinstance(remaining_conf, dict):
        raise ValueError(
            "{}.builder returned {} as the leftover configuration from build(), expected a dict".format(
                builder_prop, type(remaining_conf).__name__
            )
        )

    return serde, remaining_conf


def build_serdes(specs: List[SerdeSpec], conf: Dict[str, Any]) -> Tuple[List[Any], List[Any], Dict[str, Any]]:
    """
    Run the builders of a client configuration.

    The configuration is threaded through the builders in turn, so a builder
    can consume properties of its own before the client sees them. If a
    builder fails, the serdes built before it are closed.

    Args:
        specs (list): The serde specs from :py:func:`pop_serde_props`.

        conf (dict): The remaining client configuration.

    Returns:
        tuple: The serdes in spec order (None where none was configured), the
        subset of them that was built here and is therefore owned by the
        client, and the configuration left over for the client.
    """

    serdes = []
    owned: List[Any] = []

    try:
        for spec in specs:
            if spec.builder is None:
                serdes.append(spec.serde)
                continue

            serde, conf = validate_build_result(spec.prop, spec.builder.build(conf, spec.is_key))
            serdes.append(serde)
            owned.append(serde)
    except BaseException:
        close_serdes(owned)
        raise

    return serdes, owned, conf


async def maybe_await(result: Any) -> Any:
    """Await ``result`` if it is awaitable, else hand it back as is.

    Lets the asyncio clients accept both the asyncio serdes, whose calls
    return coroutines, and the blocking ones (plain callables included).
    """

    if inspect.isawaitable(result):
        return await result
    return result


async def async_build_serdes(
    specs: List[SerdeSpec], conf: Dict[str, Any]
) -> Tuple[List[Any], List[Any], Dict[str, Any]]:
    """
    Asyncio counterpart of :py:func:`build_serdes`.

    A builder's ``build()`` may return either a ``(serde, conf)`` pair or a
    coroutine yielding one, as the asyncio Schema Registry builders do.
    """

    serdes = []
    owned: List[Any] = []

    try:
        for spec in specs:
            if spec.builder is None:
                serdes.append(spec.serde)
                continue

            result = await maybe_await(spec.builder.build(conf, spec.is_key))
            serde, conf = validate_build_result(spec.prop, result)
            serdes.append(serde)
            owned.append(serde)
    except BaseException:
        await async_close_serdes(owned)
        raise

    return serdes, owned, conf


def propagate_cluster_id_resolver(resolver: Callable[[], Any], serdes: List[Any]) -> None:
    """
    Hand a Kafka cluster id resolver to every serde that can take one.

    Nothing is resolved here: the serdes that need the id (those resolving
    subjects through the Schema Registry associated subject name strategy
    without an explicit ``subject.name.strategy.kafka.cluster.id``) invoke the
    resolver on their first lookup, so creating a client never waits on a
    broker.

    Args:
        resolver (callable): Callable returning the cluster id; a coroutine
            function for the asyncio clients.

        serdes (list): Serdes to offer the resolver to. None entries and
            serdes without a ``set_cluster_id_resolver`` method, such as plain
            callables, are skipped.
    """

    for serde in serdes:
        set_resolver = getattr(serde, 'set_cluster_id_resolver', None)
        if callable(set_resolver):
            set_resolver(resolver)


def close_serdes(serdes: List[Any]) -> None:
    """
    Close every serde, attempting all of them before re-raising the first error.

    Serdes without a ``close`` method are skipped.
    """

    first_error: Optional[BaseException] = None

    for serde in serdes:
        close = getattr(serde, 'close', None)
        if not callable(close):
            continue
        try:
            close()
        except BaseException as e:
            if first_error is None:
                first_error = e

    if first_error is not None:
        raise first_error


async def async_close_serdes(serdes: List[Any]) -> None:
    """
    Asyncio counterpart of :py:func:`close_serdes`.

    An asyncio serde is closed through its ``aclose()`` coroutine; a serde
    without one through ``close()``, which may itself be a coroutine function
    or a plain method.
    """

    first_error: Optional[BaseException] = None

    for serde in serdes:
        close = getattr(serde, 'aclose', None)
        if not callable(close):
            close = getattr(serde, 'close', None)
        if not callable(close):
            continue
        try:
            await maybe_await(close())
        except BaseException as e:
            if first_error is None:
                first_error = e

    if first_error is not None:
        raise first_error
