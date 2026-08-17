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

:py:class:`SerializingProducer` and :py:class:`DeserializingConsumer` both
accept either a ready-made serde or a builder that produces one, and both have
to hand the Kafka cluster id to the serdes that ask for it. That handling lives
here so the two clients stay in step.
"""

from typing import Any, Dict, List, Optional, Tuple

#: Time to wait for the cluster id, in seconds. Matches the default
#: ``max.block.ms`` the Java client allows for metadata retrieval.
CLUSTER_ID_TIMEOUT = 60.0


def pop_serdes(conf: Dict[str, Any], key_prop: str, value_prop: str) -> Tuple[Any, Any, Dict[str, Any]]:
    """
    Resolve the key and value serdes from a client configuration.

    Pops ``<key_prop>`` / ``<value_prop>`` and their ``.builder`` counterparts,
    then runs any builder found. The remaining configuration is threaded through
    the builders in turn, so a builder can consume properties of its own before
    the client sees them.

    Args:
        conf (dict): Client configuration. Not modified; a copy is returned.

        key_prop (str): Property holding the key serde, e.g. ``key.serializer``.

        value_prop (str): Property holding the value serde.

    Returns:
        tuple: ``(key_serde, value_serde, remaining_conf)``. Either serde is
        None when neither the property nor its builder was configured.

    Raises:
        ValueError: If a property and its ``.builder`` counterpart are both
            configured, or if a builder does not return the leftover
            configuration.
    """

    conf_copy = conf.copy()
    serdes = []

    for prop, is_key in ((key_prop, True), (value_prop, False)):
        builder_prop = prop + '.builder'
        serde = conf_copy.pop(prop, None)
        builder = conf_copy.pop(builder_prop, None)

        if builder is None:
            serdes.append(serde)
            continue

        if serde is not None:
            raise ValueError("Cannot configure both {} and {}; use one or the other".format(prop, builder_prop))

        serde, conf_copy = _build(builder, builder_prop, conf_copy, is_key)
        serdes.append(serde)

    return serdes[0], serdes[1], conf_copy


def _build(builder: Any, builder_prop: str, conf: Dict[str, Any], is_key: bool) -> Tuple[Any, Dict[str, Any]]:
    """Run a single builder and validate what it hands back."""

    try:
        serde, remaining_conf = builder.build(conf, is_key)
    except (TypeError, ValueError) as e:
        # A builder returning something other than a (serde, conf) pair fails
        # here with a message that does not mention the caller, so say which
        # property is at fault.
        raise ValueError("{} must return a (serde, configuration) tuple from build(): {}".format(builder_prop, e))

    if not isinstance(remaining_conf, dict):
        raise ValueError(
            "{} returned {} as the leftover configuration from build(), expected a dict".format(
                builder_prop, type(remaining_conf).__name__
            )
        )

    return serde, remaining_conf


def propagate_cluster_id(client: Any, serdes: List[Any], timeout: float = CLUSTER_ID_TIMEOUT) -> Optional[str]:
    """
    Hand the Kafka cluster id to every serde that asks for it.

    The id is fetched from the broker at most once, and only when at least one
    serde needs it, so clients configured with serdes that do not care pay
    nothing.

    Args:
        client: The client to fetch the cluster id from.

        serdes (list): Serdes to offer the cluster id to. None entries and
            serdes that are plain callables are skipped.

        timeout (float): Maximum time to wait for the cluster id, in seconds.

    Returns:
        str: The cluster id, or None if no serde needed it.
    """

    needy = [serde for serde in serdes if _needs_cluster_id(serde)]
    if not needy:
        return None

    cluster_id = client.cluster_id(timeout=timeout)
    for serde in needy:
        serde.set_cluster_id(cluster_id)

    return cluster_id


def _needs_cluster_id(serde: Any) -> bool:
    """
    Whether a serde wants the cluster id.

    Serdes may be plain callables rather than
    :py:class:`~confluent_kafka.serialization.Serializer` instances, so the
    method is looked up rather than assumed.
    """

    needs = getattr(serde, 'needs_cluster_id', None)
    return callable(needs) and bool(needs())
