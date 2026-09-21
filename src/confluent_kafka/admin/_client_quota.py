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

from enum import Enum
from typing import Dict, List, Optional


class ClientQuotaMatchType(Enum):
    """Match type for a client quota filter component."""

    EXACT = 0
    DEFAULT = 1
    ANY = 2


class ClientQuotaEntity:
    """A client quota entity, represented by entity type and name pairs.

    A ``None`` name identifies the default entity for that entity type.
    The returned ``entries`` dictionary is a copy so the entity remains a
    stable dictionary key.
    """

    def __init__(self, entries: Dict[str, Optional[str]]) -> None:
        self._entries = dict(entries)

    @property
    def entries(self) -> Dict[str, Optional[str]]:
        return dict(self._entries)

    def __eq__(self, other: object) -> bool:
        return isinstance(other, ClientQuotaEntity) and self._entries == other._entries

    def __hash__(self) -> int:
        return hash(frozenset(self._entries.items()))

    def __repr__(self) -> str:
        return "ClientQuotaEntity({!r})".format(self._entries)


class ClientQuotaFilterComponent:
    """One entity component in a client quota filter."""

    def __init__(
        self,
        entity_type: str,
        match_type: ClientQuotaMatchType,
        match: Optional[str] = None,
    ) -> None:
        self.entity_type = entity_type
        self.match_type = match_type
        self.match = match


class ClientQuotaFilter:
    """Filter used by :meth:`AdminClient.describe_client_quotas`.

    When ``strict`` is true, matching entities cannot contain entity types
    absent from ``components``.
    """

    def __init__(self, components: List[ClientQuotaFilterComponent], strict: bool = False) -> None:
        self.components = components
        self.strict = strict


class ClientQuotaAlterationOp:
    """A quota key alteration. ``value=None`` removes the quota key."""

    def __init__(self, key: str, value: Optional[float]) -> None:
        self.key = key
        self.value = value


class ClientQuotaAlteration:
    """A set of quota operations for one client quota entity."""

    def __init__(self, entity: ClientQuotaEntity, ops: List[ClientQuotaAlterationOp]) -> None:
        self.entity = entity
        self.ops = ops
