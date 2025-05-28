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
import abc
import random

from attrs import define as _attrs_define
from attrs import field as _attrs_field
from collections import defaultdict
from enum import Enum
from threading import Lock
from typing import List, Dict, Type, TypeVar, \
    cast, Optional, Any, Tuple

__all__ = [
    'VALID_AUTH_PROVIDERS',
    '_BearerFieldProvider',
    'is_success',
    'is_retriable',
    'full_jitter',
    '_StaticFieldProvider',
    '_SchemaCache',
    'RuleKind',
    'RuleMode',
    'RuleParams',
    'Rule',
    'RuleSet',
    'MetadataTags',
    'MetadataProperties',
    'Metadata',
    'SchemaReference',
    'ConfigCompatibilityLevel',
    'ServerConfig',
    'Schema',
    'RegisteredSchema'
]

VALID_AUTH_PROVIDERS = ['URL', 'USER_INFO']


class _BearerFieldProvider(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def get_bearer_fields(self) -> dict:
        raise NotImplementedError


def is_success(status_code: int) -> bool:
    return 200 <= status_code <= 299


def is_retriable(status_code: int) -> bool:
    return status_code in (408, 429, 500, 502, 503, 504)


def full_jitter(base_delay_ms: int, max_delay_ms: int, retries_attempted: int) -> float:
    no_jitter_delay = base_delay_ms * (2.0 ** retries_attempted)
    return random.random() * min(no_jitter_delay, max_delay_ms)


class _StaticFieldProvider(_BearerFieldProvider):
    def __init__(self, token: str, logical_cluster: str, identity_pool: str):
        self.token = token
        self.logical_cluster = logical_cluster
        self.identity_pool = identity_pool

    def get_bearer_fields(self) -> dict:
        return {'bearer.auth.token': self.token, 'bearer.auth.logical.cluster': self.logical_cluster,
                'bearer.auth.identity.pool.id': self.identity_pool}


class _SchemaCache(object):
    """
    Thread-safe cache for use with the Schema Registry Client.

    This cache may be used to retrieve schema ids, schemas or to check
    known subject membership.
    """

    def __init__(self):
        self.lock = Lock()
        self.schema_id_index = defaultdict(dict)
        self.schema_guid_index = {}
        self.schema_index = defaultdict(dict)
        self.rs_id_index = defaultdict(dict)
        self.rs_version_index = defaultdict(dict)
        self.rs_schema_index = defaultdict(dict)

    def set_schema(self, subject: Optional[str], schema_id: Optional[int], guid: Optional[str], schema: 'Schema'):
        """
        Add a Schema identified by schema_id to the cache.

        Args:
            subject (str): The subject this schema is associated with

            schema_id (int): Schema's id

            guid (str): Schema's guid

            schema (Schema): Schema instance
        """

        with self.lock:
            if schema_id is not None:
                self.schema_id_index[subject][schema_id] = (guid, schema)
                self.schema_index[subject][schema] = schema_id
            if guid is not None:
                self.schema_guid_index[guid] = schema

    def set_registered_schema(self, schema: 'Schema', registered_schema: 'RegisteredSchema'):
        """
        Add a RegisteredSchema to the cache.

        Args:
            schema (Schema): Schema instance
            registered_schema (RegisteredSchema): RegisteredSchema instance
        """

        subject = registered_schema.subject
        schema_id = registered_schema.schema_id
        guid = registered_schema.guid
        version = registered_schema.version
        with self.lock:
            if schema_id is not None:
                self.schema_id_index[subject][schema_id] = (guid, schema)
                self.schema_index[subject][schema] = schema_id
                self.rs_id_index[subject][schema_id] = registered_schema
            if guid is not None:
                self.schema_guid_index[guid] = schema
            self.rs_version_index[subject][version] = registered_schema
            self.rs_schema_index[subject][schema] = registered_schema

    def get_schema_by_id(self, subject: str, schema_id: int) -> Optional[Tuple[str, 'Schema']]:
        """
        Get the schema instance associated with schema id from the cache.

        Args:
            subject (str): The subject this schema is associated with

            schema_id (int): Id used to identify a schema

        Returns:
            Tuple[str, Schema]: The guid and schema if known; else None
        """

        with self.lock:
            return self.schema_id_index.get(subject, {}).get(schema_id, None)

    def get_schema_by_guid(self, guid: str) -> Optional['Schema']:
        """
        Get the schema instance associated with guid from the cache.

        Args:
            guid (str): Guid used to identify a schema

        Returns:
            Schema: The schema if known; else None
        """

        with self.lock:
            return self.schema_guid_index.get(guid, None)

    def get_id_by_schema(self, subject: str, schema: 'Schema') -> Optional[int]:
        """
        Get the schema id associated with schema instance from the cache.

        Args:
            subject (str): The subject this schema is associated with

            schema (Schema): The schema

        Returns:
            int: The schema id if known; else None
        """

        with self.lock:
            return self.schema_index.get(subject, {}).get(schema, None)

    def get_registered_by_subject_schema(self, subject: str, schema: 'Schema') -> Optional['RegisteredSchema']:
        """
        Get the schema associated with this schema registered under subject.

        Args:
            subject (str): The subject this schema is associated with

            schema (Schema): The schema associated with this schema

        Returns:
            RegisteredSchema: The registered schema if known; else None
        """

        with self.lock:
            return self.rs_schema_index.get(subject, {}).get(schema, None)

    def get_registered_by_subject_id(self, subject: str, schema_id: int) -> Optional['RegisteredSchema']:
        """
        Get the schema associated with this id registered under subject.

        Args:
            subject (str): The subject this schema is associated with

            schema_id (int): The schema id associated with this schema

        Returns:
            RegisteredSchema: The registered schema if known; else None
        """

        with self.lock:
            return self.rs_id_index.get(subject, {}).get(schema_id, None)

    def get_registered_by_subject_version(self, subject: str, version: int) -> Optional['RegisteredSchema']:
        """
        Get the schema associated with this version registered under subject.

        Args:
            subject (str): The subject this schema is associated with

            version (int): The version associated with this schema

        Returns:
            RegisteredSchema: The registered schema if known; else None
        """

        with self.lock:
            return self.rs_version_index.get(subject, {}).get(version, None)

    def remove_by_subject(self, subject: str):
        """
        Remove schemas with the given subject.

        Args:
            subject (str): The subject
        """

        with self.lock:
            if subject in self.schema_id_index:
                del self.schema_id_index[subject]
            if subject in self.schema_index:
                del self.schema_index[subject]
            if subject in self.rs_id_index:
                del self.rs_id_index[subject]
            if subject in self.rs_version_index:
                del self.rs_version_index[subject]
            if subject in self.rs_schema_index:
                del self.rs_schema_index[subject]

    def remove_by_subject_version(self, subject: str, version: int):
        """
        Remove schemas with the given subject.

        Args:
            subject (str): The subject

            version (int) The version
        """

        with self.lock:
            if subject in self.rs_id_index:
                for schema_id, registered_schema in self.rs_id_index[subject].items():
                    if registered_schema.version == version:
                        del self.rs_schema_index[subject][schema_id]
            if subject in self.rs_schema_index:
                for schema, registered_schema in self.rs_schema_index[subject].items():
                    if registered_schema.version == version:
                        del self.rs_schema_index[subject][schema]
            rs = None
            if subject in self.rs_version_index:
                if version in self.rs_version_index[subject]:
                    rs = self.rs_version_index[subject][version]
                    del self.rs_version_index[subject][version]
            if rs is not None:
                if subject in self.schema_id_index:
                    if rs.schema_id in self.schema_id_index[subject]:
                        del self.schema_id_index[subject][rs.schema_id]
                    if rs.schema in self.schema_index[subject]:
                        del self.schema_index[subject][rs.schema]

    def clear(self):
        """
        Clear the cache.
        """

        with self.lock:
            self.schema_id_index.clear()
            self.schema_guid_index.clear()
            self.schema_index.clear()
            self.rs_id_index.clear()
            self.rs_version_index.clear()
            self.rs_schema_index.clear()


T = TypeVar("T")


class RuleKind(str, Enum):
    CONDITION = "CONDITION"
    TRANSFORM = "TRANSFORM"

    def __str__(self) -> str:
        return str(self.value)


class RuleMode(str, Enum):
    UPGRADE = "UPGRADE"
    DOWNGRADE = "DOWNGRADE"
    UPDOWN = "UPDOWN"
    READ = "READ"
    WRITE = "WRITE"
    WRITEREAD = "WRITEREAD"

    def __str__(self) -> str:
        return str(self.value)


@_attrs_define
class RuleParams:
    params: Dict[str, str] = _attrs_field(factory=dict, hash=False)

    def to_dict(self) -> Dict[str, Any]:
        field_dict: Dict[str, Any] = {}
        field_dict.update(self.params)

        return field_dict

    @classmethod
    def from_dict(cls: Type[T], src_dict: Dict[str, Any]) -> T:
        d = src_dict.copy()

        rule_params = cls(params=d)

        return rule_params

    def __hash__(self):
        return hash(frozenset(self.params.items()))


@_attrs_define(frozen=True)
class Rule:
    name: Optional[str]
    doc: Optional[str]
    kind: Optional[RuleKind]
    mode: Optional[RuleMode]
    type: Optional[str]
    tags: Optional[List[str]] = _attrs_field(hash=False)
    params: Optional[RuleParams]
    expr: Optional[str]
    on_success: Optional[str]
    on_failure: Optional[str]
    disabled: Optional[bool]

    def to_dict(self) -> Dict[str, Any]:
        name = self.name

        doc = self.doc

        kind_str: Optional[str] = None
        if self.kind is not None:
            kind_str = self.kind.value

        mode_str: Optional[str] = None
        if self.mode is not None:
            mode_str = self.mode.value

        rule_type = self.type

        tags = self.tags

        _params: Optional[Dict[str, Any]] = None
        if self.params is not None:
            _params = self.params.to_dict()

        expr = self.expr

        on_success = self.on_success

        on_failure = self.on_failure

        disabled = self.disabled

        field_dict: Dict[str, Any] = {}
        field_dict.update({})
        if name is not None:
            field_dict["name"] = name
        if doc is not None:
            field_dict["doc"] = doc
        if kind_str is not None:
            field_dict["kind"] = kind_str
        if mode_str is not None:
            field_dict["mode"] = mode_str
        if type is not None:
            field_dict["type"] = rule_type
        if tags is not None:
            field_dict["tags"] = tags
        if _params is not None:
            field_dict["params"] = _params
        if expr is not None:
            field_dict["expr"] = expr
        if on_success is not None:
            field_dict["onSuccess"] = on_success
        if on_failure is not None:
            field_dict["onFailure"] = on_failure
        if disabled is not None:
            field_dict["disabled"] = disabled

        return field_dict

    @classmethod
    def from_dict(cls: Type[T], src_dict: Dict[str, Any]) -> T:
        d = src_dict.copy()
        name = d.pop("name", None)

        doc = d.pop("doc", None)

        _kind = d.pop("kind", None)
        kind: Optional[RuleKind] = None
        if _kind is not None:
            kind = RuleKind(_kind)

        _mode = d.pop("mode", None)
        mode: Optional[RuleMode] = None
        if _mode is not None:
            mode = RuleMode(_mode)

        rule_type = d.pop("type", None)

        tags = cast(List[str], d.pop("tags", None))

        _params: Optional[Dict[str, Any]] = d.pop("params", None)
        params: Optional[RuleParams] = None
        if _params is not None:
            params = RuleParams.from_dict(_params)

        expr = d.pop("expr", None)

        on_success = d.pop("onSuccess", None)

        on_failure = d.pop("onFailure", None)

        disabled = d.pop("disabled", None)

        rule = cls(
            name=name,
            doc=doc,
            kind=kind,
            mode=mode,
            type=rule_type,
            tags=tags,
            params=params,
            expr=expr,
            on_success=on_success,
            on_failure=on_failure,
            disabled=disabled,
        )

        return rule


@_attrs_define
class RuleSet:
    migration_rules: Optional[List["Rule"]] = _attrs_field(hash=False)
    domain_rules: Optional[List["Rule"]] = _attrs_field(hash=False)

    def to_dict(self) -> Dict[str, Any]:
        _migration_rules: Optional[List[Dict[str, Any]]] = None
        if self.migration_rules is not None:
            _migration_rules = []
            for migration_rules_item_data in self.migration_rules:
                migration_rules_item = migration_rules_item_data.to_dict()
                _migration_rules.append(migration_rules_item)

        _domain_rules: Optional[List[Dict[str, Any]]] = None
        if self.domain_rules is not None:
            _domain_rules = []
            for domain_rules_item_data in self.domain_rules:
                domain_rules_item = domain_rules_item_data.to_dict()
                _domain_rules.append(domain_rules_item)

        field_dict: Dict[str, Any] = {}
        field_dict.update({})
        if _migration_rules is not None:
            field_dict["migrationRules"] = _migration_rules
        if _domain_rules is not None:
            field_dict["domainRules"] = _domain_rules

        return field_dict

    @classmethod
    def from_dict(cls: Type[T], src_dict: Dict[str, Any]) -> T:
        d = src_dict.copy()
        migration_rules = []
        _migration_rules = d.pop("migrationRules", None)
        for migration_rules_item_data in _migration_rules or []:
            migration_rules_item = Rule.from_dict(migration_rules_item_data)
            migration_rules.append(migration_rules_item)

        domain_rules = []
        _domain_rules = d.pop("domainRules", None)
        for domain_rules_item_data in _domain_rules or []:
            domain_rules_item = Rule.from_dict(domain_rules_item_data)
            domain_rules.append(domain_rules_item)

        rule_set = cls(
            migration_rules=migration_rules,
            domain_rules=domain_rules,
        )

        return rule_set

    def __hash__(self):
        return hash(frozenset((self.migration_rules or []) + (self.domain_rules or [])))


@_attrs_define
class MetadataTags:
    tags: Dict[str, List[str]] = _attrs_field(factory=dict, hash=False)

    def to_dict(self) -> Dict[str, Any]:
        field_dict: Dict[str, Any] = {}
        for prop_name, prop in self.tags.items():
            field_dict[prop_name] = prop

        return field_dict

    @classmethod
    def from_dict(cls: Type[T], src_dict: Dict[str, Any]) -> T:
        d = src_dict.copy()

        tags = {}
        for prop_name, prop_dict in d.items():
            tag = cast(List[str], prop_dict)

            tags[prop_name] = tag

        metadata_tags = cls(tags=tags)

        return metadata_tags

    def __hash__(self):
        return hash(frozenset(self.tags.items()))


@_attrs_define
class MetadataProperties:
    properties: Dict[str, str] = _attrs_field(factory=dict, hash=False)

    def to_dict(self) -> Dict[str, Any]:
        field_dict: Dict[str, Any] = {}
        field_dict.update(self.properties)

        return field_dict

    @classmethod
    def from_dict(cls: Type[T], src_dict: Dict[str, Any]) -> T:
        d = src_dict.copy()

        metadata_properties = cls(properties=d)

        return metadata_properties

    def __hash__(self):
        return hash(frozenset(self.properties.items()))


@_attrs_define(frozen=True)
class Metadata:
    tags: Optional[MetadataTags]
    properties: Optional[MetadataProperties]
    sensitive: Optional[List[str]] = _attrs_field(hash=False)

    def to_dict(self) -> Dict[str, Any]:
        _tags: Optional[Dict[str, Any]] = None
        if self.tags is not None:
            _tags = self.tags.to_dict()

        _properties: Optional[Dict[str, Any]] = None
        if self.properties is not None:
            _properties = self.properties.to_dict()

        sensitive: Optional[List[str]] = None
        if self.sensitive is not None:
            sensitive = []
            for sensitive_item in self.sensitive:
                sensitive.append(sensitive_item)

        field_dict: Dict[str, Any] = {}
        if _tags is not None:
            field_dict["tags"] = _tags
        if _properties is not None:
            field_dict["properties"] = _properties
        if sensitive is not None:
            field_dict["sensitive"] = sensitive

        return field_dict

    @classmethod
    def from_dict(cls: Type[T], src_dict: Dict[str, Any]) -> T:
        d = src_dict.copy()
        _tags: Optional[Dict[str, Any]] = d.pop("tags", None)
        tags: Optional[MetadataTags] = None
        if _tags is not None:
            tags = MetadataTags.from_dict(_tags)

        _properties: Optional[Dict[str, Any]] = d.pop("properties", None)
        properties: Optional[MetadataProperties] = None
        if _properties is not None:
            properties = MetadataProperties.from_dict(_properties)

        sensitive = []
        _sensitive = d.pop("sensitive", None)
        for sensitive_item in _sensitive or []:
            sensitive.append(sensitive_item)

        metadata = cls(
            tags=tags,
            properties=properties,
            sensitive=sensitive,
        )

        return metadata


@_attrs_define(frozen=True)
class SchemaReference:
    name: Optional[str]
    subject: Optional[str]
    version: Optional[int]

    def to_dict(self) -> Dict[str, Any]:
        name = self.name

        subject = self.subject

        version = self.version

        field_dict: Dict[str, Any] = {}
        if name is not None:
            field_dict["name"] = name
        if subject is not None:
            field_dict["subject"] = subject
        if version is not None:
            field_dict["version"] = version

        return field_dict

    @classmethod
    def from_dict(cls: Type[T], src_dict: Dict[str, Any]) -> T:
        d = src_dict.copy()
        name = d.pop("name", None)

        subject = d.pop("subject", None)

        version = d.pop("version", None)

        schema_reference = cls(
            name=name,
            subject=subject,
            version=version,
        )

        return schema_reference


class ConfigCompatibilityLevel(str, Enum):
    BACKWARD = "BACKWARD"
    BACKWARD_TRANSITIVE = "BACKWARD_TRANSITIVE"
    FORWARD = "FORWARD"
    FORWARD_TRANSITIVE = "FORWARD_TRANSITIVE"
    FULL = "FULL"
    FULL_TRANSITIVE = "FULL_TRANSITIVE"
    NONE = "NONE"

    def __str__(self) -> str:
        return str(self.value)


@_attrs_define
class ServerConfig:
    compatibility: Optional[ConfigCompatibilityLevel] = None
    compatibility_level: Optional[ConfigCompatibilityLevel] = None
    compatibility_group: Optional[str] = None
    default_metadata: Optional[Metadata] = None
    override_metadata: Optional[Metadata] = None
    default_rule_set: Optional[RuleSet] = None
    override_rule_set: Optional[RuleSet] = None

    def to_dict(self) -> Dict[str, Any]:
        _compatibility: Optional[str] = None
        if self.compatibility is not None:
            _compatibility = self.compatibility.value

        _compatibility_level: Optional[str] = None
        if self.compatibility_level is not None:
            _compatibility_level = self.compatibility_level.value

        compatibility_group = self.compatibility_group

        _default_metadata: Optional[Dict[str, Any]]
        if isinstance(self.default_metadata, Metadata):
            _default_metadata = self.default_metadata.to_dict()
        else:
            _default_metadata = self.default_metadata

        _override_metadata: Optional[Dict[str, Any]]
        if isinstance(self.override_metadata, Metadata):
            _override_metadata = self.override_metadata.to_dict()
        else:
            _override_metadata = self.override_metadata

        _default_rule_set: Optional[Dict[str, Any]]
        if isinstance(self.default_rule_set, RuleSet):
            _default_rule_set = self.default_rule_set.to_dict()
        else:
            _default_rule_set = self.default_rule_set

        _override_rule_set: Optional[Dict[str, Any]]
        if isinstance(self.override_rule_set, RuleSet):
            _override_rule_set = self.override_rule_set.to_dict()
        else:
            _override_rule_set = self.override_rule_set

        field_dict: Dict[str, Any] = {}
        if _compatibility is not None:
            field_dict["compatibility"] = _compatibility
        if _compatibility_level is not None:
            field_dict["compatibilityLevel"] = _compatibility_level
        if compatibility_group is not None:
            field_dict["compatibilityGroup"] = compatibility_group
        if _default_metadata is not None:
            field_dict["defaultMetadata"] = _default_metadata
        if _override_metadata is not None:
            field_dict["overrideMetadata"] = _override_metadata
        if _default_rule_set is not None:
            field_dict["defaultRuleSet"] = _default_rule_set
        if _override_rule_set is not None:
            field_dict["overrideRuleSet"] = _override_rule_set

        return field_dict

    @classmethod
    def from_dict(cls: Type[T], src_dict: Dict[str, Any]) -> T:
        d = src_dict.copy()
        _compatibility = d.pop("compatibility", None)
        compatibility: Optional[ConfigCompatibilityLevel]
        if _compatibility is None:
            compatibility = None
        else:
            compatibility = ConfigCompatibilityLevel(_compatibility)

        _compatibility_level = d.pop("compatibilityLevel", None)
        compatibility_level: Optional[ConfigCompatibilityLevel]
        if _compatibility_level is None:
            compatibility_level = None
        else:
            compatibility_level = ConfigCompatibilityLevel(_compatibility_level)

        compatibility_group = d.pop("compatibilityGroup", None)

        def _parse_default_metadata(data: object) -> Optional[Metadata]:
            if data is None:
                return data
            if not isinstance(data, dict):
                raise TypeError()
            return Metadata.from_dict(data)

        default_metadata = _parse_default_metadata(d.pop("defaultMetadata", None))

        def _parse_override_metadata(data: object) -> Optional[Metadata]:
            if data is None:
                return data
            if not isinstance(data, dict):
                raise TypeError()
            return Metadata.from_dict(data)

        override_metadata = _parse_override_metadata(d.pop("overrideMetadata", None))

        def _parse_default_rule_set(data: object) -> Optional[RuleSet]:
            if data is None:
                return data
            if not isinstance(data, dict):
                raise TypeError()
            return RuleSet.from_dict(data)

        default_rule_set = _parse_default_rule_set(d.pop("defaultRuleSet", None))

        def _parse_override_rule_set(data: object) -> Optional[RuleSet]:
            if data is None:
                return data
            if not isinstance(data, dict):
                raise TypeError()
            return RuleSet.from_dict(data)

        override_rule_set = _parse_override_rule_set(d.pop("overrideRuleSet", None))

        config = cls(
            compatibility=compatibility,
            compatibility_level=compatibility_level,
            compatibility_group=compatibility_group,
            default_metadata=default_metadata,
            override_metadata=override_metadata,
            default_rule_set=default_rule_set,
            override_rule_set=override_rule_set,
        )

        return config


@_attrs_define(frozen=True, cache_hash=True)
class Schema:
    """
    An unregistered schema.
    """

    schema_str: Optional[str]
    schema_type: Optional[str] = "AVRO"
    references: Optional[List[SchemaReference]] = _attrs_field(factory=list, hash=False)
    metadata: Optional[Metadata] = None
    rule_set: Optional[RuleSet] = None

    def to_dict(self) -> Dict[str, Any]:
        schema = self.schema_str

        schema_type = self.schema_type

        _references: Optional[List[Dict[str, Any]]] = []
        if self.references is not None:
            for references_item_data in self.references:
                references_item = references_item_data.to_dict()
                _references.append(references_item)

        _metadata: Optional[Dict[str, Any]] = None
        if isinstance(self.metadata, Metadata):
            _metadata = self.metadata.to_dict()

        _rule_set: Optional[Dict[str, Any]] = None
        if isinstance(self.rule_set, RuleSet):
            _rule_set = self.rule_set.to_dict()

        field_dict: Dict[str, Any] = {}
        if schema is not None:
            field_dict["schema"] = schema
        if schema_type is not None:
            field_dict["schemaType"] = schema_type
        if _references is not None:
            field_dict["references"] = _references
        if _metadata is not None:
            field_dict["metadata"] = _metadata
        if _rule_set is not None:
            field_dict["ruleSet"] = _rule_set

        return field_dict

    @classmethod
    def from_dict(cls: Type[T], src_dict: Dict[str, Any]) -> T:
        d = src_dict.copy()

        schema = d.pop("schema", None)

        schema_type = d.pop("schemaType", "AVRO")

        references = []
        _references = d.pop("references", None)
        for references_item_data in _references or []:
            references_item = SchemaReference.from_dict(references_item_data)

            references.append(references_item)

        def _parse_metadata(data: object) -> Optional[Metadata]:
            if data is None:
                return data
            if not isinstance(data, dict):
                raise TypeError()
            return Metadata.from_dict(data)

        metadata = _parse_metadata(d.pop("metadata", None))

        def _parse_rule_set(data: object) -> Optional[RuleSet]:
            if data is None:
                return data
            if not isinstance(data, dict):
                raise TypeError()
            return RuleSet.from_dict(data)

        rule_set = _parse_rule_set(d.pop("ruleSet", None))

        schema = cls(
            schema_str=schema,
            schema_type=schema_type,
            references=references,
            metadata=metadata,
            rule_set=rule_set,
        )

        return schema


@_attrs_define(frozen=True, cache_hash=True)
class RegisteredSchema:
    """
    An registered schema.
    """

    schema_id: Optional[int]
    guid: Optional[str]
    schema: Optional[Schema]
    subject: Optional[str]
    version: Optional[int]

    def to_dict(self) -> Dict[str, Any]:
        schema = self.schema

        schema_id = self.schema_id

        guid = self.guid

        subject = self.subject

        version = self.version

        field_dict: Dict[str, Any] = {}
        if schema is not None:
            field_dict = schema.to_dict()
        if schema_id is not None:
            field_dict["id"] = schema_id
        if guid is not None:
            field_dict["guid"] = guid
        if subject is not None:
            field_dict["subject"] = subject
        if version is not None:
            field_dict["version"] = version

        return field_dict

    @classmethod
    def from_dict(cls: Type[T], src_dict: Dict[str, Any]) -> T:
        d = src_dict.copy()

        schema = Schema.from_dict(d)

        schema_id = d.pop("id", None)

        guid = d.pop("guid", None)

        subject = d.pop("subject", None)

        version = d.pop("version", None)

        schema = cls(
            schema_id=schema_id,
            guid=guid,
            schema=schema,
            subject=subject,
            version=version,
        )

        return schema
