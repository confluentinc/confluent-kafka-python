#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright 2024 Confluent Inc.
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

__all__ = ['BaseSerializer',
           'BaseDeserializer',
           'FieldContext',
           'FieldRuleExecutor',
           'FieldTransform',
           'FieldTransformer',
           'FieldType',
           'ParsedSchemaCache',
           'RuleAction',
           'RuleContext',
           'RuleConditionError',
           'RuleError',
           'RuleExecutor']

import abc
import logging
from enum import Enum
from threading import Lock
from typing import Callable, List, Optional, Set, Dict, Any, TypeVar

from confluent_kafka.schema_registry import RegisteredSchema
from confluent_kafka.schema_registry.schema_registry_client import RuleMode, \
    Rule, RuleKind, Schema, RuleSet
from confluent_kafka.schema_registry.wildcard_matcher import wildcard_match
from confluent_kafka.serialization import Serializer, Deserializer, \
    SerializationContext, SerializationError


log = logging.getLogger(__name__)


class FieldType(str, Enum):
    RECORD = "RECORD"
    ENUM = "ENUM"
    ARRAY = "ARRAY"
    MAP = "MAP"
    COMBINED = "COMBINED"
    FIXED = "FIXED"
    STRING = "STRING"
    BYTES = "BYTES"
    INT = "INT"
    LONG = "LONG"
    FLOAT = "FLOAT"
    DOUBLE = "DOUBLE"
    BOOLEAN = "BOOLEAN"
    NULL = "NULL"


class FieldContext(object):
    __slots__ = ['containing_message', 'full_name', 'name', 'field_type', 'tags']

    def __init__(
        self, containing_message: Any, full_name: str, name: str,
        field_type: FieldType, tags: Set[str]
    ):
        self.containing_message = containing_message
        self.full_name = full_name
        self.name = name
        self.field_type = field_type
        self.tags = tags

    def is_primitive(self) -> bool:
        return self.field_type in (FieldType.INT, FieldType.LONG, FieldType.FLOAT,
                                   FieldType.DOUBLE, FieldType.BOOLEAN, FieldType.NULL,
                                   FieldType.STRING, FieldType.BYTES)

    def type_name(self) -> str:
        return self.field_type.name


class RuleContext(object):
    __slots__ = ['ser_ctx', 'source', 'target', 'subject', 'rule_mode', 'rule',
                 'index', 'rules', 'inline_tags', 'field_transformer', '_field_contexts']

    def __init__(
        self, ser_ctx: SerializationContext, source: Optional[Schema],
        target: Optional[Schema], subject: str, rule_mode: RuleMode, rule: Rule,
        index: int, rules: List[Rule], inline_tags: Optional[Dict[str, Set[str]]], field_transformer
    ):
        self.ser_ctx = ser_ctx
        self.source = source
        self.target = target
        self.subject = subject
        self.rule_mode = rule_mode
        self.rule = rule
        self.index = index
        self.rules = rules
        self.inline_tags = inline_tags
        self.field_transformer = field_transformer
        self._field_contexts: List[FieldContext] = []

    def get_parameter(self, name: str) -> Optional[str]:
        params = self.rule.params
        if params is not None:
            value = params.params.get(name)
            if value is not None:
                return value
        if (self.target is not None
                and self.target.metadata is not None
                and self.target.metadata.properties is not None):
            value = self.target.metadata.properties.properties.get(name)
            if value is not None:
                return value
        return None

    def _get_inline_tags(self, name: str) -> Set[str]:
        if self.inline_tags is None:
            return set()
        return self.inline_tags.get(name, set())

    def current_field(self) -> Optional[FieldContext]:
        if not self._field_contexts:
            return None
        return self._field_contexts[-1]

    def enter_field(
        self, containing_message: Any, full_name: str, name: str,
        field_type: FieldType, tags: Optional[Set[str]]
    ) -> FieldContext:
        all_tags = set(tags if tags is not None else self._get_inline_tags(full_name))
        all_tags.update(self.get_tags(full_name))
        field_context = FieldContext(containing_message, full_name, name, field_type, all_tags)
        self._field_contexts.append(field_context)
        return field_context

    def get_tags(self, full_name: str) -> Set[str]:
        result = set()
        if (self.target is not None
                and self.target.metadata is not None
                and self.target.metadata.tags is not None):
            tags = self.target.metadata.tags.tags
            for k, v in tags.items():
                if wildcard_match(full_name, k):
                    result.update(v)
        return result

    def exit_field(self):
        if self._field_contexts:
            self._field_contexts.pop()


FieldTransform = Callable[[RuleContext, FieldContext, Any], Any]


FieldTransformer = Callable[[RuleContext, FieldTransform, Any], Any]


class RuleBase(metaclass=abc.ABCMeta):
    def configure(self, client_conf: dict, rule_conf: dict):
        pass

    @abc.abstractmethod
    def type(self) -> str:
        raise NotImplementedError()

    def close(self):
        pass


class RuleExecutor(RuleBase):
    @abc.abstractmethod
    def transform(self, ctx: RuleContext, message: Any) -> Any:
        raise NotImplementedError()


class FieldRuleExecutor(RuleExecutor):
    @abc.abstractmethod
    def new_transform(self, ctx: RuleContext) -> FieldTransform:
        raise NotImplementedError()

    def transform(self, ctx: RuleContext, message: Any) -> Any:
        # TODO preserve source
        if ctx.rule_mode in (RuleMode.WRITE, RuleMode.UPGRADE):
            for i in range(ctx.index):
                other_rule = ctx.rules[i]
                if FieldRuleExecutor.are_transforms_with_same_tag(ctx.rule, other_rule):
                    # ignore this transform if an earlier one has the same tag
                    return message
        elif ctx.rule_mode == RuleMode.READ or ctx.rule_mode == RuleMode.DOWNGRADE:
            for i in range(ctx.index + 1, len(ctx.rules)):
                other_rule = ctx.rules[i]
                if FieldRuleExecutor.are_transforms_with_same_tag(ctx.rule, other_rule):
                    # ignore this transform if a later one has the same tag
                    return message
        return ctx.field_transformer(ctx, self.new_transform(ctx), message)

    @staticmethod
    def are_transforms_with_same_tag(rule1: Rule, rule2: Rule) -> bool:
        return (bool(rule1.tags)
                and rule1.kind == RuleKind.TRANSFORM
                and rule1.kind == rule2.kind
                and rule1.mode == rule2.mode
                and rule1.type == rule2.type
                and rule1.tags == rule2.tags)


class RuleAction(RuleBase):
    @abc.abstractmethod
    def run(self, ctx: RuleContext, message: Any, ex: Optional[Exception]):
        raise NotImplementedError()


class ErrorAction(RuleAction):
    def type(self) -> str:
        return 'ERROR'

    def run(self, ctx: RuleContext, message: Any, ex: Optional[Exception]):
        if ex is None:
            raise SerializationError()
        else:
            raise SerializationError() from ex


class NoneAction(RuleAction):
    def type(self) -> str:
        return 'NONE'

    def run(self, ctx: RuleContext, message: Any, ex: Optional[Exception]):
        pass


class RuleError(Exception):
    pass


class RuleConditionError(RuleError):
    def __init__(self, rule: Rule):
        super().__init__(RuleConditionError.error_message(rule))

    @staticmethod
    def error_message(rule: Rule) -> str:
        if rule.doc:
            return rule.doc
        elif rule.expr:
            return f"Rule expr failed: {rule.expr}"
        else:
            return f"Rule failed: {rule.name}"


class Migration(object):
    __slots__ = ['rule_mode', 'source', 'target']

    def __init__(
        self, rule_mode: RuleMode, source: Optional[RegisteredSchema],
        target: Optional[RegisteredSchema]
    ):
        self.rule_mode = rule_mode
        self.source = source
        self.target = target


class BaseSerde(object):
    __slots__ = ['_use_latest_version', '_use_latest_with_metadata',
                 '_registry', '_rule_registry', '_subject_name_func',
                 '_field_transformer']

    def _get_reader_schema(self, subject: str, fmt: str = None) -> Optional[RegisteredSchema]:
        if self._use_latest_with_metadata is not None:
            return self._registry.get_latest_with_metadata(
                subject, self._use_latest_with_metadata, True, fmt)
        if self._use_latest_version:
            return self._registry.get_latest_version(subject, fmt)
        return None

    def _execute_rules(
        self, ser_ctx: SerializationContext, subject: str,
        rule_mode: RuleMode,
        source: Optional[Schema], target: Optional[Schema],
        message: Any, inline_tags: Optional[Dict[str, Set[str]]],
        field_transformer: Optional[FieldTransformer]
    ) -> Any:
        if message is None or target is None:
            return message
        rules: Optional[List[Rule]] = None
        if rule_mode == RuleMode.UPGRADE:
            if target is not None and target.rule_set is not None:
                rules = target.rule_set.migration_rules
        elif rule_mode == RuleMode.DOWNGRADE:
            if source is not None and source.rule_set is not None:
                rules = source.rule_set.migration_rules
                rules = rules[:] if rules else []
                rules.reverse()
        else:
            if target is not None and target.rule_set is not None:
                rules = target.rule_set.domain_rules
                if rule_mode == RuleMode.READ:
                    # Execute read rules in reverse order for symmetry
                    rules = rules[:] if rules else []
                    rules.reverse()

        if not rules:
            return message

        for index in range(len(rules)):
            rule = rules[index]
            if self._is_disabled(rule):
                continue
            if rule.mode == RuleMode.WRITEREAD:
                if rule_mode != RuleMode.READ and rule_mode != RuleMode.WRITE:
                    continue
            elif rule.mode == RuleMode.UPDOWN:
                if rule_mode != RuleMode.UPGRADE and rule_mode != RuleMode.DOWNGRADE:
                    continue
            elif rule.mode != rule_mode:
                continue

            ctx = RuleContext(ser_ctx, source, target, subject, rule_mode, rule,
                              index, rules, inline_tags, field_transformer)
            rule_executor = self._rule_registry.get_executor(rule.type.upper())
            if rule_executor is None:
                self._run_action(ctx, rule_mode, rule, self._get_on_failure(rule), message,
                                 RuleError(f"Could not find rule executor of type {rule.type}"),
                                 'ERROR')
                return message
            try:
                result = rule_executor.transform(ctx, message)
                if rule.kind == RuleKind.CONDITION:
                    if not result:
                        raise RuleConditionError(rule)
                elif rule.kind == RuleKind.TRANSFORM:
                    message = result
                self._run_action(
                    ctx, rule_mode, rule,
                    self._get_on_failure(rule) if message is None else self._get_on_success(rule),
                    message, None,
                    'ERROR' if message is None else 'NONE')
            except SerializationError:
                raise
            except Exception as e:
                self._run_action(ctx, rule_mode, rule, self._get_on_failure(rule),
                                 message, e, 'ERROR')
        return message

    def _get_on_success(self, rule: Rule) -> Optional[str]:
        override = self._rule_registry.get_override(rule.type)
        if override is not None and override.on_success is not None:
            return override.on_success
        return rule.on_success

    def _get_on_failure(self, rule: Rule) -> Optional[str]:
        override = self._rule_registry.get_override(rule.type)
        if override is not None and override.on_failure is not None:
            return override.on_failure
        return rule.on_failure

    def _is_disabled(self, rule: Rule) -> Optional[bool]:
        override = self._rule_registry.get_override(rule.type)
        if override is not None and override.disabled is not None:
            return override.disabled
        return rule.disabled

    def _run_action(
        self, ctx: RuleContext, rule_mode: RuleMode, rule: Rule,
        action: Optional[str], message: Any,
        ex: Optional[Exception], default_action: str
    ):
        action_name = self._get_rule_action_name(rule, rule_mode, action)
        if action_name is None:
            action_name = default_action
        rule_action = self._get_rule_action(ctx, action_name)
        if rule_action is None:
            log.error("Could not find rule action of type %s", action_name)
            raise RuleError(f"Could not find rule action of type {action_name}")
        try:
            rule_action.run(ctx, message, ex)
        except SerializationError:
            raise
        except Exception as e:
            log.warning("Could not run post-rule action %s: %s", action_name, e)

    def _get_rule_action_name(
        self, rule: Rule, rule_mode: RuleMode, action_name: Optional[str]
    ) -> Optional[str]:
        if action_name is None or action_name == "":
            return None
        if rule.mode in (RuleMode.WRITEREAD, RuleMode.UPDOWN) and ',' in action_name:
            parts = action_name.split(',')
            if rule_mode in (RuleMode.WRITE, RuleMode.UPGRADE):
                return parts[0]
            elif rule_mode in (RuleMode.READ, RuleMode.DOWNGRADE):
                return parts[1]
        return action_name

    def _get_rule_action(self, ctx: RuleContext, action_name: str) -> Optional[RuleAction]:
        if action_name == 'ERROR':
            return ErrorAction()
        elif action_name == 'NONE':
            return NoneAction()
        return self._rule_registry.get_action(action_name)


class BaseSerializer(BaseSerde, Serializer):
    __slots__ = ['_auto_register', '_normalize_schemas']


class BaseDeserializer(BaseSerde, Deserializer):
    __slots__ = []

    def _has_rules(self, rule_set: RuleSet, mode: RuleMode) -> bool:
        if rule_set is None:
            return False
        if mode in (RuleMode.UPGRADE, RuleMode.DOWNGRADE):
            return any(rule.mode == mode or rule.mode == RuleMode.UPDOWN
                       for rule in rule_set.migration_rules or [])
        elif mode == RuleMode.UPDOWN:
            return any(rule.mode == mode for rule in rule_set.migration_rules or [])
        elif mode in (RuleMode.WRITE, RuleMode.READ):
            return any(rule.mode == mode or rule.mode == RuleMode.WRITEREAD
                       for rule in rule_set.domain_rules or [])
        elif mode == RuleMode.WRITEREAD:
            return any(rule.mode == mode for rule in rule_set.migration_rules or [])

    def _get_migrations(
        self, subject: str, source_info: Schema,
        target: RegisteredSchema, fmt: Optional[str]
    ) -> List[Migration]:
        source = self._registry.lookup_schema(subject, source_info, False, True)
        migrations = []
        if source.version < target.version:
            migration_mode = RuleMode.UPGRADE
            first = source
            last = target
        elif source.version > target.version:
            migration_mode = RuleMode.DOWNGRADE
            first = target
            last = source
        else:
            return migrations
        previous: Optional[RegisteredSchema] = None
        versions = self._get_schemas_between(subject, first, last, fmt)
        for i in range(len(versions)):
            version = versions[i]
            if i == 0:
                previous = version
                continue
            if version.schema.rule_set is not None and self._has_rules(version.schema.rule_set, migration_mode):
                if migration_mode == RuleMode.UPGRADE:
                    migration = Migration(migration_mode, previous, version)
                else:
                    migration = Migration(migration_mode, version, previous)
                migrations.append(migration)
            previous = version
        if migration_mode == RuleMode.DOWNGRADE:
            migrations.reverse()
        return migrations

    def _get_schemas_between(
        self, subject: str, first: RegisteredSchema,
        last: RegisteredSchema, fmt: str = None
    ) -> List[RegisteredSchema]:
        if last.version - first.version <= 1:
            return [first, last]
        version1 = first.version
        version2 = last.version
        result = [first]
        for i in range(version1 + 1, version2):
            result.append(self._registry.get_version(subject, i, True, fmt))
        result.append(last)
        return result

    def _execute_migrations(
        self, ser_ctx: SerializationContext, subject: str,
        migrations: List[Migration], message: Any
    ) -> Any:
        for migration in migrations:
            message = self._execute_rules(ser_ctx, subject, migration.rule_mode,
                                          migration.source.schema, migration.target.schema,
                                          message, None, None)
        return message


T = TypeVar("T")


class ParsedSchemaCache(object):
    """
    Thread-safe cache for parsed schemas
    """

    def __init__(self):
        self.lock = Lock()
        self.parsed_schemas = {}

    def set(self, schema: Schema, parsed_schema: T):
        """
        Add a Schema identified by schema_id to the cache.

        Args:
            schema (Schema): The schema

            parsed_schema (Any): The parsed schema
        """

        with self.lock:
            self.parsed_schemas[schema] = parsed_schema

    def get_parsed_schema(self, schema: Schema) -> Optional[T]:
        """
        Get the parsed schema associated with the schema

        Args:
            schema (Schema): The schema

        Returns:
            The parsed schema if known; else None
        """

        with self.lock:
            return self.parsed_schemas.get(schema, None)

    def clear(self):
        """
        Clear the cache.
        """

        with self.lock:
            self.parsed_schemas.clear()
