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
           'RuleAction',
           'RuleContext',
           'RuleExecutor']

import logging
from enum import Enum
from typing import Callable, List, Optional, Set, Dict

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
    __slots__ = ['containing_message', 'full_name', 'name', 'type', 'tags']

    def __init__(self, containing_message: object, full_name: str, name: str,
        type: FieldType, tags: Set[str]):
        self.containing_message = containing_message
        self.full_name = full_name
        self.name = name
        self.type = type
        self.tags = tags

    def _is_primitive(self) -> bool:
        return self.type in (FieldType.INT, FieldType.LONG, FieldType.FLOAT,
                             FieldType.DOUBLE, FieldType.BOOLEAN, FieldType.NULL,
                             FieldType.STRING, FieldType.BYTES)

    def _type_name(self) -> str:
        return self.type.name


class RuleContext(object):
    __slots__ = ['ser_ctx', 'source', 'target', 'subject', 'rule_mode', 'rule',
                 'index', 'rules', 'inline_tags', 'field_transformer', '_field_contexts']

    def __init__(self, ser_ctx: SerializationContext, source: Optional[RegisteredSchema],
        target: Optional[RegisteredSchema], subject: str, rule_mode: RuleMode, rule: Rule,
        index: int, rules: List[Rule], inline_tags: Optional[Dict[str, Set[str]]], field_transformer):
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
        if (self.target is not None and
            self.target.schema.metadata is not None and
            self.target.schema.metadata.properties is not None):
            value = self.target.schema.metadata.properties.properties.get(name)
            if value is not None:
                return value
        return None

    def _get_inline_tags(self, name: str) -> Set[str]:
        if self.inline_tags is None:
            return set()
        return self.inline_tags.get(name, set())

    def current_field(self) -> Optional[FieldContext]:
        if len(self._field_contexts) == 0:
            return None
        return self._field_contexts[-1]

    def enter_field(self, containing_message: object, full_name: str, name: str,
        field_type: FieldType, tags: Optional[List[str]]) -> FieldContext:
        all_tags = set(tags if tags is not None else self._get_inline_tags(full_name))
        all_tags.update(self.get_tags(full_name))
        field_context = FieldContext(containing_message, full_name, name, field_type, all_tags)
        self._field_contexts.append(field_context)
        return field_context

    def get_tags(self, full_name: str) -> Set[str]:
        result = set()
        if (self.target is not None and
            self.target.schema.metadata is not None and
            self.target.schema.metadata.tags is not None):
            tags = self.target.schema.metadata.tags.tags
            for k, v in tags.items():
                if wildcard_match(full_name, k):
                    tags.update(v)
        return result

    def exit_field(self):
        if len(self._field_contexts) > 0:
            self._field_contexts.pop()


FieldTransform = Callable[[RuleContext, FieldContext, object], object]


FieldTransformer = Callable[[RuleContext, FieldTransform, object], object]


class RuleBase(object):
    def configure(self, client_conf, conf):
        pass

    def type(self) -> str:
        raise NotImplementedError

    def close(self):
        pass


class RuleExecutor(RuleBase):
    def transform(self, ctx: RuleContext, message: object) -> object:
        pass


class FieldRuleExecutor(RuleExecutor):
    def new_transform(self, ctx: RuleContext) -> FieldTransform:
        pass

    def transform(self, ctx: RuleContext, message: object) -> object:
        # TODO preserve source
        if ctx.rule_mode == RuleMode.WRITE or ctx.rule_mode == RuleMode.UPGRADE:
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
        return (rule1.tags is not None and len(rule1.tags) > 0
                and rule1.kind == RuleKind.TRANSFORM
                and rule1.kind == rule2.kind
                and rule1.mode == rule2.mode
                and rule1.type == rule2.type
                and rule1.tags == rule2.tags)


class RuleAction(RuleBase):
    def run(self, ctx: RuleContext, message: object, ex: Optional[Exception]):
        pass


class ErrorAction(RuleAction):
    def type(self) -> str:
        return 'ERROR'

    def run(self, ctx: RuleContext, message: object, ex: Optional[Exception]):
        raise SerializationError(f"{ex}")


class NoneAction(RuleAction):
    def type(self) -> str:
        return 'NONE'

    def run(self, ctx: RuleContext, message: object, ex: Optional[Exception]):
        pass


class RuleError(Exception):
    pass


class RuleConditionError(RuleError):
    def __init__(self, rule: Rule):
        super().__init__(RuleConditionError.error_message(rule))

    @staticmethod
    def error_message(rule: Rule) -> str:
        if rule.doc is not None:
            return rule.doc
        elif rule.expr is not None:
            return f"Rule expr failed: {rule.expr}"
        else:
            return f"Rule failed: {rule.name}"


class Migration(object):
    __slots__ = ['rule_mode', 'source', 'target']

    def __init__(self, rule_mode: RuleMode, source: Optional[RegisteredSchema],
        target: Optional[RegisteredSchema]):
        self.rule_mode = rule_mode
        self.source = source
        self.target = target


class BaseSerde(object):
    __slots__ = ['_use_latest_version', '_use_latest_with_metadata',
                 '_registry', '_rule_registry', '_subject_name_func',
                 '_field_transformer']

    def _get_reader_schema(self, subject: str) -> RegisteredSchema:
        latest_schema = None
        if self._use_latest_with_metadata is not None:
            latest_schema = self._registry.get_latest_version_with_metadata(
                subject, self._use_latest_with_metadata)
        if self._use_latest_version:
            latest_schema = self._registry.get_latest_version(subject)
        return latest_schema

    def _execute_rules(self, ser_ctx: SerializationContext, subject: str,
        rule_mode: RuleMode,
        source: Optional[RegisteredSchema], target: Optional[RegisteredSchema],
        message: object, inline_tags: Optional[Dict[str, Set[str]]]) -> object:
        if message is None or target is None:
            return None
        rules: Optional[List[Rule]] = None
        if rule_mode == RuleMode.UPGRADE:
            if target.schema.rule_set is not None:
                rules = target.schema.rule_set.migration_rules
        elif rule_mode == RuleMode.DOWNGRADE:
            if source.schema.rule_set is not None:
                rules = source.schema.rule_set.migration_rules
                if rules is not None:
                    rules = rules[:].reverse()

        if rules is None:
            return message

        for index in range(len(rules)):
            rule = rules[index]
            if rule.disabled:
                continue
            if rule.mode == RuleMode.WRITEREAD:
                if rule_mode != RuleMode.READ and rule_mode != RuleMode.WRITE:
                    continue
                continue
            elif rule.mode == RuleMode.UPDOWN:
                if rule_mode != RuleMode.UPGRADE and rule_mode != RuleMode.DOWNGRADE:
                    continue
            elif rule.mode != rule_mode:
                continue

            ctx = RuleContext(ser_ctx, source, target, subject, rule_mode, rule,
                              index, rules, inline_tags, self._field_transform)
            rule_executor = self._rule_registry.get_executor(rule.type.upper())
            if rule_executor is None:
                self._run_action(ctx, rule_mode, rule, rule.on_failure, message,
                                 RuleError(f"Could not find rule executor of type {rule.type}"),
                                 'ERROR')
                return message
            try:
                result = rule_executor.transform(ctx, message)
                if rule.kind == RuleKind.CONDITION:
                    if not result:
                        raise RuleConditionError(rule)
                    break
                elif rule.kind == RuleKind.TRANSFORM:
                    message = result
                    break
                self._run_action(ctx, rule_mode, rule,
                                 rule.on_failure if message is None else rule.on_success,
                                 message, None,
                                 'ERROR' if message is None else 'NONE')
            except SerializationError:
                raise
            except Exception as e:
                self._run_action(ctx, rule_mode, rule, rule.on_failure, message, e, 'ERROR')
        return message

    def _run_action(self, ctx: RuleContext, rule_mode: RuleMode, rule: Rule,
        action: Optional[str], message: object,
        ex: Optional[Exception], default_action: str):
        action_name = self._get_rule_action_name(rule, rule_mode, action)
        if action_name is None:
            action_name = default_action
        rule_action = self._get_rule_action(self._rule_registry, action_name)
        if rule_action is None:
            raise RuleError(f"Could not find rule action of type {action_name}")
        try:
            rule_action.run(ctx, message, ex)
        except SerializationError:
            raise
        except Exception as e:
            log.warning(f"Could not run post-rule action {action_name}: {e}")

    def _get_rule_action_name(self, rule: Rule, rule_mode: RuleMode,
        action_name: Optional[str]) -> Optional[str]:
        if action_name is None or action_name == "":
            return None
        if (rule.mode == RuleMode.WRITEREAD or rule.mode == RuleMode.UPDOWN) and ',' in action_name:
            parts = action_name.split(',')
            if rule_mode == RuleMode.WRITE or rule_mode == RuleMode.UPGRADE:
                return parts[0]
            elif rule_mode == RuleMode.READ or rule_mode == RuleMode.DOWNGRADE:
                return parts[1]
        return action_name

    def _get_rule_action(self, ctx: RuleContext, action_name: str) -> Optional[RuleAction]:
        if action_name == 'ERROR':
            return ErrorAction()
        elif action_name == 'NONE':
            return NoneAction()
        return self._rule_registry.get_action(action_name)

    def _field_transform(self, rule_ctx, message, transform):
        pass


class BaseSerializer(BaseSerde, Serializer):
    __slots__ = ['_auto_register', '_normalize_schemas']


class BaseDeserializer(BaseSerde, Deserializer):
    __slots__ = []

    def _has_rules(self, rule_set: RuleSet, mode: RuleMode) -> bool:
        if rule_set is None:
            return False
        if mode == RuleMode.UPGRADE or mode == RuleMode.DOWNGRADE:
            return any(rule.mode == mode or rule.mode == RuleMode.UPDOWN
                       for rule in rule_set.migration_rules)
        elif mode == RuleMode.UPDOWN:
            return any(rule.mode == mode for rule in rule_set.migration_rules)
        elif mode == RuleMode.WRITE or mode == RuleMode.READ:
            return any(rule.mode == mode or rule.mode == RuleMode.WRITEREAD
                       for rule in rule_set.domain_rules)
        elif mode == RuleMode.WRITEREAD:
            return any(rule.mode == mode for rule in rule_set.migration_rules)

    def _get_migrations(self, subject: str, source_info: Schema,
        target: RegisteredSchema, format: Optional[str]) -> List[Migration]:
        version = self._registry.lookup_schema(subject, source_info)
        source = RegisteredSchema(0, source_info, subject, version)
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
        versions = self._get_schemas_between(subject, first, last, format)
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

    def _get_schemas_between(self, subject: str, first: RegisteredSchema,
        last: RegisteredSchema, format: str = None) -> List[RegisteredSchema]:
        if last.version - first.version <= 1:
            return [first, last]
        version1 = first.version
        version2 = last.version
        result = [first]
        for i in range(version1 + 1, version2):
            result.append(self._registry.get_version(subject, i, format))
        result.append(last)
        return result

    def _execute_migrations(self, ser_ctx: SerializationContext, subject: str,
        migrations: List[Migration], message: object) -> object:
        for migration in migrations:
            message = self._execute_rules(ser_ctx, subject, migration.rule_mode,
                                          migration.source, migration.target, message, None)
        return message







