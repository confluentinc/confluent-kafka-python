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

import logging
from typing import List, Optional, Set, Dict, Any

from confluent_kafka.schema_registry import RegisteredSchema
from confluent_kafka.schema_registry.common.serde import ErrorAction, \
    FieldTransformer, Migration, NoneAction, RuleAction, \
    RuleConditionError, RuleContext, RuleError, SchemaId
from confluent_kafka.schema_registry.schema_registry_client import RuleMode, \
    Rule, RuleKind, Schema, RuleSet
from confluent_kafka.serialization import Serializer, Deserializer, \
    SerializationContext, SerializationError

__all__ = [
    'AsyncBaseSerde',
    'AsyncBaseSerializer',
    'AsyncBaseDeserializer',
]

log = logging.getLogger(__name__)


class AsyncBaseSerde(object):
    __slots__ = ['_use_schema_id', '_use_latest_version', '_use_latest_with_metadata',
                 '_registry', '_rule_registry', '_subject_name_func',
                 '_field_transformer']

    async def _get_reader_schema(self, subject: str, fmt: Optional[str] = None) -> Optional[RegisteredSchema]:
        if self._use_schema_id is not None:
            schema = await self._registry.get_schema(self._use_schema_id, subject, fmt)
            return await self._registry.lookup_schema(subject, schema, False, True)
        if self._use_latest_with_metadata is not None:
            return await self._registry.get_latest_with_metadata(
                subject, self._use_latest_with_metadata, True, fmt)
        if self._use_latest_version:
            return await self._registry.get_latest_version(subject, fmt)
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


class AsyncBaseSerializer(AsyncBaseSerde, Serializer):
    __slots__ = ['_auto_register', '_normalize_schemas', '_schema_id_serializer']


class AsyncBaseDeserializer(AsyncBaseSerde, Deserializer):
    __slots__ = ['_schema_id_deserializer']

    async def _get_writer_schema(
            self, schema_id: SchemaId, subject: Optional[str] = None,
            fmt: Optional[str] = None) -> Schema:
        if schema_id.id is not None:
            return await self._registry.get_schema(schema_id.id, subject, fmt)
        elif schema_id.guid is not None:
            return await self._registry.get_schema_by_guid(str(schema_id.guid), fmt)
        else:
            raise SerializationError("Schema ID or GUID is not set")

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
        return False

    async def _get_migrations(
        self, subject: str, source_info: Schema,
        target: RegisteredSchema, fmt: Optional[str]
    ) -> List[Migration]:
        source = await self._registry.lookup_schema(subject, source_info, False, True)
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
        versions = await self._get_schemas_between(subject, first, last, fmt)
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

    async def _get_schemas_between(
        self, subject: str, first: RegisteredSchema,
        last: RegisteredSchema, fmt: Optional[str] = None
    ) -> List[RegisteredSchema]:
        if last.version - first.version <= 1:
            return [first, last]
        version1 = first.version
        version2 = last.version
        result = [first]
        for i in range(version1 + 1, version2):
            result.append(await self._registry.get_version(subject, i, True, fmt))
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
