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
           'RuleExecutor']

import logging
from enum import IntEnum
from typing import Callable, List, Optional

from confluent_kafka.schema_registry import RegisteredSchema
from confluent_kafka.schema_registry.schema_registry_client import RuleMode, \
    Rule, RuleKind
from confluent_kafka.serialization import Serializer, Deserializer, \
    SerializationContext, SerializationError


log = logging.getLogger(__name__)

class RuleContext(object):
    __slots__ = ['ctx', 'subject', 'rule_mode', 'source', 'target', 'message', 'field_transformer']

    def __init__(self, ctx, subject, rule_mode, source, target, message, field_transformer):
        self.ctx = ctx
        self.subject = subject
        self.rule_mode = rule_mode
        self.source = source
        self.target = target
        self.message = message
        self.field_transformer = field_transformer


class RuleBase(object):

    def configure(self, client_conf, conf):
        pass

    def type(self) -> str:
        raise NotImplementedError

    def close(self):
        pass


class RuleExecutor(RuleBase):
    pass


class FieldRuleExecutor(RuleExecutor):
    pass


class FieldContext(object):
    pass


class FieldType(IntEnum):
    TYPE_RECORD = 1
    TYPE_ENUM = 2
    TYPE_ARRAY = 3
    TYPE_MAP = 4
    TYPE_COMBINED = 5
    TYPE_FIXED = 6
    TYPE_STRING = 7
    TYPE_BYTES = 8
    TYPE_INT = 9
    TYPE_LONG = 10
    TYPE_FLOAT = 11
    TYPE_DOUBLE = 12
    TYPE_BOOLEAN = 13
    TYPE_NULL = 14


FieldTransform = Callable[[RuleContext, FieldContext, object], object]


FieldTransformer = Callable[[RuleContext, FieldTransform, object], object]


class RuleAction(object):
    pass


class ErrorAction(RuleAction):
    pass


class NoneAction(RuleAction):
    pass


class RuleError(Exception):
    pass


class RuleConditionError(RuleError):
    pass


class Migration(object):
    pass


class BaseSerde(object):
    __slots__ = ['_use_latest_version', '_use_latest_with_metadata',
                 '_registry', '_rule_registry', '_subject_name_func']


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
        message: object, field_transformer: FieldTransformer) -> object:
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

        for rule in rules:
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

            ctx = RuleContext(ser_ctx, subject, rule_mode, source, target, message, field_transformer)
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

    def _run_action(self, ctx: RuleContext, rule_mode: RuleMode, rule: Rule, action: str, message: object,
        ex: Exception, default_action: str):
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

    def _get_rule_action_name(self, rule: Rule, rule_mode: RuleMode, action_name: str) -> Optional[str]:
        if action_name is None or action_name == "":
            return None
        if (rule.mode == RuleMode.WRITEREAD or rule.mode == RuleMode.UPDOWN) and ',' in action_name:
            parts = action_name.split(',')
            if rule_mode == RuleMode.WRITE or rule_mode == RuleMode.UPGRADE:
                return parts[0]
            elif rule_mode == RuleMode.READ or rule_mode == RuleMode.DOWNGRADE:
                return parts[1]
        return action_name

    def _get_rule_action(self, ctx: RuleContext, action_name: str) -> RuleAction:
        if action_name == 'ERROR':
            return ErrorAction()
        elif action_name == 'NONE':
            return NoneAction()
        return self._rule_registry.get_action(action_name)


class BaseSerializer(BaseSerde, Serializer):
    __slots__ = ['_auto_register', '_normalize_schemas']


class BaseDeserializer(BaseSerde, Deserializer):
    __slots__ = []
