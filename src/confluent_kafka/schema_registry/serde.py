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
           'BaseDeserializer']

from enum import IntEnum
from typing import Callable

from confluent_kafka.schema_registry import RegisteredSchema
from confluent_kafka.schema_registry.schema_registry_client import RuleMode
from confluent_kafka.serialization import Serializer, Deserializer, \
    SerializationContext


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
                 '_registry', '_subject_name_func']

    def _get_reader_schema(self, subject: str) -> RegisteredSchema:
        latest_schema = None
        if self._use_latest_with_metadata is not None:
            latest_schema = self._registry.get_latest_version_with_metadata(
                subject, self._use_latest_with_metadata)
        if self._use_latest_version:
            latest_schema = self._registry.get_latest_version(subject)
        return latest_schema

    def _execute_rules(self, ctx: SerializationContext, subject: str,
        rule_mode: RuleMode, source: RegisteredSchema, target: RegisteredSchema,
        message: object, field_transformer: FieldTransformer) -> object:
        if message is None or target is None:
            return None
        rules = None
        if rule_mode == RuleMode.UPGRADE:
            if target.schema.rule_set is not None:
                rules = target.schema.rule_set.migration_rules
        elif rule_mode == RuleMode.DOWNGRADE:
            if source.schema.rule_set is not None:
                rules = source.schema.rule_set.migration_rules
                if rules is not None:
                    rules = rules[:].reverse()

        # TODO
        return message


class BaseSerializer(BaseSerde, Serializer):
    __slots__ = ['_auto_register', '_normalize_schemas']


class BaseDeserializer(BaseSerde, Deserializer):
    __slots__ = []
