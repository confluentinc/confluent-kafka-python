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

from confluent_kafka.schema_registry import RegisteredSchema
from confluent_kafka.schema_registry.schema_registry_client import RuleMode, \
    FieldTransformer
from confluent_kafka.serialization import Serializer, Deserializer, \
    SerializationContext


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
        # TODO
        return message


class BaseSerializer(BaseSerde, Serializer):
    __slots__ = ['_auto_register', '_normalize_schemas']


class BaseDeserializer(BaseSerde, Deserializer):
    __slots__ = []
