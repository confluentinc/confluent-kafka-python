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
#
from typing import Optional

from .rules.cel.cel_executor import CelExecutor
from .rules.cel.cel_field_executor import CelFieldExecutor
from .rules.encryption.awskms.aws_driver import AwsKmsDriver
from .rules.encryption.azurekms.azure_driver import AzureKmsDriver
from .rules.encryption.gcpkms.gcp_driver import GcpKmsDriver
from .rules.encryption.hcvault.hcvault_driver import HcVaultKmsDriver
from .rules.encryption.localkms.local_driver import LocalKmsDriver
from .rules.encryption.encrypt_executor import FieldEncryptionExecutor
from .rules.encryption.kms_driver_registry import (
  KmsDriver,
  register_kms_driver,
  get_kms_driver,
  reset_kms_drivers
)
from .rules.jsonata.jsonata_executor import JsonataExecutor
from .rule_registry import RuleRegistry
from .schema_registry_client import (
  ConfigCompatibilityLevel,
  Metadata,
  MetadataProperties,
  MetadataTags,
  RegisteredSchema,
  Rule,
  RuleKind,
  RuleMode,
  RuleParams,
  RuleSet,
  Schema,
  SchemaRegistryClient,
  SchemaRegistryError,
  SchemaReference,
  ServerConfig
)
from .serde import (
  RuleConditionError,
  RuleError
)

_MAGIC_BYTE = 0

__all__ = [
  "ConfigCompatibilityLevel",
  "Metadata",
  "MetadataProperties",
  "MetadataTags",
  "RegisteredSchema",
  "Rule",
  "RuleKind",
  "RuleMode",
  "RuleParams",
  "RuleSet",
  "Schema",
  "SchemaRegistryClient",
  "SchemaRegistryError",
  "SchemaReference",
  "ServerConfig",
  "topic_subject_name_strategy",
  "topic_record_subject_name_strategy",
  "record_subject_name_strategy"
]


def topic_subject_name_strategy(ctx: 'SerializationContext',
    record_name: Optional[str]) -> Optional[str]:
    """
    Constructs a subject name in the form of {topic}-key|value.

    Args:
        ctx (SerializationContext): Metadata pertaining to the serialization
            operation.

        record_name (Optional[str]): Record name.

    """
    return ctx.topic + "-" + ctx.field


def topic_record_subject_name_strategy(ctx: 'SerializationContext',
    record_name: Optional[str]) -> Optional[str]:
    """
    Constructs a subject name in the form of {topic}-{record_name}.

    Args:
        ctx (SerializationContext): Metadata pertaining to the serialization
            operation.

        record_name (Optional[str]): Record name.

    """
    return ctx.topic + "-" + record_name if record_name is not None else None


def record_subject_name_strategy(ctx: 'SerializationContext',
    record_name: Optional[str]) -> Optional[str]:
    """
    Constructs a subject name in the form of {record_name}.

    Args:
        ctx (SerializationContext): Metadata pertaining to the serialization
            operation.

        record_name (Optional[str]): Record name.

    """
    return record_name if record_name is not None else None


def reference_subject_name_strategy(ctx: 'SerializationContext',
    schema_ref: SchemaReference) -> Optional[str]:
    """
    Constructs a subject reference name in the form of {reference name}.

    Args:
        ctx (SerializationContext): Metadata pertaining to the serialization
            operation.

        schema_ref (SchemaReference): SchemaReference instance.

    """
    return schema_ref.name if schema_ref is not None else None
