#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright 2025 Confluent Inc.
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

from confluent_kafka.schema_registry.common.schema_registry_client import Rule, RuleKind, RuleMode, RuleParams
from confluent_kafka.schema_registry.common.serde import RuleContext
from confluent_kafka.schema_registry.rules.encryption.dek_registry.mock_dek_registry_client import (
    MockDekRegistryClient,
)
from confluent_kafka.schema_registry.rules.encryption.encrypt_executor import (
    ENCRYPT_KEK_NAME,
    EncryptionExecutor,
    EncryptionExecutorTransform,
)


def _new_context(subject: str) -> RuleContext:
    rule = Rule(
        name="rule1",
        doc=None,
        kind=RuleKind.TRANSFORM,
        mode=RuleMode.WRITE,
        type="ENCRYPT_PAYLOAD",
        tags=None,
        params=RuleParams(params={ENCRYPT_KEK_NAME: "kek1"}),
        expr=None,
        on_success=None,
        on_failure=None,
        disabled=False,
    )
    return RuleContext(
        enabled_env=None,
        ser_ctx=None,
        source=None,
        target=None,
        subject=subject,
        rule_mode=RuleMode.WRITE,
        rule=rule,
        index=0,
        rules=[rule],
        inline_tags=None,
        field_transformer=None,
    )


def test_get_or_create_kek_uses_context_from_subject():
    client = MockDekRegistryClient({"url": "mock://"})
    # Pre-register the same kek name under two different contexts, with a
    # different kms_key_id each, so a wrong (or dropped) context shows up as
    # a mismatched kms_key_id rather than just "it didn't raise".
    client.register_kek("kek1", "local-kms", "myctxkey", context=".myctx")
    client.register_kek("kek1", "local-kms", "defaultkey", context=None)

    executor = EncryptionExecutor()
    executor.client = client
    transform = EncryptionExecutorTransform(executor, cryptor=None, kek_name="kek1", dek_expiry_days=0)

    # Context-qualified subject: the context should be parsed out of the
    # subject and threaded through to the dek registry client, not dropped.
    kek = transform._get_or_create_kek(_new_context(":.myctx:widget-value"))
    assert kek.kms_key_id == "myctxkey"

    # Unqualified subject (default context): the context should normalize to
    # None rather than being looked up under the literal "." context.
    kek = transform._get_or_create_kek(_new_context("widget-value"))
    assert kek.kms_key_id == "defaultkey"
