#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
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
#
import json
import struct
import uuid

import pytest

from confluent_kafka.schema_registry._sync.serde import BaseSerde
from confluent_kafka.schema_registry.common.avro import get_inline_tags
from confluent_kafka.schema_registry.common.avro import transform as avro_transform
from confluent_kafka.schema_registry.common.schema_registry_client import Metadata, MetadataTags
from confluent_kafka.schema_registry.rules.dlq.dlq_action import DlqAction, FieldRedactionExecutor
from confluent_kafka.schema_registry.schema_registry_client import Rule, RuleKind, RuleMode, RuleParams, Schema
from confluent_kafka.schema_registry.serde import RuleContext
from confluent_kafka.serialization import MessageField, SerializationContext, SerializationError

_TOPIC = "topic1"
_SUBJECT = _TOPIC + "-value"


class RecordingProducer:
    def __init__(self, produce_error=None):
        self.records = []
        self.flush_count = 0
        self.poll_count = 0
        self.produce_error = produce_error

    def produce(self, topic, value=None, key=None, headers=None, on_delivery=None, **kwargs):
        if self.produce_error is not None:
            raise self.produce_error
        self.records.append((topic, key, value, headers))

    def poll(self, timeout=0):
        self.poll_count += 1
        return 0

    def flush(self, timeout=None):
        self.flush_count += 1
        return 0


def _make_rule(
    name='myrule',
    rule_type='CEL',
    kind=RuleKind.CONDITION,
    mode=RuleMode.WRITE,
    tags=None,
    params=None,
    on_failure=None,
):
    return Rule(
        name,
        "",
        kind,
        mode,
        rule_type,
        tags,
        RuleParams(params) if params is not None else None,
        None,
        None,
        on_failure,
        False,
    )


def _make_ctx(
    rule=None,
    rules=None,
    rule_mode=RuleMode.WRITE,
    ser_ctx=None,
    target=None,
    inline_tags=None,
    field_transformer=None,
    original_key=None,
    original_value=None,
):
    if rule is None:
        rule = _make_rule()
    if rules is None:
        rules = [rule]
    if ser_ctx is None:
        ser_ctx = SerializationContext(_TOPIC, MessageField.VALUE)
    return RuleContext(
        None,
        ser_ctx,
        None,
        target,
        _SUBJECT,
        rule_mode,
        rule,
        0,
        rules,
        inline_tags,
        field_transformer,
        original_key,
        original_value,
    )


def _headers_dict(record):
    return {k: v for k, v in record[3]}


def test_convert_to_bytes_matrix():
    action = DlqAction({'dlq.topic': 'dlq'})
    ctx = _make_ctx()
    convert = action._convert_to_bytes

    assert convert(ctx, None) is None
    assert convert(ctx, b'raw') == b'raw'
    assert convert(ctx, bytearray(b'raw')) == b'raw'
    assert convert(ctx, memoryview(b'raw')) == b'raw'
    assert convert(ctx, 'str') == b'str'
    u = uuid.uuid4()
    assert convert(ctx, u) == str(u).encode('utf-8')
    # bool falls through to the JSON path
    assert convert(ctx, True) == b'true'
    assert convert(ctx, 7) == struct.pack('>q', 7)
    # signed int64 boundaries still take the fixed-width path
    assert convert(ctx, 2**63 - 1) == struct.pack('>q', 2**63 - 1)
    assert convert(ctx, -(2**63)) == struct.pack('>q', -(2**63))
    # ints outside the signed int64 range fall through to the JSON path instead
    # of raising struct.error and dropping the whole DLQ record
    assert convert(ctx, 2**63) == b'9223372036854775808'
    assert convert(ctx, -(2**63) - 1) == b'-9223372036854775809'
    assert convert(ctx, -1.5) == struct.pack('>d', -1.5)
    assert convert(ctx, [1, 2]) == b'[1, 2]'
    # bytes values inside structured messages are rendered as latin-1 strings
    assert json.loads(convert(ctx, {'a': b'\xe9', 'b': 1})) == {'a': '\xe9', 'b': 1}


def test_topic_resolution_precedence():
    producer = RecordingProducer()
    ctx = _make_ctx()

    # topic from the constructor conf
    action = DlqAction({'dlq.topic': 'conf-topic', 'producer': producer})
    with pytest.raises(SerializationError):
        action.run(ctx, None, None)
    assert producer.records[-1][0] == 'conf-topic'

    # rule_conf overrides the constructor conf; non-dlq keys are not merged
    action.configure({'url': 'mock://'}, {'dlq.topic': 'rule-conf-topic', 'secret': 'mysecret'})
    with pytest.raises(SerializationError):
        action.run(ctx, None, None)
    assert producer.records[-1][0] == 'rule-conf-topic'
    assert 'secret' not in action._conf

    # rule params via ctx.get_parameter when no topic is configured
    action = DlqAction({'producer': producer})
    rule = _make_rule(params={'dlq.topic': 'param-topic'})
    with pytest.raises(SerializationError):
        action.run(_make_ctx(rule=rule), None, None)
    assert producer.records[-1][0] == 'param-topic'

    # no topic anywhere
    action = DlqAction({'producer': producer})
    with pytest.raises(SerializationError, match="Could not send to DLQ as no topic is configured"):
        action.run(ctx, None, None)


def test_populate_headers():
    producer = RecordingProducer()
    action = DlqAction({'dlq.topic': 'dlq', 'producer': producer})
    incoming = [('h1', b'v1')]
    ser_ctx = SerializationContext(_TOPIC, MessageField.VALUE, incoming)
    rule = _make_rule(name='my-rule')
    ctx = _make_ctx(rule=rule, ser_ctx=ser_ctx, original_value='payload')

    with pytest.raises(SerializationError):
        action.run(ctx, None, ValueError("boom"))

    topic, key, value, headers = producer.records[0]
    assert key is None
    assert value == b'payload'
    assert headers[0] == ('h1', b'v1')
    headers_dict = dict(headers)
    assert headers_dict[DlqAction.RULE_NAME] == b'my-rule'
    assert headers_dict[DlqAction.RULE_MODE] == b'WRITE'
    assert headers_dict[DlqAction.RULE_SUBJECT] == _SUBJECT.encode('utf-8')
    assert headers_dict[DlqAction.RULE_TOPIC] == _TOPIC.encode('utf-8')
    assert headers_dict[DlqAction.RULE_EXCEPTION] == b'boom'
    # the caller's headers are copied, not mutated
    assert incoming == [('h1', b'v1')]

    # dict headers are normalized, and no exception header without an exception
    ser_ctx = SerializationContext(_TOPIC, MessageField.VALUE, {'h1': 'v1'})
    with pytest.raises(SerializationError):
        action.run(_make_ctx(rule=rule, ser_ctx=ser_ctx), None, None)
    headers = producer.records[1][3]
    assert ('h1', b'v1') in headers
    assert DlqAction.RULE_EXCEPTION not in dict(headers)


def test_always_raises_and_chains_cause():
    producer = RecordingProducer()
    action = DlqAction({'dlq.topic': 'dlq', 'producer': producer})
    cause = ValueError("boom")

    with pytest.raises(SerializationError, match="Rule failed: myrule") as exc_info:
        action.run(_make_ctx(original_value='v'), None, cause)
    assert exc_info.value.__cause__ is cause
    assert len(producer.records) == 1

    with pytest.raises(SerializationError, match="Rule failed: myrule") as exc_info:
        action.run(_make_ctx(original_value='v'), None, None)
    assert exc_info.value.__cause__ is None


def test_produce_failure_is_swallowed():
    producer = RecordingProducer(produce_error=RuntimeError("kafka down"))
    action = DlqAction({'dlq.topic': 'dlq', 'producer': producer})
    # the produce failure is logged, and the rule failure is still raised
    with pytest.raises(SerializationError, match="Rule failed: myrule"):
        action.run(_make_ctx(original_value='v'), None, None)
    assert producer.records == []


def test_auto_flush():
    producer = RecordingProducer()
    action = DlqAction({'dlq.topic': 'dlq', 'dlq.auto.flush': 'true', 'producer': producer})
    with pytest.raises(SerializationError):
        action.run(_make_ctx(original_value='v'), None, None)
    assert producer.flush_count == 1

    producer = RecordingProducer()
    action = DlqAction({'dlq.topic': 'dlq', 'producer': producer})
    with pytest.raises(SerializationError):
        action.run(_make_ctx(original_value='v'), None, None)
    assert producer.flush_count == 0


_REDACTION_SCHEMA = {
    'type': 'record',
    'name': 'test',
    'fields': [
        {'name': 'stringField', 'type': 'string', 'confluent:tags': ['PII']},
        {'name': 'bytesField', 'type': 'bytes', 'confluent:tags': ['PII']},
        {'name': 'lastName', 'type': 'string'},
        {'name': 'intField', 'type': 'int'},
    ],
}


def _redaction_ctx(rule, target=None):
    def field_transformer(rule_ctx, field_transform, message):
        return avro_transform(rule_ctx, _REDACTION_SCHEMA, message, field_transform)

    return _make_ctx(
        rule=rule,
        target=target if target is not None else Schema(json.dumps(_REDACTION_SCHEMA), 'AVRO'),
        inline_tags=get_inline_tags(_REDACTION_SCHEMA),
        field_transformer=field_transformer,
    )


def _redaction_message():
    return {'stringField': 'John', 'bytesField': b'secret', 'lastName': 'Smith', 'intField': 42}


def test_redact_fields_simple():
    action = DlqAction({'dlq.topic': 'dlq'})
    rule = _make_rule(rule_type='ENCRYPT', kind=RuleKind.TRANSFORM, tags=['PII'])
    result = action._redact_fields(_redaction_ctx(rule), _redaction_message())
    assert result['stringField'] == FieldRedactionExecutor.REDACTED_STRING
    assert result['bytesField'] == FieldRedactionExecutor.REDACTED_BYTES
    assert result['lastName'] == 'Smith'
    assert result['intField'] == 42


def test_redact_fields_wildcard_metadata_tags():
    action = DlqAction({'dlq.topic': 'dlq'})
    rule = _make_rule(rule_type='ENCRYPT', kind=RuleKind.TRANSFORM, tags=['PII2'])
    target = Schema(
        json.dumps(_REDACTION_SCHEMA), 'AVRO', [], Metadata(MetadataTags({'**.lastName': ['PII2']}), None, None), None
    )
    result = action._redact_fields(_redaction_ctx(rule, target=target), _redaction_message())
    assert result['lastName'] == FieldRedactionExecutor.REDACTED_STRING
    # tags of the rule (PII2) are disjoint from the inline tags (PII)
    assert result['stringField'] == 'John'


def test_redact_fields_no_matching_rule_type():
    action = DlqAction({'dlq.topic': 'dlq'})
    rule = _make_rule(rule_type='CEL', kind=RuleKind.CONDITION)
    message = _redaction_message()
    result = action._redact_fields(_redaction_ctx(rule), message)
    assert result is message
    assert result['stringField'] == 'John'
    assert result['bytesField'] == b'secret'


def test_redact_fields_fail_open():
    # redaction errors are fail-open: the unredacted message is sent
    producer = RecordingProducer()
    action = DlqAction({'dlq.topic': 'dlq', 'producer': producer})
    rule = _make_rule(rule_type='ENCRYPT', kind=RuleKind.TRANSFORM, tags=['PII'])
    # no field transformer is available, so redaction blows up
    ctx = _make_ctx(rule=rule, original_value={'stringField': 'hi'})
    with pytest.raises(SerializationError):
        action.run(ctx, None, None)
    assert len(producer.records) == 1
    assert b'"hi"' in producer.records[0][2]


def test_is_dlq_replay():
    is_dlq_replay = BaseSerde._is_dlq_replay
    rule = _make_rule(name='my-rule')

    def ctx_with(headers):
        return _make_ctx(rule=rule, ser_ctx=SerializationContext(_TOPIC, MessageField.VALUE, headers))

    assert is_dlq_replay(None, ctx_with(None), rule) is False
    assert is_dlq_replay(None, ctx_with([]), rule) is False
    assert is_dlq_replay(None, ctx_with([('__rule.name', b'my-rule')]), rule) is True
    assert is_dlq_replay(None, ctx_with([('__rule.name', 'my-rule')]), rule) is True
    assert is_dlq_replay(None, ctx_with([('__rule.name', b'other')]), rule) is False
    assert is_dlq_replay(None, ctx_with([('other', b'my-rule')]), rule) is False
    # last occurrence wins when a header key repeats
    assert is_dlq_replay(None, ctx_with([('__rule.name', b'other'), ('__rule.name', b'my-rule')]), rule) is True
    assert is_dlq_replay(None, ctx_with([('__rule.name', b'my-rule'), ('__rule.name', b'other')]), rule) is False
    assert is_dlq_replay(None, ctx_with({'__rule.name': b'my-rule'}), rule) is True
    assert is_dlq_replay(None, ctx_with({'__rule.name': None}), rule) is False
    # undecodable header values do not match
    assert is_dlq_replay(None, ctx_with([('__rule.name', b'\xff\xfe')]), rule) is False

    nameless = _make_rule(name=None)
    assert is_dlq_replay(None, ctx_with([('__rule.name', b'my-rule')]), nameless) is False
