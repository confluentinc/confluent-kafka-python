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
import base64
import json
import os
import sys
import time

import pytest

from confluent_kafka.schema_registry import Schema, SchemaRegistryClient
from confluent_kafka.schema_registry._sync.json_schema import JSONSerializer
from confluent_kafka.schema_registry._sync.protobuf import ProtobufSerializer
from confluent_kafka.schema_registry.avro import AvroDeserializer, AvroSerializer
from confluent_kafka.schema_registry.common.serde import clear_original_key, get_original_key
from confluent_kafka.schema_registry.protobuf import _schema_to_str
from confluent_kafka.schema_registry.rule_registry import RuleOverride, RuleRegistry
from confluent_kafka.schema_registry.rules.cel.cel_executor import CelExecutor
from confluent_kafka.schema_registry.rules.dlq.dlq_action import DlqAction, FieldRedactionExecutor
from confluent_kafka.schema_registry.rules.encryption.encrypt_executor import (
    Clock,
    EncryptionExecutor,
    FieldEncryptionExecutor,
)
from confluent_kafka.schema_registry.rules.encryption.localkms.local_driver import LocalKmsDriver
from confluent_kafka.schema_registry.schema_registry_client import Rule, RuleKind, RuleMode, RuleParams, RuleSet
from confluent_kafka.serialization import MessageField, SerializationContext, SerializationError

# Add proto directory to sys.path to resolve protobuf import dependencies
proto_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data', 'proto')
if proto_path not in sys.path:
    sys.path.insert(0, proto_path)

from tests.schema_registry.data.proto import example_pb2  # noqa: E402


class FakeClock(Clock):

    def __init__(self):
        self.fixed_now = int(round(time.time() * 1000))

    def now(self) -> int:
        return self.fixed_now


class RecordingProducer:
    def __init__(self):
        self.records = []
        self.flush_count = 0

    def produce(self, topic, value=None, key=None, headers=None, on_delivery=None, **kwargs):
        self.records.append((topic, key, value, headers))

    def poll(self, timeout=0):
        return 0

    def flush(self, timeout=None):
        self.flush_count += 1
        return 0


_BASE_URL = "mock://"
_TOPIC = "topic1"
_SUBJECT = _TOPIC + "-value"
_DLQ_TOPIC = "dlq-topic"

_AVRO_SCHEMA = {
    'type': 'record',
    'name': 'test',
    'fields': [
        {'name': 'intField', 'type': 'int'},
        {'name': 'doubleField', 'type': 'double'},
        {'name': 'stringField', 'type': 'string', 'confluent:tags': ['PII']},
        {'name': 'booleanField', 'type': 'boolean'},
        {'name': 'bytesField', 'type': 'bytes', 'confluent:tags': ['PII']},
    ],
}


def _avro_obj():
    return {
        'intField': 123,
        'doubleField': 45.67,
        'stringField': 'hi',
        'booleanField': True,
        'bytesField': b'foobar',
    }


def _encrypt_rule(kms_type='local-kms', rule_type='ENCRYPT', tags=None, on_failure="ERROR,NONE", extra_params=None):
    params = {"encrypt.kek.name": "kek1", "encrypt.kms.type": kms_type, "encrypt.kms.key.id": "mykey"}
    if extra_params:
        params.update(extra_params)
    if tags is None and rule_type == 'ENCRYPT':
        tags = ["PII"]
    return Rule(
        "test-encrypt",
        "",
        RuleKind.TRANSFORM,
        RuleMode.WRITEREAD,
        rule_type,
        tags,
        RuleParams(params),
        None,
        None,
        on_failure,
        False,
    )


def _cel_fail_rule(mode, on_failure, params=None):
    return Rule(
        "test-cel",
        "",
        RuleKind.CONDITION,
        mode,
        "CEL",
        None,
        RuleParams(params) if params is not None else None,
        "message.stringField != 'hi'",
        None,
        on_failure,
        False,
    )


def _register_dlq_action(topic=_DLQ_TOPIC):
    producer = RecordingProducer()
    conf = {'producer': producer}
    if topic is not None:
        conf['dlq.topic'] = topic
    DlqAction.register(conf)
    return producer


@pytest.fixture(autouse=True)
def run_before_and_after_tests(tmpdir):
    """Fixture to execute asserts before and after a test is run"""
    CelExecutor.register()
    LocalKmsDriver.register()

    yield  # this is where the testing happens

    conf = {'url': _BASE_URL}
    client = SchemaRegistryClient.new_client(conf)
    subjects = client.get_subjects()
    for subject in subjects:
        try:
            client.delete_subject(subject, True)
        except Exception:
            pass


def test_avro_dlq_encryption_write_redaction():
    FieldEncryptionExecutor.register_with_clock(FakeClock())
    producer = _register_dlq_action()

    conf = {'url': _BASE_URL}
    client = SchemaRegistryClient.new_client(conf)
    ser_conf = {'auto.register.schemas': False, 'use.latest.version': True}
    rule_conf = {'secret': 'mysecret'}
    rule = _encrypt_rule(kms_type='bad-kms', on_failure="DLQ,NONE")
    client.register_schema(_SUBJECT, Schema(json.dumps(_AVRO_SCHEMA), "AVRO", [], None, RuleSet(None, [rule])))

    obj = _avro_obj()
    ser = AvroSerializer(client, schema_str=None, conf=ser_conf, rule_conf=rule_conf)
    ser_ctx = SerializationContext(_TOPIC, MessageField.VALUE)
    with pytest.raises(SerializationError, match="Rule failed: test-encrypt"):
        ser(obj, ser_ctx)

    assert len(producer.records) == 1
    topic, key, value, headers = producer.records[0]
    assert topic == _DLQ_TOPIC
    assert key is None
    # tagged fields are redacted, so no plaintext leaks to the DLQ
    dlq_value = json.loads(value)
    assert dlq_value['stringField'] == FieldRedactionExecutor.REDACTED_STRING
    assert dlq_value['bytesField'] == FieldRedactionExecutor.REDACTED_STRING
    assert b'"hi"' not in value
    assert b'foobar' not in value
    # untagged fields are passed through
    assert dlq_value['intField'] == 123
    assert dlq_value['booleanField'] is True
    headers_dict = dict(headers)
    assert headers_dict[DlqAction.RULE_NAME] == b'test-encrypt'
    assert headers_dict[DlqAction.RULE_MODE] == b'WRITE'
    assert headers_dict[DlqAction.RULE_SUBJECT] == _SUBJECT.encode('utf-8')
    assert headers_dict[DlqAction.RULE_TOPIC] == _TOPIC.encode('utf-8')
    assert DlqAction.RULE_EXCEPTION in headers_dict
    # redaction mutates the failed message in place, as in Java
    assert obj['stringField'] == FieldRedactionExecutor.REDACTED_STRING


def test_avro_dlq_encryption_read_ciphertext_and_replay_skip():
    FieldEncryptionExecutor.register_with_clock(FakeClock())
    producer = _register_dlq_action()

    conf = {'url': _BASE_URL}
    client = SchemaRegistryClient.new_client(conf)
    ser_conf = {'auto.register.schemas': False, 'use.latest.version': True}
    rule_conf = {'secret': 'mysecret'}
    rule = _encrypt_rule(on_failure="ERROR,DLQ")
    client.register_schema(_SUBJECT, Schema(json.dumps(_AVRO_SCHEMA), "AVRO", [], None, RuleSet(None, [rule])))

    obj = _avro_obj()
    ser = AvroSerializer(client, schema_str=None, conf=ser_conf, rule_conf=rule_conf)
    ser_ctx = SerializationContext(_TOPIC, MessageField.VALUE)
    obj_bytes = ser(obj, ser_ctx)
    # encryption succeeded, so nothing went to the DLQ
    assert producer.records == []

    # a fresh executor (as in a separate consumer process) has an empty DEK
    # registry, so decryption fails
    FieldEncryptionExecutor.register_with_clock(FakeClock())
    deser = AvroDeserializer(client, rule_conf=rule_conf)
    with pytest.raises(SerializationError, match="Rule failed: test-encrypt"):
        deser(obj_bytes, ser_ctx)

    assert len(producer.records) == 1
    topic, key, value, headers = producer.records[0]
    assert key is None
    # the original wire bytes (ciphertext) are sent verbatim, so the DLQ record is replayable
    assert value == obj_bytes
    assert dict(headers)[DlqAction.RULE_MODE] == b'READ'

    # a record consumed from the DLQ carries a __rule.name header, which skips
    # the previously failed rule so the replay does not fail again
    replay_ctx = SerializationContext(_TOPIC, MessageField.VALUE, [(DlqAction.RULE_NAME, b'test-encrypt')])
    obj2 = deser(obj_bytes, replay_ctx)
    assert obj2['intField'] == 123
    # the rule was skipped, so the tagged fields still hold ciphertext
    assert obj2['stringField'] != 'hi'

    # dict-shaped headers are also supported
    replay_ctx = SerializationContext(_TOPIC, MessageField.VALUE, {DlqAction.RULE_NAME: b'test-encrypt'})
    obj3 = deser(obj_bytes, replay_ctx)
    assert obj3['intField'] == 123


def test_avro_dlq_with_key():
    FieldEncryptionExecutor.register_with_clock(FakeClock())
    producer = _register_dlq_action()

    conf = {'url': _BASE_URL}
    client = SchemaRegistryClient.new_client(conf)
    ser_conf = {'auto.register.schemas': False, 'use.latest.version': True}
    rule_conf = {'secret': 'mysecret'}
    rule = _encrypt_rule(kms_type='bad-kms', on_failure="DLQ,NONE")
    client.register_schema(_SUBJECT, Schema(json.dumps(_AVRO_SCHEMA), "AVRO", [], None, RuleSet(None, [rule])))

    key_ser = AvroSerializer(client, schema_str='"string"', conf={'auto.register.schemas': True})
    key_bytes = key_ser('mykey', SerializationContext(_TOPIC, MessageField.KEY))
    assert key_bytes is not None

    obj = _avro_obj()
    ser = AvroSerializer(client, schema_str=None, conf=ser_conf, rule_conf=rule_conf)
    with pytest.raises(SerializationError, match="Rule failed: test-encrypt"):
        ser(obj, SerializationContext(_TOPIC, MessageField.VALUE))

    assert len(producer.records) == 1
    topic, key, value, headers = producer.records[0]
    # the key serde stashed the original key for the value-side DLQ record
    assert key == b'mykey'
    assert json.loads(value)['stringField'] == FieldRedactionExecutor.REDACTED_STRING


def test_avro_dlq_tombstone_clears_original_key():
    FieldEncryptionExecutor.register_with_clock(FakeClock())
    producer = _register_dlq_action()

    conf = {'url': _BASE_URL}
    client = SchemaRegistryClient.new_client(conf)
    ser_conf = {'auto.register.schemas': False, 'use.latest.version': True}
    rule_conf = {'secret': 'mysecret'}
    rule = _encrypt_rule(kms_type='bad-kms', on_failure="DLQ,NONE")
    client.register_schema(_SUBJECT, Schema(json.dumps(_AVRO_SCHEMA), "AVRO", [], None, RuleSet(None, [rule])))

    clear_original_key()
    key_ser = AvroSerializer(client, schema_str='"string"', conf={'auto.register.schemas': True})
    key_ser('keyA', SerializationContext(_TOPIC, MessageField.KEY))
    # the key serde stashed the original key for a subsequent value-side DLQ record
    assert get_original_key() == 'keyA'

    value_ser = AvroSerializer(client, schema_str=None, conf=ser_conf, rule_conf=rule_conf)
    # a tombstone (value=None) must still clear the stashed key rather than leak it
    assert value_ser(None, SerializationContext(_TOPIC, MessageField.VALUE)) is None
    assert get_original_key() is None

    # a subsequent keyless failing value must produce a DLQ record with key=None,
    # not the stale key from the earlier key serialization
    with pytest.raises(SerializationError, match="Rule failed: test-encrypt"):
        value_ser(_avro_obj(), SerializationContext(_TOPIC, MessageField.VALUE))
    assert len(producer.records) == 1
    topic, key, value, headers = producer.records[0]
    assert key is None


def test_serializer_close_flushes_dlq_and_closes_executors():
    conf = {'url': _BASE_URL}
    client = SchemaRegistryClient.new_client(conf)

    producer = RecordingProducer()
    registry = RuleRegistry()

    class _SpyExecutor(FieldRedactionExecutor):
        def __init__(self):
            self.closed = False

        def close(self):
            self.closed = True

    spy = _SpyExecutor()
    registry.register_executor(spy)
    registry.register_action(DlqAction({'dlq.topic': _DLQ_TOPIC, 'producer': producer}))

    ser = AvroSerializer(client, schema_str='"string"', conf={'auto.register.schemas': True}, rule_registry=registry)
    # closing the serde flushes the DLQ producer and closes each registered executor
    ser.close()
    assert producer.flush_count == 1
    assert spy.closed is True


def test_serializer_close_does_not_close_shared_global_registry():
    # Closing a serde bound to the shared global RuleRegistry must NOT tear down
    # executors/actions still in use by other default-configured serdes.
    conf = {'url': _BASE_URL}
    client = SchemaRegistryClient.new_client(conf)

    registry = RuleRegistry.get_global_instance()
    registry.clear()

    producer = RecordingProducer()

    class _SpyExecutor(FieldRedactionExecutor):
        def __init__(self):
            self.closed = False

        def close(self):
            self.closed = True

    spy = _SpyExecutor()
    registry.register_executor(spy)
    action = DlqAction({'dlq.topic': _DLQ_TOPIC, 'producer': producer})
    registry.register_action(action)

    # a default serde (no rule_registry) shares the global instance
    ser = AvroSerializer(client, schema_str='"string"', conf={'auto.register.schemas': True})
    assert ser._rule_registry is registry
    ser.close()

    # the shared action's producer is untouched and the shared executor is not closed,
    # so a still-live sibling serde keeps working
    assert producer.flush_count == 0
    assert action._producer is producer
    assert spy.closed is False

    registry.clear()


def test_avro_dlq_cel_condition_read():
    producer = _register_dlq_action()

    conf = {'url': _BASE_URL}
    client = SchemaRegistryClient.new_client(conf)
    ser_conf = {'auto.register.schemas': False, 'use.latest.version': True}
    rule = _cel_fail_rule(RuleMode.READ, "DLQ")
    schema = dict(_AVRO_SCHEMA)
    client.register_schema(_SUBJECT, Schema(json.dumps(schema), "AVRO", [], None, RuleSet(None, [rule])))

    obj = _avro_obj()
    ser = AvroSerializer(client, schema_str=None, conf=ser_conf)
    ser_ctx = SerializationContext(_TOPIC, MessageField.VALUE)
    obj_bytes = ser(obj, ser_ctx)
    assert producer.records == []

    deser = AvroDeserializer(client)
    with pytest.raises(SerializationError, match="Rule failed: test-cel"):
        deser(obj_bytes, ser_ctx)

    assert len(producer.records) == 1
    topic, key, value, headers = producer.records[0]
    # on the read path the original wire bytes are sent verbatim
    assert value == obj_bytes
    headers_dict = dict(headers)
    assert headers_dict[DlqAction.RULE_MODE] == b'READ'
    assert headers_dict[DlqAction.RULE_EXCEPTION] == b"Rule expr failed: message.stringField != 'hi'"


def test_avro_dlq_on_failure_from_override():
    conf = {'url': _BASE_URL}
    client = SchemaRegistryClient.new_client(conf)
    ser_conf = {'auto.register.schemas': False, 'use.latest.version': True}
    rule = _cel_fail_rule(RuleMode.WRITE, None)
    client.register_schema(_SUBJECT, Schema(json.dumps(_AVRO_SCHEMA), "AVRO", [], None, RuleSet(None, [rule])))

    producer = RecordingProducer()
    registry = RuleRegistry()
    registry.register_rule_executor(CelExecutor())
    registry.register_action(DlqAction({'dlq.topic': _DLQ_TOPIC, 'producer': producer}))
    registry.register_override(RuleOverride("CEL", None, "DLQ", None))

    ser = AvroSerializer(client, schema_str=None, conf=ser_conf, rule_registry=registry)
    with pytest.raises(SerializationError, match="Rule failed: test-cel"):
        ser(_avro_obj(), SerializationContext(_TOPIC, MessageField.VALUE))
    assert len(producer.records) == 1
    assert producer.records[0][0] == _DLQ_TOPIC


def test_avro_dlq_disabled_from_override():
    conf = {'url': _BASE_URL}
    client = SchemaRegistryClient.new_client(conf)
    ser_conf = {'auto.register.schemas': False, 'use.latest.version': True}
    rule = _cel_fail_rule(RuleMode.WRITE, "DLQ")
    client.register_schema(_SUBJECT, Schema(json.dumps(_AVRO_SCHEMA), "AVRO", [], None, RuleSet(None, [rule])))

    producer = RecordingProducer()
    registry = RuleRegistry()
    registry.register_rule_executor(CelExecutor())
    registry.register_action(DlqAction({'dlq.topic': _DLQ_TOPIC, 'producer': producer}))
    registry.register_override(RuleOverride("CEL", None, None, True))

    ser = AvroSerializer(client, schema_str=None, conf=ser_conf, rule_registry=registry)
    obj_bytes = ser(_avro_obj(), SerializationContext(_TOPIC, MessageField.VALUE))
    assert obj_bytes is not None
    assert producer.records == []


def test_avro_dlq_topic_from_rule_params():
    producer = _register_dlq_action(topic=None)

    conf = {'url': _BASE_URL}
    client = SchemaRegistryClient.new_client(conf)
    ser_conf = {'auto.register.schemas': False, 'use.latest.version': True}
    rule = _cel_fail_rule(RuleMode.WRITE, "DLQ", params={'dlq.topic': 'param-dlq'})
    client.register_schema(_SUBJECT, Schema(json.dumps(_AVRO_SCHEMA), "AVRO", [], None, RuleSet(None, [rule])))

    ser = AvroSerializer(client, schema_str=None, conf=ser_conf)
    with pytest.raises(SerializationError, match="Rule failed: test-cel"):
        ser(_avro_obj(), SerializationContext(_TOPIC, MessageField.VALUE))
    assert len(producer.records) == 1
    assert producer.records[0][0] == 'param-dlq'


def test_avro_dlq_no_topic_configured():
    producer = _register_dlq_action(topic=None)

    conf = {'url': _BASE_URL}
    client = SchemaRegistryClient.new_client(conf)
    ser_conf = {'auto.register.schemas': False, 'use.latest.version': True}
    rule = _cel_fail_rule(RuleMode.WRITE, "DLQ")
    client.register_schema(_SUBJECT, Schema(json.dumps(_AVRO_SCHEMA), "AVRO", [], None, RuleSet(None, [rule])))

    ser = AvroSerializer(client, schema_str=None, conf=ser_conf)
    with pytest.raises(SerializationError, match="Could not send to DLQ as no topic is configured"):
        ser(_avro_obj(), SerializationContext(_TOPIC, MessageField.VALUE))
    assert producer.records == []


def test_avro_dlq_payload_encryption_write():
    EncryptionExecutor.register_with_clock(FakeClock())
    producer = _register_dlq_action()

    conf = {'url': _BASE_URL}
    client = SchemaRegistryClient.new_client(conf)
    ser_conf = {'auto.register.schemas': False, 'use.latest.version': True}
    rule_conf = {'secret': 'mysecret'}
    rule = _encrypt_rule(kms_type='bad-kms', rule_type='ENCRYPT_PAYLOAD', on_failure="DLQ,NONE")
    client.register_schema(_SUBJECT, Schema(json.dumps(_AVRO_SCHEMA), "AVRO", [], None, RuleSet(None, None, [rule])))

    obj = _avro_obj()
    ser = AvroSerializer(client, schema_str=None, conf=ser_conf, rule_conf=rule_conf)
    with pytest.raises(SerializationError, match="Rule failed: test-encrypt"):
        ser(obj, SerializationContext(_TOPIC, MessageField.VALUE))

    assert len(producer.records) == 1
    topic, key, value, headers = producer.records[0]
    # payload-level rules carry no field tags, so the plaintext serialized bytes
    # are sent verbatim, matching Java
    assert b'hi' in value
    assert b'foobar' in value
    assert dict(headers)[DlqAction.RULE_MODE] == b'WRITE'


def test_avro_dlq_payload_encryption_read_ciphertext():
    EncryptionExecutor.register_with_clock(FakeClock())
    producer = _register_dlq_action()

    conf = {'url': _BASE_URL}
    client = SchemaRegistryClient.new_client(conf)
    ser_conf = {'auto.register.schemas': False, 'use.latest.version': True}
    rule_conf = {'secret': 'mysecret'}
    rule = _encrypt_rule(rule_type='ENCRYPT_PAYLOAD', on_failure="ERROR,DLQ")
    client.register_schema(_SUBJECT, Schema(json.dumps(_AVRO_SCHEMA), "AVRO", [], None, RuleSet(None, None, [rule])))

    obj = _avro_obj()
    ser = AvroSerializer(client, schema_str=None, conf=ser_conf, rule_conf=rule_conf)
    ser_ctx = SerializationContext(_TOPIC, MessageField.VALUE)
    obj_bytes = ser(obj, ser_ctx)
    # payload encryption succeeded on write, so nothing went to the DLQ
    assert producer.records == []

    # a fresh executor (as in a separate consumer process) cannot decrypt
    EncryptionExecutor.register_with_clock(FakeClock())
    deser = AvroDeserializer(client, rule_conf=rule_conf)
    with pytest.raises(SerializationError, match="Rule failed: test-encrypt"):
        deser(obj_bytes, ser_ctx)

    assert len(producer.records) == 1
    topic, key, value, headers = producer.records[0]
    # the encoding-phase read DLQ record carries the original wire bytes (framing
    # included), so it is replayable -- not the frame-stripped payload
    assert value == obj_bytes
    assert dict(headers)[DlqAction.RULE_MODE] == b'READ'


def test_json_dlq_encryption_write_redaction():
    FieldEncryptionExecutor.register_with_clock(FakeClock())
    producer = _register_dlq_action()

    conf = {'url': _BASE_URL}
    client = SchemaRegistryClient.new_client(conf)
    ser_conf = {'auto.register.schemas': False, 'use.latest.version': True}
    rule_conf = {'secret': 'mysecret'}
    schema = {
        "type": "object",
        "properties": {
            "intField": {"type": "integer"},
            "doubleField": {"type": "number"},
            "stringField": {"type": "string", "confluent:tags": ["PII"]},
            "booleanField": {"type": "boolean"},
            "bytesField": {"type": "string", "contentEncoding": "base64", "confluent:tags": ["PII"]},
        },
    }
    rule = _encrypt_rule(kms_type='bad-kms', on_failure="DLQ,NONE")
    client.register_schema(_SUBJECT, Schema(json.dumps(schema), "JSON", [], None, RuleSet(None, [rule])))

    obj = {
        'intField': 123,
        'doubleField': 45.67,
        'stringField': 'hi',
        'booleanField': True,
        'bytesField': base64.b64encode(b'foobar').decode('utf-8'),
    }
    ser = JSONSerializer(json.dumps(schema), client, conf=ser_conf, rule_conf=rule_conf)
    with pytest.raises(SerializationError, match="Rule failed: test-encrypt"):
        ser(obj, SerializationContext(_TOPIC, MessageField.VALUE))

    assert len(producer.records) == 1
    value = producer.records[0][2]
    dlq_value = json.loads(value)
    assert dlq_value['stringField'] == FieldRedactionExecutor.REDACTED_STRING
    assert dlq_value['bytesField'] == FieldRedactionExecutor.REDACTED_STRING
    assert b'"hi"' not in value
    assert base64.b64encode(b'foobar') not in value
    assert dlq_value['intField'] == 123


def test_proto_dlq_encryption_write_redaction():
    FieldEncryptionExecutor.register_with_clock(FakeClock())
    producer = _register_dlq_action()

    conf = {'url': _BASE_URL}
    client = SchemaRegistryClient.new_client(conf)
    ser_conf = {'auto.register.schemas': False, 'use.latest.version': True, 'use.deprecated.format': False}
    rule_conf = {'secret': 'mysecret'}
    rule = _encrypt_rule(kms_type='bad-kms', on_failure="DLQ,NONE")
    client.register_schema(
        _SUBJECT,
        Schema(_schema_to_str(example_pb2.Author.DESCRIPTOR.file), "PROTOBUF", [], None, RuleSet(None, [rule])),
    )

    obj = example_pb2.Author(
        name='Kafka', id=123, picture=b'foobar', works=['The Castle', 'The Trial'], oneof_string='oneof'
    )
    ser = ProtobufSerializer(example_pb2.Author, client, conf=ser_conf, rule_conf=rule_conf)
    with pytest.raises(SerializationError, match="Rule failed: test-encrypt"):
        ser(obj, SerializationContext(_TOPIC, MessageField.VALUE))

    assert len(producer.records) == 1
    value = producer.records[0][2]
    # the protobuf message is JSON-encoded with tagged fields redacted
    assert b'Kafka' not in value
    assert base64.b64encode(b'foobar') not in value
    assert b'<REDACTED>' in value
    assert b'The Castle' in value
    # redaction mutates the failed message in place, as in Java
    assert obj.name == FieldRedactionExecutor.REDACTED_STRING
