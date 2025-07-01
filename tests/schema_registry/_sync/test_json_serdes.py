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
import base64
import json

import pytest

from confluent_kafka.schema_registry import SchemaRegistryClient, \
    Schema, Metadata, MetadataProperties, header_schema_id_serializer
from confluent_kafka.schema_registry.json_schema import JSONSerializer, \
    JSONDeserializer
from confluent_kafka.schema_registry.rules.cel.cel_executor import CelExecutor
from confluent_kafka.schema_registry.rules.cel.cel_field_executor import \
    CelFieldExecutor
from confluent_kafka.schema_registry.rules.encryption.awskms.aws_driver import \
    AwsKmsDriver
from confluent_kafka.schema_registry.rules.encryption.azurekms.azure_driver import \
    AzureKmsDriver
from confluent_kafka.schema_registry.rules.encryption.encrypt_executor import \
    FieldEncryptionExecutor
from confluent_kafka.schema_registry.rules.encryption.gcpkms.gcp_driver import \
    GcpKmsDriver
from confluent_kafka.schema_registry.rules.encryption.hcvault.hcvault_driver import \
    HcVaultKmsDriver
from confluent_kafka.schema_registry.rules.encryption.localkms.local_driver import \
    LocalKmsDriver
from confluent_kafka.schema_registry.rules.jsonata.jsonata_executor import \
    JsonataExecutor
from confluent_kafka.schema_registry.schema_registry_client import RuleSet, \
    Rule, RuleKind, RuleMode, SchemaReference, RuleParams, ServerConfig
from confluent_kafka.schema_registry.serde import RuleConditionError
from confluent_kafka.serialization import SerializationContext, MessageField, SerializationError
from tests.schema_registry._sync.test_avro_serdes import FakeClock


_BASE_URL = "mock://"
# _BASE_URL = "http://localhost:8081"
_TOPIC = "topic1"
_SUBJECT = _TOPIC + "-value"


@pytest.fixture(autouse=True)
def run_before_and_after_tests(tmpdir):
    """Fixture to execute asserts before and after a test is run"""
    # Setup: fill with any logic you want
    CelExecutor.register()
    CelFieldExecutor.register()
    AwsKmsDriver.register()
    AzureKmsDriver.register()
    GcpKmsDriver.register()
    HcVaultKmsDriver.register()
    JsonataExecutor.register()
    LocalKmsDriver.register()

    yield  # this is where the testing happens

    # Teardown : fill with any logic you want
    conf = {'url': _BASE_URL}
    client = SchemaRegistryClient.new_client(conf)
    try:
        client.delete_subject(_SUBJECT, True)
    except Exception:
        pass
    subjects = client.get_subjects()
    for subject in subjects:
        try:
            client.delete_subject(subject, True)
        except Exception:
            pass


def test_json_basic_serialization():
    conf = {'url': _BASE_URL}
    client = SchemaRegistryClient.new_client(conf)
    ser_conf = {'auto.register.schemas': True, 'validate': True}
    obj = {
        'intField': 123,
        'doubleField': 45.67,
        'stringField': 'hi',
        'booleanField': True,
        'bytesField': base64.b64encode(b'foobar').decode('utf-8'),
    }
    schema = {
        "type": "object",
        "properties": {
            "intField": {"type": "integer"},
            "doubleField": {"type": "number"},
            "stringField": {
                "type": "string",
                "confluent:tags": ["PII"]
            },
            "booleanField": {"type": "boolean"},
            "bytesField": {
                "type": "string",
                "contentEncoding": "base64",
                "confluent:tags": ["PII"]
            }
        }
    }
    ser = JSONSerializer(json.dumps(schema), client, conf=ser_conf)
    ser_ctx = SerializationContext(_TOPIC, MessageField.VALUE)
    obj_bytes = ser(obj, ser_ctx)

    deser = JSONDeserializer(None, schema_registry_client=client)
    obj2 = deser(obj_bytes, ser_ctx)
    assert obj == obj2


def test_json_basic_failing_validation():
    conf = {'url': _BASE_URL}
    client = SchemaRegistryClient.new_client(conf)
    ser_conf = {'auto.register.schemas': True, 'validate': True}
    obj = {
        'intField': '123',
        'doubleField': 45.67,
        'stringField': 'hi',
        'booleanField': True,
        'bytesField': base64.b64encode(b'foobar').decode('utf-8'),
    }
    schema = {
        "type": "object",
        "properties": {
            "intField": {"type": "integer"},
            "doubleField": {"type": "number"},
            "stringField": {
                "type": "string",
                "confluent:tags": ["PII"]
            },
            "booleanField": {"type": "boolean"},
            "bytesField": {
                "type": "string",
                "contentEncoding": "base64",
                "confluent:tags": ["PII"]
            }
        }
    }
    ser = JSONSerializer(json.dumps(schema), client, conf=ser_conf)
    ser_ctx = SerializationContext(_TOPIC, MessageField.VALUE)
    with pytest.raises(SerializationError):
        ser(obj, ser_ctx)


def test_json_guid_in_header():
    conf = {'url': _BASE_URL}
    client = SchemaRegistryClient.new_client(conf)
    ser_conf = {
        'auto.register.schemas': True,
        'schema.id.serializer': header_schema_id_serializer
    }
    obj = {
        'intField': 123,
        'doubleField': 45.67,
        'stringField': 'hi',
        'booleanField': True,
        'bytesField': base64.b64encode(b'foobar').decode('utf-8'),
    }
    schema = {
        "type": "object",
        "properties": {
            "intField": {"type": "integer"},
            "doubleField": {"type": "number"},
            "stringField": {
                "type": "string",
                "confluent:tags": ["PII"]
            },
            "booleanField": {"type": "boolean"},
            "bytesField": {
                "type": "string",
                "contentEncoding": "base64",
                "confluent:tags": ["PII"]
            }
        }
    }
    ser = JSONSerializer(json.dumps(schema), client, conf=ser_conf)
    ser_ctx = SerializationContext(_TOPIC, MessageField.VALUE, {})
    obj_bytes = ser(obj, ser_ctx)

    deser = JSONDeserializer(None, schema_registry_client=client)
    obj2 = deser(obj_bytes, ser_ctx)
    assert obj == obj2


def test_json_basic_deserialization_no_client():
    conf = {'url': _BASE_URL}
    client = SchemaRegistryClient.new_client(conf)
    ser_conf = {'auto.register.schemas': True}
    obj = {
        'intField': 123,
        'doubleField': 45.67,
        'stringField': 'hi',
        'booleanField': True,
        'bytesField': base64.b64encode(b'foobar').decode('utf-8'),
    }
    schema = {
        "type": "object",
        "properties": {
            "intField": {"type": "integer"},
            "doubleField": {"type": "number"},
            "stringField": {
                "type": "string",
                "confluent:tags": ["PII"]
            },
            "booleanField": {"type": "boolean"},
            "bytesField": {
                "type": "string",
                "contentEncoding": "base64",
                "confluent:tags": ["PII"]
            }
        }
    }
    ser = JSONSerializer(json.dumps(schema), client, conf=ser_conf)
    ser_ctx = SerializationContext(_TOPIC, MessageField.VALUE)
    obj_bytes = ser(obj, ser_ctx)

    deser = JSONDeserializer(json.dumps(schema))
    obj2 = deser(obj_bytes, ser_ctx)
    assert obj == obj2


def test_json_serialize_nested():
    conf = {'url': _BASE_URL}
    client = SchemaRegistryClient.new_client(conf)
    ser_conf = {'auto.register.schemas': True}
    nested = {
        'intField': 123,
        'doubleField': 45.67,
        'stringField': 'hi',
        'booleanField': True,
        'bytesField': base64.b64encode(b'foobar').decode('utf-8'),
    }
    obj = {
        'nested': nested
    }
    schema = {
        "type": "object",
        "properties": {
            "otherField": {
                "type": "object",
                "properties": {
                    "intField": {
                        "type": "integer"
                    },
                    "doubleField": {
                        "type": "number"
                    },
                    "stringField": {
                        "type": "string"
                    },
                    "booleanField": {
                        "type": "boolean"
                    },
                    "bytesField": {
                        "type": "string"
                    }
                }
            }
        }
    }
    ser = JSONSerializer(json.dumps(schema), client, conf=ser_conf)
    ser_ctx = SerializationContext(_TOPIC, MessageField.VALUE)
    obj_bytes = ser(obj, ser_ctx)

    deser = JSONDeserializer(None, schema_registry_client=client)
    obj2 = deser(obj_bytes, ser_ctx)
    assert obj == obj2


def test_json_serialize_references():
    conf = {'url': _BASE_URL}
    client = SchemaRegistryClient.new_client(conf)
    ser_conf = {'auto.register.schemas': False, 'use.latest.version': True}

    referenced = {
        'intField': 123,
        'doubleField': 45.67,
        'stringField': 'hi',
        'booleanField': True,
        'bytesField': base64.b64encode(b'foobar').decode('utf-8'),
    }
    obj = {
        'otherField': referenced
    }
    ref_schema = {
        "type": "object",
        "properties": {
            "intField": {"type": "integer"},
            "doubleField": {"type": "number"},
            "stringField": {
                "type": "string",
                "confluent:tags": ["PII"]
            },
            "booleanField": {"type": "boolean"},
            "bytesField": {
                "type": "string",
                "contentEncoding": "base64",
                "confluent:tags": ["PII"]
            }
        }
    }
    client.register_schema('ref', Schema(json.dumps(ref_schema), "JSON"))
    schema = {
        "type": "object",
        "properties": {
            "otherField": {"$ref": "ref"}
        }
    }
    refs = [SchemaReference('ref', 'ref', 1)]
    client.register_schema(_SUBJECT, Schema(json.dumps(schema), 'JSON', refs))

    ser = JSONSerializer(None, client, conf=ser_conf)
    ser_ctx = SerializationContext(_TOPIC, MessageField.VALUE)
    obj_bytes = ser(obj, ser_ctx)

    deser = JSONDeserializer(None, schema_registry_client=client)
    obj2 = deser(obj_bytes, ser_ctx)
    assert obj == obj2


def test_json_cel_condition():
    conf = {'url': _BASE_URL}
    client = SchemaRegistryClient.new_client(conf)
    ser_conf = {'auto.register.schemas': False, 'use.latest.version': True}
    schema = {
        "type": "object",
        "properties": {
            "intField": {"type": "integer"},
            "doubleField": {"type": "number"},
            "stringField": {
                "type": "string",
                "confluent:tags": ["PII"]
            },
            "booleanField": {"type": "boolean"},
            "bytesField": {
                "type": "string",
                "contentEncoding": "base64",
                "confluent:tags": ["PII"]
            }
        }
    }

    rule = Rule(
        "test-cel",
        "",
        RuleKind.CONDITION,
        RuleMode.WRITE,
        "CEL",
        None,
        None,
        "message.stringField == 'hi'",
        None,
        None,
        False
    )
    client.register_schema(_SUBJECT, Schema(
        json.dumps(schema),
        "JSON",
        [],
        None,
        RuleSet(None, [rule])
    ))

    obj = {
        'intField': 123,
        'doubleField': 45.67,
        'stringField': 'hi',
        'booleanField': True,
        'bytesField': base64.b64encode(b'foobar').decode('utf-8'),
    }
    ser = JSONSerializer(json.dumps(schema), client, conf=ser_conf)
    ser_ctx = SerializationContext(_TOPIC, MessageField.VALUE)
    obj_bytes = ser(obj, ser_ctx)

    deser = JSONDeserializer(None, schema_registry_client=client)
    obj2 = deser(obj_bytes, ser_ctx)
    assert obj == obj2


def test_json_cel_condition_fail():
    conf = {'url': _BASE_URL}
    client = SchemaRegistryClient.new_client(conf)
    ser_conf = {'auto.register.schemas': False, 'use.latest.version': True}
    schema = {
        "type": "object",
        "properties": {
            "intField": {"type": "integer"},
            "doubleField": {"type": "number"},
            "stringField": {
                "type": "string",
                "confluent:tags": ["PII"]
            },
            "booleanField": {"type": "boolean"},
            "bytesField": {
                "type": "string",
                "contentEncoding": "base64",
                "confluent:tags": ["PII"]
            }
        }
    }

    rule = Rule(
        "test-cel",
        "",
        RuleKind.CONDITION,
        RuleMode.WRITE,
        "CEL",
        None,
        None,
        "message.stringField != 'hi'",
        None,
        None,
        False
    )
    client.register_schema(_SUBJECT, Schema(
        json.dumps(schema),
        "JSON",
        [],
        None,
        RuleSet(None, [rule])
    ))

    obj = {
        'intField': 123,
        'doubleField': 45.67,
        'stringField': 'hi',
        'booleanField': True,
        'bytesField': base64.b64encode(b'foobar').decode('utf-8'),
    }
    ser = JSONSerializer(json.dumps(schema), client, conf=ser_conf)
    ser_ctx = SerializationContext(_TOPIC, MessageField.VALUE)
    with pytest.raises(SerializationError) as e:
        ser(obj, ser_ctx)

    assert isinstance(e.value.__cause__, RuleConditionError)


def test_json_cel_condition_ignore_fail():
    conf = {'url': _BASE_URL}
    client = SchemaRegistryClient.new_client(conf)
    ser_conf = {'auto.register.schemas': False, 'use.latest.version': True}
    schema = {
        "type": "object",
        "properties": {
            "intField": {"type": "integer"},
            "doubleField": {"type": "number"},
            "stringField": {
                "type": "string",
                "confluent:tags": ["PII"]
            },
            "booleanField": {"type": "boolean"},
            "bytesField": {
                "type": "string",
                "contentEncoding": "base64",
                "confluent:tags": ["PII"]
            }
        }
    }

    rule = Rule(
        "test-cel",
        "",
        RuleKind.CONDITION,
        RuleMode.WRITE,
        "CEL",
        None,
        None,
        "message.stringField != 'hi'",
        None,
        "NONE",
        False
    )
    client.register_schema(_SUBJECT, Schema(
        json.dumps(schema),
        "JSON",
        [],
        None,
        RuleSet(None, [rule])
    ))

    obj = {
        'intField': 123,
        'doubleField': 45.67,
        'stringField': 'hi',
        'booleanField': True,
        'bytesField': base64.b64encode(b'foobar').decode('utf-8'),
    }
    ser = JSONSerializer(json.dumps(schema), client, conf=ser_conf)
    ser_ctx = SerializationContext(_TOPIC, MessageField.VALUE)
    obj_bytes = ser(obj, ser_ctx)

    deser = JSONDeserializer(None, schema_registry_client=client)
    obj2 = deser(obj_bytes, ser_ctx)
    assert obj == obj2


def test_json_cel_field_transform():
    conf = {'url': _BASE_URL}
    client = SchemaRegistryClient.new_client(conf)
    ser_conf = {'auto.register.schemas': False, 'use.latest.version': True}
    schema = {
        "type": "object",
        "properties": {
            "intField": {"type": "integer"},
            "doubleField": {"type": "number"},
            "stringField": {
                "type": "string",
                "confluent:tags": ["PII"]
            },
            "booleanField": {"type": "boolean"},
            "bytesField": {
                "type": "string",
                "contentEncoding": "base64",
                "confluent:tags": ["PII"]
            }
        }
    }

    rule = Rule(
        "test-cel",
        "",
        RuleKind.TRANSFORM,
        RuleMode.WRITE,
        "CEL_FIELD",
        None,
        None,
        "name == 'stringField' ; value + '-suffix'",
        None,
        None,
        False
    )
    client.register_schema(_SUBJECT, Schema(
        json.dumps(schema),
        "JSON",
        [],
        None,
        RuleSet(None, [rule])
    ))

    obj = {
        'intField': 123,
        'doubleField': 45.67,
        'stringField': 'hi',
        'booleanField': True,
        'bytesField': base64.b64encode(b'foobar').decode('utf-8'),
    }
    ser = JSONSerializer(json.dumps(schema), client, conf=ser_conf)
    ser_ctx = SerializationContext(_TOPIC, MessageField.VALUE)
    obj_bytes = ser(obj, ser_ctx)

    obj2 = {
        'intField': 123,
        'doubleField': 45.67,
        'stringField': 'hi-suffix',
        'booleanField': True,
        'bytesField': base64.b64encode(b'foobar').decode('utf-8'),
    }
    deser = JSONDeserializer(None, schema_registry_client=client)
    newobj = deser(obj_bytes, ser_ctx)
    assert obj2 == newobj


def test_json_cel_field_transform_with_def():
    conf = {'url': _BASE_URL}
    client = SchemaRegistryClient.new_client(conf)
    ser_conf = {'auto.register.schemas': False, 'use.latest.version': True}
    schema = {
        "$schema": "http://json-schema.org/draft-07/schema#",
        "additionalProperties": False,
        "definitions": {
            "Address": {
                "additionalProperties": False,
                "properties": {
                    "doornumber": {
                        "type": "integer"
                    },
                    "doorpin": {
                        "confluent:tags": ["PII"],
                        "type": "string"
                    }
                },
                "type": "object"
            }
        },
        "properties": {
            "address": {
                "$ref": "#/definitions/Address"
            },
            "name": {
                "confluent:tags": ["PII"],
                "type": "string"
            }
        },
        "title": "Sample Event",
        "type": "object"
    }

    rule = Rule(
        "test-cel",
        "",
        RuleKind.TRANSFORM,
        RuleMode.WRITE,
        "CEL_FIELD",
        ["PII"],
        None,
        "value + '-suffix'",
        None,
        None,
        False
    )
    client.register_schema(_SUBJECT, Schema(
        json.dumps(schema),
        "JSON",
        [],
        None,
        RuleSet(None, [rule])
    ))

    obj = {
        'name': 'bob',
        'address': {
            'doornumber': 123,
            'doorpin': '1234'
        }
    }
    ser = JSONSerializer(json.dumps(schema), client, conf=ser_conf)
    ser_ctx = SerializationContext(_TOPIC, MessageField.VALUE)
    obj_bytes = ser(obj, ser_ctx)

    obj2 = {
        'name': 'bob-suffix',
        'address': {
            'doornumber': 123,
            'doorpin': '1234-suffix'
        }
    }
    deser = JSONDeserializer(None, schema_registry_client=client)
    newobj = deser(obj_bytes, ser_ctx)
    assert obj2 == newobj


def test_json_cel_field_transform_complex():
    conf = {'url': _BASE_URL}
    client = SchemaRegistryClient.new_client(conf)
    ser_conf = {'auto.register.schemas': False, 'use.latest.version': True}
    schema = {
        "type": "object",
        "properties": {
            "arrayField": {
                "type": "array",
                "items": {
                    "type": "string"
                }
            },
            "objectField": {
                "type": "object",
                "properties": {
                    "stringField": {"type": "string"}
                }
            },
            "unionField": {
                "oneOf": [
                    {
                        "type": "null"
                    },
                    {
                        "type": "string"
                    }
                ],
                "confluent:tags": ["PII"]
            }
        }
    }

    rule = Rule(
        "test-cel",
        "",
        RuleKind.TRANSFORM,
        RuleMode.WRITE,
        "CEL_FIELD",
        None,
        None,
        "typeName == 'STRING' ; value + '-suffix'",
        None,
        None,
        False
    )
    client.register_schema(_SUBJECT, Schema(
        json.dumps(schema),
        "JSON",
        [],
        None,
        RuleSet(None, [rule])
    ))

    obj = {
        'arrayField': ['a', 'b', 'c'],
        'objectField': {
            'stringField': 'hello'
        },
        'unionField': 'world'
    }
    ser = JSONSerializer(json.dumps(schema), client, conf=ser_conf)
    ser_ctx = SerializationContext(_TOPIC, MessageField.VALUE)
    obj_bytes = ser(obj, ser_ctx)

    obj2 = {
        'arrayField': ['a-suffix', 'b-suffix', 'c-suffix'],
        'objectField': {
            'stringField': 'hello-suffix'
        },
        'unionField': 'world-suffix'
    }
    deser = JSONDeserializer(None, schema_registry_client=client)
    newobj = deser(obj_bytes, ser_ctx)
    assert obj2 == newobj


def test_json_cel_field_transform_complex_with_none():
    conf = {'url': _BASE_URL}
    client = SchemaRegistryClient.new_client(conf)
    ser_conf = {'auto.register.schemas': False, 'use.latest.version': True}
    schema = {
        "type": "object",
        "properties": {
            "arrayField": {
                "type": "array",
                "items": {
                    "type": "string"
                }
            },
            "objectField": {
                "type": "object",
                "properties": {
                    "stringField": {"type": "string"}
                }
            },
            "unionField": {
                "oneOf": [
                    {
                        "type": "null"
                    },
                    {
                        "type": "string"
                    }
                ],
                "confluent:tags": ["PII"]
            }
        }
    }

    rule = Rule(
        "test-cel",
        "",
        RuleKind.TRANSFORM,
        RuleMode.WRITE,
        "CEL_FIELD",
        None,
        None,
        "typeName == 'STRING' ; value + '-suffix'",
        None,
        None,
        False
    )
    client.register_schema(_SUBJECT, Schema(
        json.dumps(schema),
        "JSON",
        [],
        None,
        RuleSet(None, [rule])
    ))

    obj = {
        'arrayField': ['a', 'b', 'c'],
        'objectField': {
            'stringField': 'hello'
        },
        'unionField': None
    }
    ser = JSONSerializer(json.dumps(schema), client, conf=ser_conf)
    ser_ctx = SerializationContext(_TOPIC, MessageField.VALUE)
    obj_bytes = ser(obj, ser_ctx)

    obj2 = {
        'arrayField': ['a-suffix', 'b-suffix', 'c-suffix'],
        'objectField': {
            'stringField': 'hello-suffix'
        },
        'unionField': None
    }
    deser = JSONDeserializer(None, schema_registry_client=client)
    newobj = deser(obj_bytes, ser_ctx)
    assert obj2 == newobj


def test_json_cel_field_condition():
    conf = {'url': _BASE_URL}
    client = SchemaRegistryClient.new_client(conf)
    ser_conf = {'auto.register.schemas': False, 'use.latest.version': True}
    schema = {
        "type": "object",
        "properties": {
            "intField": {"type": "integer"},
            "doubleField": {"type": "number"},
            "stringField": {
                "type": "string",
                "confluent:tags": ["PII"]
            },
            "booleanField": {"type": "boolean"},
            "bytesField": {
                "type": "string",
                "contentEncoding": "base64",
                "confluent:tags": ["PII"]
            }
        }
    }

    rule = Rule(
        "test-cel",
        "",
        RuleKind.CONDITION,
        RuleMode.WRITE,
        "CEL_FIELD",
        None,
        None,
        "name == 'stringField' ; value == 'hi'",
        None,
        None,
        False
    )
    client.register_schema(_SUBJECT, Schema(
        json.dumps(schema),
        "JSON",
        [],
        None,
        RuleSet(None, [rule])
    ))

    obj = {
        'intField': 123,
        'doubleField': 45.67,
        'stringField': 'hi',
        'booleanField': True,
        'bytesField': base64.b64encode(b'foobar').decode('utf-8'),
    }
    ser = JSONSerializer(json.dumps(schema), client, conf=ser_conf)
    ser_ctx = SerializationContext(_TOPIC, MessageField.VALUE)
    obj_bytes = ser(obj, ser_ctx)

    deser = JSONDeserializer(None, schema_registry_client=client)
    obj2 = deser(obj_bytes, ser_ctx)
    assert obj == obj2


def test_json_cel_field_condition_fail():
    conf = {'url': _BASE_URL}
    client = SchemaRegistryClient.new_client(conf)
    ser_conf = {'auto.register.schemas': False, 'use.latest.version': True}
    schema = {
        "type": "object",
        "properties": {
            "intField": {"type": "integer"},
            "doubleField": {"type": "number"},
            "stringField": {
                "type": "string",
                "confluent:tags": ["PII"]
            },
            "booleanField": {"type": "boolean"},
            "bytesField": {
                "type": "string",
                "contentEncoding": "base64",
                "confluent:tags": ["PII"]
            }
        }
    }

    rule = Rule(
        "test-cel",
        "",
        RuleKind.CONDITION,
        RuleMode.WRITE,
        "CEL_FIELD",
        None,
        None,
        "name == 'stringField' ; value != 'hi'",
        None,
        None,
        False
    )
    client.register_schema(_SUBJECT, Schema(
        json.dumps(schema),
        "JSON",
        [],
        None,
        RuleSet(None, [rule])
    ))

    obj = {
        'intField': 123,
        'doubleField': 45.67,
        'stringField': 'hi',
        'booleanField': True,
        'bytesField': base64.b64encode(b'foobar').decode('utf-8'),
    }
    ser = JSONSerializer(json.dumps(schema), client, conf=ser_conf)
    ser_ctx = SerializationContext(_TOPIC, MessageField.VALUE)
    with pytest.raises(SerializationError) as e:
        ser(obj, ser_ctx)
    assert isinstance(e.value.__cause__, RuleConditionError)


def test_json_encryption():
    executor = FieldEncryptionExecutor.register_with_clock(FakeClock())

    conf = {'url': _BASE_URL}
    client = SchemaRegistryClient.new_client(conf)
    ser_conf = {'auto.register.schemas': False, 'use.latest.version': True}
    rule_conf = {'secret': 'mysecret'}
    schema = {
        "type": "object",
        "properties": {
            "intField": {"type": "integer"},
            "doubleField": {"type": "number"},
            "stringField": {
                "type": "string",
                "confluent:tags": ["PII"]
            },
            "booleanField": {"type": "boolean"},
            "bytesField": {
                "type": "string",
                "contentEncoding": "base64",
                "confluent:tags": ["PII"]
            }
        }
    }

    rule = Rule(
        "test-encrypt",
        "",
        RuleKind.TRANSFORM,
        RuleMode.WRITEREAD,
        "ENCRYPT",
        ["PII"],
        RuleParams({
            "encrypt.kek.name": "kek1",
            "encrypt.kms.type": "local-kms",
            "encrypt.kms.key.id": "mykey"
        }),
        None,
        None,
        "ERROR,NONE",
        False
    )
    client.register_schema(_SUBJECT, Schema(
        json.dumps(schema),
        "JSON",
        [],
        None,
        RuleSet(None, [rule])
    ))

    obj = {
        'intField': 123,
        'doubleField': 45.67,
        'stringField': 'hi',
        'booleanField': True,
        'bytesField': base64.b64encode(b'foobar').decode('utf-8'),
    }
    ser = JSONSerializer(json.dumps(schema), client, conf=ser_conf, rule_conf=rule_conf)
    dek_client = executor.client
    ser_ctx = SerializationContext(_TOPIC, MessageField.VALUE)
    obj_bytes = ser(obj, ser_ctx)

    # reset encrypted fields
    assert obj['stringField'] != 'hi'
    obj['stringField'] = 'hi'
    obj['bytesField'] = base64.b64encode(b'foobar').decode('utf-8')

    deser = JSONDeserializer(None, schema_registry_client=client, rule_conf=rule_conf)
    executor.client = dek_client
    obj2 = deser(obj_bytes, ser_ctx)
    assert obj == obj2


def test_json_encryption_with_union():
    executor = FieldEncryptionExecutor.register_with_clock(FakeClock())

    conf = {'url': _BASE_URL}
    client = SchemaRegistryClient.new_client(conf)
    ser_conf = {'auto.register.schemas': False, 'use.latest.version': True}
    rule_conf = {'secret': 'mysecret'}
    schema = {
        "type": "object",
        "properties": {
            "intField": {"type": "integer"},
            "doubleField": {"type": "number"},
            "stringField": {
                "oneOf": [
                    {
                        "type": "null"
                    },
                    {
                        "type": "string"
                    }
                ],
                "confluent:tags": ["PII"]
            },
            "booleanField": {"type": "boolean"},
            "bytesField": {
                "type": "string",
                "contentEncoding": "base64",
                "confluent:tags": ["PII"]
            }
        }
    }

    rule = Rule(
        "test-encrypt",
        "",
        RuleKind.TRANSFORM,
        RuleMode.WRITEREAD,
        "ENCRYPT",
        ["PII"],
        RuleParams({
            "encrypt.kek.name": "kek1-union",
            "encrypt.kms.type": "local-kms",
            "encrypt.kms.key.id": "mykey"
        }),
        None,
        None,
        "ERROR,NONE",
        False
    )
    client.register_schema(_SUBJECT, Schema(
        json.dumps(schema),
        "JSON",
        [],
        None,
        RuleSet(None, [rule])
    ))

    obj = {
        'intField': 123,
        'doubleField': 45.67,
        'stringField': 'hi',
        'booleanField': True,
        'bytesField': base64.b64encode(b'foobar').decode('utf-8'),
    }
    ser = JSONSerializer(json.dumps(schema), client, conf=ser_conf, rule_conf=rule_conf)
    dek_client = executor.client
    ser_ctx = SerializationContext(_TOPIC, MessageField.VALUE)
    obj_bytes = ser(obj, ser_ctx)

    # reset encrypted fields
    assert obj['stringField'] != 'hi'
    obj['stringField'] = 'hi'
    obj['bytesField'] = base64.b64encode(b'foobar').decode('utf-8')

    deser = JSONDeserializer(None, schema_registry_client=client, rule_conf=rule_conf)
    executor.client = dek_client
    obj2 = deser(obj_bytes, ser_ctx)
    assert obj == obj2


def test_json_encryption_with_references():
    executor = FieldEncryptionExecutor.register_with_clock(FakeClock())

    conf = {'url': _BASE_URL}
    client = SchemaRegistryClient.new_client(conf)
    ser_conf = {'auto.register.schemas': False, 'use.latest.version': True}
    rule_conf = {'secret': 'mysecret'}
    schema = {
        "type": "object",
        "properties": {
            "intField": {"type": "integer"},
            "doubleField": {"type": "number"},
            "stringField": {
                "oneOf": [
                    {
                        "type": "null"
                    },
                    {
                        "type": "string"
                    }
                ],
                "confluent:tags": ["PII"]
            },
            "booleanField": {"type": "boolean"},
            "bytesField": {
                "type": "string",
                "contentEncoding": "base64",
                "confluent:tags": ["PII"]
            }
        }
    }
    client.register_schema('ref', Schema(json.dumps(schema), "JSON"))

    schema = {
        "type": "object",
        "properties": {
            "otherField": {"$ref": "ref"}
        }
    }
    refs = [SchemaReference('ref', 'ref', 1)]
    rule = Rule(
        "test-encrypt",
        "",
        RuleKind.TRANSFORM,
        RuleMode.WRITEREAD,
        "ENCRYPT",
        ["PII"],
        RuleParams({
            "encrypt.kek.name": "kek1-ref",
            "encrypt.kms.type": "local-kms",
            "encrypt.kms.key.id": "mykey"
        }),
        None,
        None,
        "ERROR,NONE",
        False
    )
    client.register_schema(_SUBJECT, Schema(
        json.dumps(schema),
        "JSON",
        refs,
        None,
        RuleSet(None, [rule])
    ))

    nested = {
        'intField': 123,
        'doubleField': 45.67,
        'stringField': 'hi',
        'booleanField': True,
        'bytesField': base64.b64encode(b'foobar').decode('utf-8'),
    }
    obj = {
        'otherField': nested
    }
    ser = JSONSerializer(json.dumps(schema), client, conf=ser_conf, rule_conf=rule_conf)
    dek_client = executor.client
    ser_ctx = SerializationContext(_TOPIC, MessageField.VALUE)
    obj_bytes = ser(obj, ser_ctx)

    # reset encrypted fields
    assert obj['otherField']['stringField'] != 'hi'
    obj['otherField']['stringField'] = 'hi'
    obj['otherField']['bytesField'] = base64.b64encode(b'foobar').decode('utf-8')

    deser = JSONDeserializer(None, schema_registry_client=client, rule_conf=rule_conf)
    executor.client = dek_client
    obj2 = deser(obj_bytes, ser_ctx)
    assert obj == obj2


def test_json_jsonata_fully_compatible():
    rule1_to_2 = "$merge([$sift($, function($v, $k) {$k != 'size'}), {'height': $.'size'}])"
    rule2_to_1 = "$merge([$sift($, function($v, $k) {$k != 'height'}), {'size': $.'height'}])"
    rule2_to_3 = "$merge([$sift($, function($v, $k) {$k != 'height'}), {'length': $.'height'}])"
    rule3_to_2 = "$merge([$sift($, function($v, $k) {$k != 'length'}), {'height': $.'length'}])"

    conf = {'url': _BASE_URL}
    client = SchemaRegistryClient.new_client(conf)

    client.set_config(_SUBJECT, ServerConfig(
        compatibility_group='application.version'
    ))

    schema = {
        "type": "object",
        "properties": {
            "name": {
                "type": "string",
                "confluent:tags": ["PII"]
            },
            "size": {"type": "number"},
            "version": {"type": "integer"}
        }
    }
    client.register_schema(_SUBJECT, Schema(
        json.dumps(schema),
        "JSON",
        [],
        Metadata(
            None,
            MetadataProperties({"application.version": "v1"}),
            None
        ),
        None
    ))

    schema = {
        "type": "object",
        "properties": {
            "name": {
                "type": "string",
                "confluent:tags": ["PII"]
            },
            "height": {"type": "number"},
            "version": {"type": "integer"}
        }
    }

    rule1 = Rule(
        "rule1",
        "",
        RuleKind.TRANSFORM,
        RuleMode.UPGRADE,
        "JSONATA",
        None,
        None,
        rule1_to_2,
        None,
        None,
        False
    )
    rule2 = Rule(
        "rule2",
        "",
        RuleKind.TRANSFORM,
        RuleMode.DOWNGRADE,
        "JSONATA",
        None,
        None,
        rule2_to_1,
        None,
        None,
        False
    )
    client.register_schema(_SUBJECT, Schema(
        json.dumps(schema),
        "JSON",
        [],
        Metadata(
            None,
            MetadataProperties({"application.version": "v2"}),
            None
        ),
        RuleSet([rule1, rule2], None)
    ))

    schema = {
        "type": "object",
        "properties": {
            "name": {
                "type": "string",
                "confluent:tags": ["PII"]
            },
            "length": {"type": "number"},
            "version": {"type": "integer"}
        }
    }

    rule3 = Rule(
        "rule3",
        "",
        RuleKind.TRANSFORM,
        RuleMode.UPGRADE,
        "JSONATA",
        None,
        None,
        rule2_to_3,
        None,
        None,
        False
    )
    rule4 = Rule(
        "rule4",
        "",
        RuleKind.TRANSFORM,
        RuleMode.DOWNGRADE,
        "JSONATA",
        None,
        None,
        rule3_to_2,
        None,
        None,
        False
    )
    client.register_schema(_SUBJECT, Schema(
        json.dumps(schema),
        "JSON",
        [],
        Metadata(
            None,
            MetadataProperties({"application.version": "v3"}),
            None
        ),
        RuleSet([rule3, rule4], None)
    ))

    obj = {
        'name': 'alice',
        'size': 123,
        'version': 1,
    }
    obj2 = {
        'name': 'alice',
        'height': 123,
        'version': 1,
    }
    obj3 = {
        'name': 'alice',
        'length': 123,
        'version': 1,
    }

    ser_conf = {
        'auto.register.schemas': False,
        'use.latest.version': False,
        'use.latest.with.metadata': {
            'application.version': 'v1'
        }
    }
    ser = JSONSerializer(json.dumps(schema), client, conf=ser_conf)
    ser_ctx = SerializationContext(_TOPIC, MessageField.VALUE)
    obj_bytes = ser(obj, ser_ctx)

    deserialize_with_all_versions(client, ser_ctx, obj_bytes, obj, obj2, obj3)

    ser_conf = {
        'auto.register.schemas': False,
        'use.latest.version': False,
        'use.latest.with.metadata': {
            'application.version': 'v2'
        }
    }
    ser = JSONSerializer(json.dumps(schema), client, conf=ser_conf)
    ser_ctx = SerializationContext(_TOPIC, MessageField.VALUE)
    obj_bytes = ser(obj2, ser_ctx)

    deserialize_with_all_versions(client, ser_ctx, obj_bytes, obj, obj2, obj3)

    ser_conf = {
        'auto.register.schemas': False,
        'use.latest.version': False,
        'use.latest.with.metadata': {
            'application.version': 'v3'
        }
    }
    ser = JSONSerializer(json.dumps(schema), client, conf=ser_conf)
    ser_ctx = SerializationContext(_TOPIC, MessageField.VALUE)
    obj_bytes = ser(obj3, ser_ctx)

    deserialize_with_all_versions(client, ser_ctx, obj_bytes, obj, obj2, obj3)


def deserialize_with_all_versions(client, ser_ctx, obj_bytes, obj, obj2, obj3):
    deser_conf = {
        'use.latest.with.metadata': {
            'application.version': 'v1'
        }
    }
    deser = JSONDeserializer(None, schema_registry_client=client, conf=deser_conf)
    newobj = deser(obj_bytes, ser_ctx)
    assert obj == newobj

    deser_conf = {
        'use.latest.with.metadata': {
            'application.version': 'v2'
        }
    }
    deser = JSONDeserializer(None, schema_registry_client=client, conf=deser_conf)
    newobj = deser(obj_bytes, ser_ctx)
    assert obj2 == newobj

    deser_conf = {
        'use.latest.with.metadata': {
            'application.version': 'v3'
        }
    }
    deser = JSONDeserializer(None, schema_registry_client=client, conf=deser_conf)
    newobj = deser(obj_bytes, ser_ctx)
    assert obj3 == newobj
