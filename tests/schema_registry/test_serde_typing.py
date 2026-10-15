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
import inspect
import os

import pytest

from confluent_kafka.schema_registry.avro import AvroDeserializer, AvroSerializer
from confluent_kafka.schema_registry.json_schema import JSONDeserializer, JSONSerializer
from confluent_kafka.schema_registry.protobuf import ProtobufDeserializer, ProtobufSerializer

SERDE_CLASSES = [
    AvroSerializer,
    AvroDeserializer,
    JSONSerializer,
    JSONDeserializer,
    ProtobufSerializer,
    ProtobufDeserializer,
]


@pytest.mark.parametrize("cls", SERDE_CLASSES)
def test_serde_init_method_name(cls):
    """
    Ensure constructor is directly defined as __init__ and not assigned from __init_impl.
    """
    assert cls.__init__.__name__ == "__init__"


def test_serde_init_signatures():
    """
    Ensure constructors expose their expected parameters in inspect.signature.
    """
    sig = inspect.signature(AvroSerializer.__init__)
    assert "schema_registry_client" in sig.parameters
    assert "schema_str" in sig.parameters
    assert "to_dict" in sig.parameters

    sig = inspect.signature(AvroDeserializer.__init__)
    assert "schema_registry_client" in sig.parameters
    assert "schema_str" in sig.parameters
    assert "from_dict" in sig.parameters

    sig = inspect.signature(JSONSerializer.__init__)
    assert "schema_str" in sig.parameters
    assert "schema_registry_client" in sig.parameters

    sig = inspect.signature(JSONDeserializer.__init__)
    assert "schema_str" in sig.parameters
    assert "from_dict" in sig.parameters
    assert "schema_registry_client" in sig.parameters

    sig = inspect.signature(ProtobufSerializer.__init__)
    assert "msg_type" in sig.parameters
    assert "schema_registry_client" in sig.parameters

    sig = inspect.signature(ProtobufDeserializer.__init__)
    assert "message_type" in sig.parameters
    assert "schema_registry_client" in sig.parameters


def test_serde_mypy_type_checking():
    """
    Ensure mypy correctly resolves the return type and parameter types for all
    serializers and deserializers without degrading to Any (#2390).
    """
    try:
        import mypy.api
    except ImportError:
        pytest.skip("mypy not installed")

    env_backup = os.environ.get("MYPYPATH")
    try:
        os.environ["MYPYPATH"] = "src"

        # 1. Valid snippet should pass without any errors (including --warn-return-any)
        valid_code = """
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer, AvroDeserializer
from confluent_kafka.schema_registry.json_schema import JSONSerializer, JSONDeserializer
from confluent_kafka.schema_registry.protobuf import ProtobufSerializer, ProtobufDeserializer

def build_avro_s(c: SchemaRegistryClient, s: str) -> AvroSerializer:
    return AvroSerializer(schema_registry_client=c, schema_str=s)

def build_avro_d(c: SchemaRegistryClient, s: str) -> AvroDeserializer:
    return AvroDeserializer(schema_registry_client=c, schema_str=s)

def build_json_s(c: SchemaRegistryClient, s: str) -> JSONSerializer:
    return JSONSerializer(schema_str=s, schema_registry_client=c)

def build_json_d(c: SchemaRegistryClient, s: str) -> JSONDeserializer:
    return JSONDeserializer(schema_str=s, schema_registry_client=c)

def build_proto_s(c: SchemaRegistryClient) -> ProtobufSerializer:
    return ProtobufSerializer(msg_type=None, schema_registry_client=c)

def build_proto_d(c: SchemaRegistryClient) -> ProtobufDeserializer:
    return ProtobufDeserializer(message_type=None, schema_registry_client=c)
"""
        stdout, stderr, exit_status = mypy.api.run([
            "-c",
            valid_code,
            "--follow-imports=silent",
            "--warn-return-any",
        ])
        assert exit_status == 0, f"mypy failed on valid code:\nstdout: {stdout}\nstderr: {stderr}"

        # 2. Invalid parameter type should be caught by mypy
        invalid_code = """
from confluent_kafka.schema_registry.avro import AvroSerializer

def build() -> AvroSerializer:
    return AvroSerializer(schema_str=12345)
"""
        stdout, stderr, exit_status = mypy.api.run([
            "-c",
            invalid_code,
            "--follow-imports=silent",
        ])
        assert exit_status == 1, "mypy should have failed on invalid argument type"
        assert 'Argument "schema_str" to "AvroSerializer" has incompatible type "int"' in stdout
    finally:
        if env_backup is not None:
            os.environ["MYPYPATH"] = env_backup
        else:
            os.environ.pop("MYPYPATH", None)
