#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright 2023 Confluent Inc.
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

import pytest

from confluent_kafka.schema_registry import SchemaReference, Schema
from confluent_kafka.schema_registry.json_schema import JSONDeserializer, JSONSerializer


def test_json_deserializer_referenced_schema_no_schema_registry_client(load_avsc):
    """
    Ensures that the deserializer raises a ValueError if a referenced schema is provided but no schema registry
    client is provided.
    """
    schema = Schema(load_avsc("order_details.json"), 'JSON',
                    [SchemaReference("http://example.com/customer.schema.json", "customer", 1)])
    with pytest.raises(
            ValueError,
            match="""schema_registry_client must be provided if "schema_str" is a Schema instance with references"""):
        JSONDeserializer(schema, schema_registry_client=None)


def test_json_deserializer_invalid_schema_type():
    """
    Ensures that the deserializer raises a ValueError if an invalid schema type is provided.
    """
    with pytest.raises(TypeError, match="You must pass either str or Schema"):
        JSONDeserializer(1)


def test_json_serializer_invalid_schema_type():
    """
    Ensures that the serializer raises a ValueError if an invalid schema type is provided.
    """
    with pytest.raises(TypeError, match="You must pass either str or Schema"):
        JSONSerializer(1, schema_registry_client=None)
