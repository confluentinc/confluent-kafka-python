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
import pytest

from confluent_kafka.schema_registry import (record_subject_name_strategy,
                                             SchemaRegistryClient,
                                             topic_record_subject_name_strategy)
from confluent_kafka.schema_registry.avro import AvroSerializer, AvroDeserializer
from confluent_kafka.serialization import (MessageField,
                                           SerializationContext)

# MockSchemaRegistryClient, see ./conftest.py for additional details.
TEST_URL = 'http://SchemaRegistry:65534'


def test_avro_serializer_config_auto_register_schemas():
    """
    Ensures auto.register.schemas is applied
    """
    conf = {'url': TEST_URL}
    test_client = SchemaRegistryClient(conf)
    test_serializer = AvroSerializer(test_client, '"string"',
                                     conf={'auto.register.schemas': False})
    assert not test_serializer._auto_register


def test_avro_serializer_config_auto_register_schemas_invalid():
    """
    Ensures auto.register.schemas is applied
    """
    conf = {'url': TEST_URL}
    test_client = SchemaRegistryClient(conf)

    with pytest.raises(ValueError, match="must be a boolean"):
        AvroSerializer(test_client, 'string',
                       conf={'auto.register.schemas': dict()})


def test_avro_serializer_config_auto_register_schemas_false(mock_schema_registry):
    """
    Ensures auto.register.schemas=False does not register schema
    """
    conf = {'url': TEST_URL}
    test_client = mock_schema_registry(conf)
    topic = "test-auto-register"
    subject = topic + '-key'

    test_serializer = AvroSerializer(test_client, '"string"',
                                     conf={'auto.register.schemas': False})

    test_serializer("test",
                    SerializationContext("test-auto-register",
                                         MessageField.KEY))

    register_count = test_client.counter['POST'].get('/subjects/{}/versions'
                                                     .format(subject), 0)
    assert register_count == 0
    # Ensure lookup_schema was invoked instead
    assert test_client.counter['POST'].get('/subjects/{}'.format(subject)) == 1


def test_avro_serializer_config_use_latest_version(mock_schema_registry):
    """
    Ensures auto.register.schemas=False does not register schema
    """
    conf = {'url': TEST_URL}
    test_client = mock_schema_registry(conf)
    topic = "test-use-latest-version"
    subject = topic + '-key'

    test_serializer = AvroSerializer(test_client, '"string"',
                                     conf={'auto.register.schemas': False, 'use.latest.version': True})

    test_serializer("test",
                    SerializationContext("test-use-latest-version",
                                         MessageField.KEY))

    register_count = test_client.counter['POST'].get('/subjects/{}/versions'
                                                     .format(subject), 0)
    assert register_count == 0
    # Ensure latest was requested
    assert test_client.counter['GET'].get('/subjects/{}/versions/latest'.format(subject)) == 1


def test_avro_serializer_config_subject_name_strategy():
    """
    Ensures subject.name.strategy is applied
    """

    conf = {'url': TEST_URL}
    test_client = SchemaRegistryClient(conf)
    test_serializer = AvroSerializer(test_client, '"int"',
                                     conf={'subject.name.strategy':
                                           record_subject_name_strategy})

    assert test_serializer._subject_name_func is record_subject_name_strategy


def test_avro_serializer_config_subject_name_strategy_invalid():
    """
    Ensures subject.name.strategy is applied
    """

    conf = {'url': TEST_URL}
    test_client = SchemaRegistryClient(conf)
    with pytest.raises(ValueError, match="must be callable"):
        AvroSerializer(test_client, '"int"',
                       conf={'subject.name.strategy': dict()})


def test_avro_serializer_record_subject_name_strategy(load_avsc):
    """
    Ensures record_subject_name_strategy returns the correct record name
    """
    conf = {'url': TEST_URL}
    test_client = SchemaRegistryClient(conf)
    test_serializer = AvroSerializer(test_client,
                                     load_avsc('basic_schema.avsc'),
                                     conf={'subject.name.strategy':
                                           record_subject_name_strategy})

    ctx = SerializationContext('test_subj', MessageField.VALUE, [])
    assert test_serializer._subject_name_func(ctx,
                                              test_serializer._schema_name) == 'python.test.basic'
    assert ctx is not None
    assert not ctx.headers


def test_avro_serializer_record_subject_name_strategy_primitive(load_avsc):
    """
    Ensures record_subject_name_strategy returns the correct record name.
    Also verifies transformation from Avro canonical form.
    """
    conf = {'url': TEST_URL}
    test_client = SchemaRegistryClient(conf)
    test_serializer = AvroSerializer(test_client, '"int"',
                                     conf={'subject.name.strategy':
                                           record_subject_name_strategy})

    ctx = SerializationContext('test_subj', MessageField.VALUE, [('header1', 'header value 1'), ])
    assert test_serializer._subject_name_func(ctx,
                                              test_serializer._schema_name) == 'int'
    assert ('header1', 'header value 1') in ctx.headers


def test_avro_serializer_topic_record_subject_name_strategy(load_avsc):
    """
    Ensures record_subject_name_strategy returns the correct record name
    """
    conf = {'url': TEST_URL}
    test_client = SchemaRegistryClient(conf)
    test_serializer = AvroSerializer(test_client,
                                     load_avsc('basic_schema.avsc'),
                                     conf={'subject.name.strategy':
                                           topic_record_subject_name_strategy})

    ctx = SerializationContext('test_subj', MessageField.VALUE)
    assert test_serializer._subject_name_func(
        ctx, test_serializer._schema_name) == 'test_subj-python.test.basic'


def test_avro_serializer_topic_record_subject_name_strategy_primitive(load_avsc):
    """
    Ensures record_subject_name_strategy returns the correct record name.
    Also verifies transformation from Avro canonical form.
    """
    conf = {'url': TEST_URL}
    test_client = SchemaRegistryClient(conf)
    test_serializer = AvroSerializer(test_client, '"int"',
                                     conf={'subject.name.strategy':
                                           topic_record_subject_name_strategy})

    ctx = SerializationContext('test_subj', MessageField.VALUE)
    assert test_serializer._subject_name_func(
        ctx, test_serializer._schema_name) == 'test_subj-int'
    assert ctx is not None
    assert ctx.headers is None


def test_avro_serializer_subject_name_strategy_default(load_avsc):
    """
    Ensures record_subject_name_strategy returns the correct record name
    """
    conf = {'url': TEST_URL}
    test_client = SchemaRegistryClient(conf)
    test_serializer = AvroSerializer(test_client,
                                     load_avsc('basic_schema.avsc'))

    ctx = SerializationContext('test_subj', MessageField.VALUE)
    assert test_serializer._subject_name_func(
        ctx, test_serializer._schema_name) == 'test_subj-value'


def test_avro_serializer_schema_loads_union(load_avsc):
    """
    Ensures union types are correctly parsed
    """
    conf = {'url': TEST_URL}
    test_client = SchemaRegistryClient(conf)
    test_serializer = AvroSerializer(test_client,
                                     load_avsc('union_schema.avsc'))

    assert test_serializer._schema_name is None

    schema = test_serializer._parsed_schema
    assert isinstance(schema, list)
    assert schema[0]["name"] == "RecordOne"
    assert schema[1]["name"] == "RecordTwo"


def test_avro_serializer_invalid_schema_type():
    """
    Ensures invalid schema types are rejected
    """
    conf = {'url': TEST_URL}
    test_client = SchemaRegistryClient(conf)
    with pytest.raises(TypeError, match="You must pass either schema string or schema object"):
        AvroSerializer(test_client, 1)


def test_avro_deserializer_invalid_schema_type():
    """
    Ensures invalid schema types are rejected
    """
    conf = {'url': TEST_URL}
    test_client = SchemaRegistryClient(conf)
    with pytest.raises(TypeError, match="You must pass either schema string or schema object"):
        AvroDeserializer(test_client, 1)
