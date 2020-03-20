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
import json
from uuid import uuid1

from confluent_kafka.schema_registry.schema_registry_client import CompatibilityType


def _subject_name(prefix):
    return prefix + "-" + str(uuid1())


def cmp_schema(schema1, schema2):
    schema_dict1 = json.loads(schema1)
    schema_dict2 = json.loads(schema2)

    return all({v == schema_dict2[k] for k, v in schema_dict1.items()})


def test_api_get_schema(kafka_cluster, load_avsc):
    """
    Registers a schema then retrieves it using the schema id returned from the
    call to register the Schema.

    Args:
        kafka_cluster (KafkaClusterFixture): Kafka Cluster fixture
        load_avsc (callable(str)): Schema fixture constructor

    Raises:
        AssertionError if the Schema retrieved by ID does not match the expected
        Schema
    """
    sr = kafka_cluster.schema_registry()
    schema = load_avsc('basic_schema.avsc')
    subject = _subject_name('get_schema')

    schema_id = sr.register_schema(subject, schema)
    registration = sr.get_registration(subject, schema)

    assert registration.schema_id == schema_id
    assert cmp_schema(schema.schema, registration.schema)


def test_api_get_subjects(kafka_cluster, load_avsc):
    """
    Populates KafkaClusterFixture SR instance with a fixed number of subjects
    then verifies the response includes them all.

    Args:
        kafka_cluster (KafkaClusterFixture): Kafka Cluster fixture
        load_avsc (callable(str)): Schema fixture constructor

    Raises:
        AssertionError if all registered subjects are not returned
    """
    sr = kafka_cluster.schema_registry()

    avscs = ['basic_schema.avsc', 'primitive_string.avsc',
             'primitive_bool.avsc', 'primitive_float.avsc']

    subjects = []
    for avsc in avscs:
        schema = load_avsc(avsc)
        subject = _subject_name(avsc)
        subjects.append(subject)

        sr.register_schema(subject, schema)

    registered = sr.get_subjects()

    assert all([s in registered for s in subjects])


def test_api_register_schema(kafka_cluster, load_avsc):
    """
    Registers a schema

    Args:
        kafka_cluster (KafkaClusterFixture): Kafka Cluster fixture
        load_avsc (callable(str)): Schema fixture constructor

    Raises:
        AssertionError if all registered subjects are not returned
    """
    sr = kafka_cluster.schema_registry()
    avsc = 'basic_schema.avsc'
    subject = _subject_name(avsc)
    schema = load_avsc(avsc)

    schema_id = sr.register_schema(subject, schema)
    registered_schema = sr.get_registration(subject, schema)

    assert registered_schema.schema_id == schema_id
    assert registered_schema.subject == subject
    assert cmp_schema(schema.schema, registered_schema.schema)


def test_api_get_subject_versions(kafka_cluster, load_avsc):
    """
    Registers a Schema with a subject, lists the versions associated with that
    subject and ensures the versions and their schemas match what was
    registered.

    Args:
        kafka_cluster (KafkaClusterFixture): Kafka Cluster fixture
        load_avsc (callable(str)): Schema fixture constructor

    Raises:
        AssertionError if the versions or schemas do not match what was
        registered.

    """
    sr = kafka_cluster.schema_registry()

    subject = _subject_name("list-version-test")
    sr.set_compatibility(level=CompatibilityType.NONE)

    avscs = ['basic_schema.avsc', 'primitive_string.avsc',
             'primitive_bool.avsc', 'primitive_float.avsc']

    schemas = []
    for avsc in avscs:
        schema = load_avsc(avsc)
        schemas.append(schema)
        sr.register_schema(subject, schema)

    versions = sr.list_versions(subject)
    assert len(versions) == len(avscs)
    for schema in schemas:
        registered_schema = sr.get_registration(subject, schema)
        assert registered_schema.subject == subject
        assert registered_schema.version in versions


def test_api_delete_subject(kafka_cluster, load_avsc):
    """
    Registers a Schema under a specific subject then deletes it.

    Args:
        kafka_cluster (KafkaClusterFixture): Kafka Cluster fixture
        load_avsc (callable(str)): Schema fixture constructor

    Raises:
        AssertionError if the version number is not returned in the response or
        if the subject has not been deleted.

    """
    sr = kafka_cluster.schema_registry()

    schema = load_avsc('basic_schema.avsc')
    subject = _subject_name("test-delete")

    sr.register_schema(subject, schema)
    sr.delete_subject(subject)

    assert subject not in sr.get_subjects()


def test_api_get_subject_version(kafka_cluster, load_avsc):
    """
    Registers a schema, fetches that schema by it's subject version id.

    Args:
        kafka_cluster (KafkaClusterFixture): Kafka Cluster fixture
        load_avsc (callable(str)): Schema fixture constructor

    Raises:
        AssertionError if the returned schema and id do not match the
        registration schema and schema id.

    """
    sr = kafka_cluster.schema_registry()

    schema = load_avsc('basic_schema.avsc')
    subject = _subject_name('test-get_subject')

    sr.register_schema(subject, schema)
    registered_schema = sr.get_registration(subject, schema)
    version = sr.get_version(subject, registered_schema.version)

    assert version.schema_id == registered_schema.schema_id
    assert version.schema == registered_schema.schema
    assert version.version == registered_schema.version


def test_api_post_subject_registration(kafka_cluster, load_avsc):
    """
    Registers a schema, fetches that schema by it's subject version id.

    Args:
        kafka_cluster (KafkaClusterFixture): Kafka Cluster fixture
        load_avsc (callable(str)): Schema fixture constructor

    Raises:
        AssertionError if the returned schema and id do not match the
        registration schema and schema id.

    """
    sr = kafka_cluster.schema_registry()

    schema = load_avsc('basic_schema.avsc')
    subject = _subject_name('test_registration')

    schema_id = sr.register_schema(subject, schema)
    registered_schema = sr.get_registration(subject, schema)

    assert registered_schema.schema_id == schema_id
    assert registered_schema.subject == subject


def test_api_delete_subject_version(kafka_cluster, load_avsc):
    """
    Registers a Schema under a specific subject then deletes it.

    Args:
        kafka_cluster (KafkaClusterFixture): Kafka Cluster fixture
        load_avsc (callable(str)): Schema fixture constructor

    Raises:
        AssertionError if the version number is not returned in the response or
        if the subject has not been deleted.

    """
    sr = kafka_cluster.schema_registry()

    schema = load_avsc('basic_schema.avsc')
    subject = str(uuid1())

    sr.register_schema(subject, schema)
    sr.delete_version(subject, 1)

    assert subject not in sr.get_subjects()


def test_api_subject_config_update(kafka_cluster, load_avsc):
    """
    Updates a subjects compatibility policy then ensures the same policy
    is returned when queried.

    Args:
        kafka_cluster (KafkaClusterFixture): Kafka Cluster fixture
        load_avsc (callable(str)): Schema fixture constructor

    Raises:
        AssertionError if the compatibility policy returned by the schema
        registry fixture does not match the updated value.

    """
    sr = kafka_cluster.schema_registry()

    schema = load_avsc('basic_schema.avsc')
    subject = str(uuid1())

    sr.register_schema(subject, schema)
    sr.set_compatibility(subject_name=subject,
                         level=CompatibilityType.FULL_TRANSITIVE)

    assert sr.get_compatibility(
        subject_name=subject)['compatibilityLevel'] == CompatibilityType.FULL_TRANSITIVE


def test_api_config_update(kafka_cluster, load_avsc):
    """
    Updates a global compatibility policy then ensures the same policy
    is returned when queried.

    Args:
        kafka_cluster (KafkaClusterFixture): Kafka Cluster fixture
        load_avsc (callable(str)): Schema fixture constructor

    Raises:
        AssertionError if the compatibility policy returned by the schema
        registry fixture does not match the updated value.

    """
    sr = kafka_cluster.schema_registry()

    sr.set_compatibility(level=CompatibilityType.FORWARD_TRANSITIVE)

    assert sr.get_compatibility()['compatibilityLevel'] == CompatibilityType.FORWARD_TRANSITIVE
