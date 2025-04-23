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
from uuid import uuid1

import pytest

from confluent_kafka.schema_registry import Schema
from confluent_kafka.schema_registry.error import SchemaRegistryError
from tests.integration.conftest import kafka_cluster_fixture


@pytest.fixture(scope="module")
def kafka_cluster_cp_7_0_1():
    """
    Returns a Trivup cluster with CP version 7.0.1.
    SR version 7.0.1 is the last returning 500 instead of 422
    for the invalid schema passed to test_api_get_register_schema_invalid
    """
    for fixture in kafka_cluster_fixture(
        brokers_env="BROKERS_7_0_1",
        sr_url_env="SR_URL_7_0_1",
        trivup_cluster_conf={'cp_version': '7.0.1'}
    ):
        yield fixture


def _subject_name(prefix):
    return prefix + "-" + str(uuid1())


def test_api_register_schema(kafka_cluster, load_file):
    """
    Registers a schema, verifies the registration

    Args:
        kafka_cluster (KafkaClusterFixture): Kafka Cluster fixture
        load_file (callable(str)): Schema fixture constructor

    """
    sr = kafka_cluster.schema_registry()
    avsc = 'basic_schema.avsc'
    subject = _subject_name(avsc)
    schema = Schema(load_file(avsc), schema_type='AVRO')

    schema_id = sr.register_schema(subject, schema)
    registered_schema = sr.lookup_schema(subject, schema)

    assert registered_schema.schema_id == schema_id
    assert registered_schema.subject == subject
    assert schema.schema_str, registered_schema.schema.schema_str


def test_api_register_normalized_schema(kafka_cluster, load_file):
    """
    Registers a schema, verifies the registration

    Args:
        kafka_cluster (KafkaClusterFixture): Kafka Cluster fixture
        load_file (callable(str)): Schema fixture constructor

    """
    sr = kafka_cluster.schema_registry()
    avsc = 'basic_schema.avsc'
    subject = _subject_name(avsc)
    schema = Schema(load_file(avsc), schema_type='AVRO')

    schema_id = sr.register_schema(subject, schema, True)
    registered_schema = sr.lookup_schema(subject, schema, True)

    assert registered_schema.schema_id == schema_id
    assert registered_schema.subject == subject
    assert schema.schema_str, registered_schema.schema.schema_str


def test_api_register_schema_incompatible(kafka_cluster, load_file):
    """
    Attempts to register an incompatible Schema verifies the error.

    Args:
        kafka_cluster (KafkaClusterFixture): Kafka Cluster fixture
        load_file (callable(str)): Schema fixture constructor

    """
    sr = kafka_cluster.schema_registry()
    schema1 = Schema(load_file('basic_schema.avsc'), schema_type='AVRO')
    schema2 = Schema(load_file('adv_schema.avsc'), schema_type='AVRO')
    subject = _subject_name('test_register_incompatible')

    sr.register_schema(subject, schema1)

    with pytest.raises(SchemaRegistryError, match="Schema being registered is"
                                                  " incompatible with an"
                                                  " earlier schema") as e:
        # The default Schema Registry compatibility type is BACKWARD.
        # this allows 1) fields to be deleted, 2) optional fields to
        # be added. schema2 adds non-optional fields to schema1, so
        # registering schema2 after schema1 should fail.
        sr.register_schema(subject, schema2)
    assert e.value.http_status_code == 409  # conflict
    assert e.value.error_code == 409


def test_api_register_schema_invalid(kafka_cluster, load_file):
    """
    Attempts to register an invalid schema, validates the error.

    Args:
        kafka_cluster (KafkaClusterFixture): Kafka Cluster fixture
        load_file (callable(str)): Schema fixture constructor

    """
    sr = kafka_cluster.schema_registry()
    schema = Schema(load_file('invalid_schema.avsc'), schema_type='AVRO')
    subject = _subject_name('test_invalid_schema')

    with pytest.raises(SchemaRegistryError) as e:
        sr.register_schema(subject, schema)
    assert e.value.http_status_code == 422
    assert e.value.error_code == 42201


def test_api_get_schema(kafka_cluster, load_file):
    """
    Registers a schema then retrieves it using the schema id returned by the
    call to register the Schema.

    Args:
        kafka_cluster (KafkaClusterFixture): Kafka Cluster fixture
        load_file (callable(str)): Schema fixture constructor

    """
    sr = kafka_cluster.schema_registry()
    schema = Schema(load_file('basic_schema.avsc'), schema_type='AVRO')
    subject = _subject_name('get_schema')

    schema_id = sr.register_schema(subject, schema)
    registration = sr.lookup_schema(subject, schema)

    assert registration.schema_id == schema_id
    assert registration.subject == subject
    assert schema.schema_str, registration.schema.schema_str


def test_api_get_schema_not_found(kafka_cluster, load_file):
    """
    Attempts to fetch an unknown schema by id, validates the error.

    Args:
        kafka_cluster (KafkaClusterFixture): Kafka Cluster fixture
        load_file (callable(str)): Schema fixture constructor

    """
    sr = kafka_cluster.schema_registry()

    with pytest.raises(SchemaRegistryError, match="Schema .*not found.*") as e:
        sr.get_schema(999999)

    assert e.value.http_status_code == 404
    assert e.value.error_code == 40403


def test_api_get_registration_subject_not_found(kafka_cluster, load_file):
    """
    Attempts to obtain information about a schema's subject registration for
    an unknown subject.

    Args:
        kafka_cluster (KafkaClusterFixture): Kafka Cluster fixture
        load_file (callable(str)): Schema fixture constructor

    """
    sr = kafka_cluster.schema_registry()
    schema = Schema(load_file('basic_schema.avsc'), schema_type='AVRO')

    subject = _subject_name("registration_subject_not_found")

    with pytest.raises(SchemaRegistryError, match="Subject .*not found.*") as e:
        sr.lookup_schema(subject, schema)
    assert e.value.http_status_code == 404
    assert e.value.error_code == 40401


@pytest.mark.parametrize("kafka_cluster_name, http_status_code, error_code", [
    ["kafka_cluster_cp_7_0_1", 500, 500],
    ["kafka_cluster", 422, 42201],
])
def test_api_get_register_schema_invalid(
        kafka_cluster_name,
        http_status_code,
        error_code,
        load_file,
        request):
    """
    Attempts to obtain registration information with an invalid schema
    with different CP versions.

    Args:
        kafka_cluster_name (str): name of the Kafka Cluster fixture to use
        http_status_code (int): HTTP status return code expected in this version
        error_code (int): error code expected in this version
        load_file (callable(str)): Schema fixture constructor
        request (FixtureRequest): PyTest object giving access to the test context
    """
    kafka_cluster = request.getfixturevalue(kafka_cluster_name)
    sr = kafka_cluster.schema_registry()
    subject = _subject_name("registration_invalid_schema")
    schema = Schema(load_file('basic_schema.avsc'), schema_type='AVRO')

    # register valid schema so we don't hit subject not found exception
    sr.register_schema(subject, schema)
    schema2 = Schema(load_file('invalid_schema.avsc'), schema_type='AVRO')

    with pytest.raises(SchemaRegistryError, match="Invalid schema") as e:
        sr.lookup_schema(subject, schema2)

    assert e.value.http_status_code == http_status_code
    assert e.value.error_code == error_code


def test_api_get_subjects(kafka_cluster, load_file):
    """
    Populates KafkaClusterFixture SR instance with a fixed number of subjects
    then verifies the response includes them all.

    Args:
        kafka_cluster (KafkaClusterFixture): Kafka Cluster fixture
        load_file (callable(str)): Schema fixture constructor

    """
    sr = kafka_cluster.schema_registry()

    avscs = ['basic_schema.avsc', 'primitive_string.avsc',
             'primitive_bool.avsc', 'primitive_float.avsc']

    subjects = []
    for avsc in avscs:
        schema = Schema(load_file(avsc), schema_type='AVRO')
        subject = _subject_name(avsc)
        subjects.append(subject)

        sr.register_schema(subject, schema)

    registered = sr.get_subjects()

    assert all([s in registered for s in subjects])


def test_api_get_subject_versions(kafka_cluster, load_file):
    """
    Registers a Schema with a subject, lists the versions associated with that
    subject and ensures the versions and their schemas match what was
    registered.

    Args:
        kafka_cluster (KafkaClusterFixture): Kafka Cluster fixture
        load_file (callable(str)): Schema fixture constructor.

    """
    sr = kafka_cluster.schema_registry()

    subject = _subject_name("list-version-test")
    sr.set_compatibility(level="NONE")

    avscs = ['basic_schema.avsc', 'primitive_string.avsc',
             'primitive_bool.avsc', 'primitive_float.avsc']

    schemas = []
    for avsc in avscs:
        schema = Schema(load_file(avsc), schema_type='AVRO')
        schemas.append(schema)
        sr.register_schema(subject, schema)

    versions = sr.get_versions(subject)
    assert len(versions) == len(avscs)
    for schema in schemas:
        registered_schema = sr.lookup_schema(subject, schema)
        assert registered_schema.subject == subject
        assert registered_schema.version in versions

    # revert global compatibility level back to the default.
    sr.set_compatibility(level="BACKWARD")


def test_api_delete_subject(kafka_cluster, load_file):
    """
    Registers a Schema under a specific subject then deletes it.

    Args:
        kafka_cluster (KafkaClusterFixture): Kafka Cluster fixture
        load_file (callable(str)): Schema fixture constructor

    """
    sr = kafka_cluster.schema_registry()

    schema = Schema(load_file('basic_schema.avsc'), schema_type='AVRO')
    subject = _subject_name("test-delete")

    sr.register_schema(subject, schema)
    assert subject in sr.get_subjects()

    sr.delete_subject(subject)
    assert subject not in sr.get_subjects()


def test_api_delete_subject_not_found(kafka_cluster):
    sr = kafka_cluster.schema_registry()

    subject = _subject_name("test-delete_invalid_subject")

    with pytest.raises(SchemaRegistryError, match="Subject .*not found.*") as e:
        sr.delete_subject(subject)
    assert e.value.http_status_code == 404
    assert e.value.error_code == 40401


def test_api_get_subject_version(kafka_cluster, load_file):
    """
    Registers a schema, fetches that schema by it's subject version id.

    Args:
        kafka_cluster (KafkaClusterFixture): Kafka Cluster fixture
        load_file (callable(str)): Schema fixture constructor

    """
    sr = kafka_cluster.schema_registry()

    schema = Schema(load_file('basic_schema.avsc'), schema_type='AVRO')
    subject = _subject_name('test-get_subject')

    sr.register_schema(subject, schema)
    registered_schema = sr.lookup_schema(subject, schema)
    registered_schema2 = sr.get_version(subject, registered_schema.version)

    assert registered_schema2.schema_id == registered_schema.schema_id
    assert registered_schema2.schema.schema_str == registered_schema.schema.schema_str
    assert registered_schema2.version == registered_schema.version


def test_api_get_subject_version_no_version(kafka_cluster, load_file):
    sr = kafka_cluster.schema_registry()

    # ensures subject exists and has a single version
    schema = Schema(load_file('basic_schema.avsc'), schema_type='AVRO')
    subject = _subject_name('test-get_subject')
    sr.register_schema(subject, schema)

    with pytest.raises(SchemaRegistryError, match="Version .*not found") as e:
        sr.get_version(subject, version=3)
    assert e.value.http_status_code == 404
    assert e.value.error_code == 40402


def test_api_get_subject_version_invalid(kafka_cluster, load_file):
    sr = kafka_cluster.schema_registry()

    # ensures subject exists and has a single version
    schema = Schema(load_file('basic_schema.avsc'), schema_type='AVRO')
    subject = _subject_name('test-get_subject')
    sr.register_schema(subject, schema)

    with pytest.raises(SchemaRegistryError,
                       match="The specified version .*is not"
                       " a valid version id.*") as e:
        sr.get_version(subject, version='a')
    assert e.value.http_status_code == 422
    assert e.value.error_code == 42202


def test_api_post_subject_registration(kafka_cluster, load_file):
    """
    Registers a schema, fetches that schema by it's subject version id.

    Args:
        kafka_cluster (KafkaClusterFixture): Kafka Cluster fixture
        load_file (callable(str)): Schema fixture constructor

    """
    sr = kafka_cluster.schema_registry()

    schema = Schema(load_file('basic_schema.avsc'), schema_type='AVRO')
    subject = _subject_name('test_registration')

    schema_id = sr.register_schema(subject, schema)
    registered_schema = sr.lookup_schema(subject, schema)

    assert registered_schema.schema_id == schema_id
    assert registered_schema.subject == subject


def test_api_delete_subject_version(kafka_cluster, load_file):
    """
    Registers a Schema under a specific subject then deletes it.

    Args:
        kafka_cluster (KafkaClusterFixture): Kafka Cluster fixture
        load_file (callable(str)): Schema fixture constructor

    """
    sr = kafka_cluster.schema_registry()

    schema = Schema(load_file('basic_schema.avsc'), schema_type='AVRO')
    subject = str(uuid1())

    sr.register_schema(subject, schema)
    sr.delete_version(subject, 1)

    assert subject not in sr.get_subjects()


def test_api_subject_config_update(kafka_cluster, load_file):
    """
    Updates a subjects compatibility policy then ensures the same policy
    is returned when queried.

    Args:
        kafka_cluster (KafkaClusterFixture): Kafka Cluster fixture
        load_file (callable(str)): Schema fixture constructor

    """
    sr = kafka_cluster.schema_registry()

    schema = Schema(load_file('basic_schema.avsc'), schema_type='AVRO')
    subject = str(uuid1())

    sr.register_schema(subject, schema)
    sr.set_compatibility(subject_name=subject,
                         level="FULL_TRANSITIVE")

    assert sr.get_compatibility(subject_name=subject) == "FULL_TRANSITIVE"


def test_api_config_invalid(kafka_cluster):
    """
    Sets an invalid compatibility level, validates the exception.

    Args:
        kafka_cluster (KafkaClusterFixture): Kafka Cluster fixture
    """
    sr = kafka_cluster.schema_registry()

    with pytest.raises(SchemaRegistryError, match="Invalid compatibility"
                                                  " level") as e:
        sr.set_compatibility(level="INVALID")
    e.value.http_status_code = 422
    e.value.error_code = 42203


def test_api_config_update(kafka_cluster):
    """
    Updates the global compatibility policy then ensures the same policy
    is returned when queried.

    Args:
        kafka_cluster (KafkaClusterFixture): Kafka Cluster fixture
    """
    sr = kafka_cluster.schema_registry()

    for level in ["BACKWARD", "BACKWARD_TRANSITIVE", "FORWARD", "FORWARD_TRANSITIVE"]:
        sr.set_compatibility(level=level)
        assert sr.get_compatibility() == level

    # revert global compatibility level back to the default.
    sr.set_compatibility(level="BACKWARD")


def test_api_register_logical_schema(kafka_cluster, load_file):
    sr = kafka_cluster.schema_registry()

    schema = Schema(load_file('logical_date.avsc'), schema_type='AVRO')
    subject = _subject_name('test_logical_registration')

    schema_id = sr.register_schema(subject, schema)
    registered_schema = sr.lookup_schema(subject, schema)

    assert registered_schema.schema_id == schema_id
    assert registered_schema.subject == subject
