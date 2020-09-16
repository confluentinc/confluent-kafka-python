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

from concurrent.futures import ThreadPoolExecutor, wait

from confluent_kafka.schema_registry.error import SchemaRegistryError
from confluent_kafka.schema_registry.schema_registry_client import Schema

"""
    Basic SchemaRegistryClient API functionality tests.

    These tests cover the following criteria using the MockSchemaRegistryClient:
        - Proper request/response handling:
            The right data sent to the right place in the right format
        - Error handling: (SR error codes are converted to a
            SchemaRegistryError correctly)
        - Caching: Caching of schema_ids and schemas works as expected.

    See ./conftest.py for details on MockSchemaRegistryClient usage.
"""
TEST_URL = 'http://SchemaRegistry:65534'
TEST_USERNAME = 'sr_user'
TEST_USER_PASSWORD = 'sr_user_secret'


def cmp_schema(schema1, schema2):
    """
    Compare to Schemas for equivalence

    Args:
        schema1 (Schema): Schema instance to compare
        schema2 (Schema): Schema instance to compare against

    Returns:
        bool: True if the schema's match else False

    """
    return all([schema1.schema_str == schema2.schema_str,
                schema1.schema_type == schema2.schema_type])


def test_basic_auth_unauthorized(mock_schema_registry, load_avsc):
    conf = {'url': TEST_URL,
            'basic.auth.user.info': "user:secret"}
    sr = mock_schema_registry(conf)

    with pytest.raises(SchemaRegistryError, match="401 Unauthorized"):
        sr.get_subjects()


def test_basic_auth_authorized(mock_schema_registry, load_avsc):
    conf = {'url': TEST_URL,
            'basic.auth.user.info': mock_schema_registry.USERINFO}
    sr = mock_schema_registry(conf)

    result = sr.get_subjects()

    assert result == mock_schema_registry.SUBJECTS


def test_register_schema(mock_schema_registry, load_avsc):
    conf = {'url': TEST_URL}
    sr = mock_schema_registry(conf)
    schema = Schema(load_avsc('basic_schema.avsc'), schema_type='AVRO')

    result = sr.register_schema('test-key', schema)
    assert result == mock_schema_registry.SCHEMA_ID


def test_register_schema_incompatible(mock_schema_registry, load_avsc):
    conf = {'url': TEST_URL}
    sr = mock_schema_registry(conf)
    schema = Schema(load_avsc('basic_schema.avsc'), schema_type='AVRO')

    with pytest.raises(SchemaRegistryError, match="Incompatible Schema") as e:
        sr.register_schema('conflict', schema)

    assert e.value.http_status_code == 409
    assert e.value.error_code == -1


def test_register_schema_invalid(mock_schema_registry, load_avsc):
    conf = {'url': TEST_URL}
    sr = mock_schema_registry(conf)
    schema = Schema(load_avsc('invalid_schema.avsc'), schema_type='AVRO')

    with pytest.raises(SchemaRegistryError, match="Invalid Schema") as e:
        sr.register_schema('invalid', schema)

    assert e.value.http_status_code == 422
    assert e.value.error_code == 42201


def test_register_schema_cache(mock_schema_registry, load_avsc):
    conf = {'url': TEST_URL}
    sr = mock_schema_registry(conf)
    schema = load_avsc('basic_schema.avsc')

    count_before = sr.counter['POST'].get(
        '/subjects/test-cache/versions', 0)

    # Caching only starts after the first response is handled.
    # A possible improvement would be to add request caching to the http client
    # to catch in-flight requests as well.
    sr.register_schema('test-cache', Schema(schema, 'AVRO'))

    fs = []
    with ThreadPoolExecutor(max_workers=10) as executor:
        for _ in range(0, 1000):
            fs.append(executor.submit(sr.register_schema,
                                      'test-cache', schema))
    wait(fs)

    count_after = sr.counter['POST'].get(
        '/subjects/test-cache/versions')

    assert count_after - count_before == 1


def test_get_schema(mock_schema_registry, load_avsc):
    conf = {'url': TEST_URL}
    sr = mock_schema_registry(conf)

    schema = Schema(load_avsc(mock_schema_registry.SCHEMA), schema_type='AVRO')
    schema2 = sr.get_schema(47)

    assert cmp_schema(schema, schema2)


def test_get_schema_not_found(mock_schema_registry):
    conf = {'url': TEST_URL}
    sr = mock_schema_registry(conf)

    with pytest.raises(SchemaRegistryError, match="Schema not found") as e:
        sr.get_schema(404)
    assert e.value.http_status_code == 404
    assert e.value.error_code == 40403


def test_get_schema_cache(mock_schema_registry):
    conf = {'url': TEST_URL}
    sr = mock_schema_registry(conf)

    count_before = mock_schema_registry.counter['GET'].get(
        '/schemas/ids/47', 0)

    # Caching only starts after the first response is handled.
    # A possible improvement would be to add request caching to the http client
    # to catch in-flight requests as well.
    sr.get_schema(47)

    fs = []
    with ThreadPoolExecutor(max_workers=10) as executor:
        for _ in range(0, 1000):
            fs.append(executor.submit(sr.get_schema, 47))
    wait(fs)

    count_after = mock_schema_registry.counter['GET'].get(
        '/schemas/ids/47')

    assert count_after - count_before == 1


def test_get_registration(mock_schema_registry, load_avsc):
    conf = {'url': TEST_URL}
    sr = mock_schema_registry(conf)

    subject = 'get_registration'
    schema = Schema(load_avsc(mock_schema_registry.SCHEMA), schema_type='AVRO')

    response = sr.lookup_schema(subject, schema)

    assert response.subject == subject
    assert response.version == mock_schema_registry.VERSION
    assert response.schema_id == mock_schema_registry.SCHEMA_ID
    assert cmp_schema(response.schema, schema)


def test_get_registration_subject_not_found(mock_schema_registry, load_avsc):
    conf = {'url': TEST_URL}
    sr = mock_schema_registry(conf)

    subject = 'notfound'
    schema = Schema(load_avsc(mock_schema_registry.SCHEMA), schema_type='AVRO')

    with pytest.raises(SchemaRegistryError, match="Subject not found") as e:
        sr.lookup_schema(subject, schema)
    assert e.value.http_status_code == 404
    assert e.value.error_code == 40401


def test_get_registration_schema_not_found(mock_schema_registry, load_avsc):
    conf = {'url': TEST_URL}
    sr = mock_schema_registry(conf)

    subject = 'schemanotfound'
    schema = Schema(load_avsc(mock_schema_registry.SCHEMA), schema_type='AVRO')

    with pytest.raises(SchemaRegistryError, match="Schema not found") as e:
        sr.lookup_schema(subject, schema)
    assert e.value.http_status_code == 404
    assert e.value.error_code == 40403


def test_get_subjects(mock_schema_registry):
    conf = {'url': TEST_URL}
    sr = mock_schema_registry(conf)

    result = sr.get_subjects()

    assert result == mock_schema_registry.SUBJECTS


def test_delete(mock_schema_registry):
    conf = {'url': TEST_URL}
    sr = mock_schema_registry(conf)

    result = sr.delete_subject("delete_subject")
    assert result == mock_schema_registry.VERSIONS


def test_delete_subject_not_found(mock_schema_registry):
    conf = {'url': TEST_URL}
    sr = mock_schema_registry(conf)

    with pytest.raises(SchemaRegistryError, match="Subject not found") as e:
        sr.delete_subject("notfound")
    assert e.value.http_status_code == 404
    assert e.value.error_code == 40401


def test_get_version(mock_schema_registry, load_avsc):
    conf = {'url': TEST_URL}
    sr = mock_schema_registry(conf)

    subject = "get_version"
    version = 3
    schema = Schema(load_avsc(mock_schema_registry.SCHEMA), schema_type='AVRO')

    result = sr.get_version(subject, version)
    assert result.subject == subject
    assert result.version == version
    assert cmp_schema(result.schema, schema)
    assert result.schema_id == mock_schema_registry.SCHEMA_ID


def test_get_version_no_version(mock_schema_registry):
    conf = {'url': TEST_URL}
    sr = mock_schema_registry(conf)

    subject = "get_version"
    version = 404

    with pytest.raises(SchemaRegistryError, match="Version not found") as e:
        sr.get_version(subject, version)
    assert e.value.http_status_code == 404
    assert e.value.error_code == 40402


def test_get_version_invalid(mock_schema_registry):
    conf = {'url': TEST_URL}
    sr = mock_schema_registry(conf)

    subject = "get_version"
    version = 422

    with pytest.raises(SchemaRegistryError, match="Invalid version") as e:
        sr.get_version(subject, version)
    assert e.value.http_status_code == 422
    assert e.value.error_code == 42202


def test_get_version_subject_not_found(mock_schema_registry):
    conf = {'url': TEST_URL}
    sr = mock_schema_registry(conf)

    subject = "notfound"
    version = 3

    with pytest.raises(SchemaRegistryError, match="Subject not found") as e:
        sr.get_version(subject, version)
    assert e.value.http_status_code == 404
    assert e.value.error_code == 40401


def test_delete_version(mock_schema_registry):
    conf = {'url': TEST_URL}
    sr = mock_schema_registry(conf)

    result = sr.delete_version("delete_version", 3)

    assert result == 3


def test_delete_version_not_found(mock_schema_registry):
    conf = {'url': TEST_URL}
    sr = mock_schema_registry(conf)

    with pytest.raises(SchemaRegistryError, match="Version not found") as e:
        sr.delete_version("delete_version", 404)
    assert e.value.http_status_code == 404
    assert e.value.error_code == 40402


def test_delete_version_subject_not_found(mock_schema_registry):
    conf = {'url': TEST_URL}
    sr = mock_schema_registry(conf)

    with pytest.raises(SchemaRegistryError, match="Subject not found") as e:
        sr.delete_version("notfound", 3)
    assert e.value.http_status_code == 404
    assert e.value.error_code == 40401


def test_delete_version_invalid(mock_schema_registry):
    conf = {'url': TEST_URL}
    sr = mock_schema_registry(conf)

    with pytest.raises(SchemaRegistryError, match="Invalid version") as e:
        sr.delete_version("invalid_version", 422)
    assert e.value.http_status_code == 422
    assert e.value.error_code == 42202


def test_set_compatibility(mock_schema_registry):
    conf = {'url': TEST_URL}
    sr = mock_schema_registry(conf)

    result = sr.set_compatibility(level="FULL")
    assert result == {'compatibility': 'FULL'}


def test_set_compatibility_invalid(mock_schema_registry):
    conf = {'url': TEST_URL}
    sr = mock_schema_registry(conf)
    with pytest.raises(SchemaRegistryError, match="Invalid compatibility level") as e:
        sr.set_compatibility(level="INVALID")
    e.value.http_status_code = 422
    e.value.error_code = 42203


def test_get_compatibility_subject_not_found(mock_schema_registry):
    conf = {'url': TEST_URL}
    sr = mock_schema_registry(conf)

    with pytest.raises(SchemaRegistryError, match="Subject not found") as e:
        sr.get_compatibility("notfound")
    assert e.value.http_status_code == 404
    assert e.value.error_code == 40401


def test_schema_equivilence(load_avsc):
    schema_str1 = load_avsc('basic_schema.avsc')
    schema_str2 = load_avsc('basic_schema.avsc')

    schema = Schema(schema_str1, 'AVRO')
    schema2 = Schema(schema_str2, 'AVRO')

    assert schema.__eq__(schema2)
    assert schema == schema2
    assert schema_str1.__eq__(schema_str2)
    assert schema_str1 == schema_str2


@pytest.mark.parametrize(
    'subject_name,version,expected_compatibility',
    [
        ('conflict', 'latest', False),
        ('conflict', 1, False),
        ('test-key', 'latest', True),
        ('test-key', 1, True),
    ]
)
def test_test_compatibility_no_error(
    mock_schema_registry, load_avsc, subject_name, version, expected_compatibility
):
    conf = {'url': TEST_URL}
    sr = mock_schema_registry(conf)
    schema = Schema(load_avsc('basic_schema.avsc'), schema_type='AVRO')

    is_compatible = sr.test_compatibility(subject_name, schema)
    assert is_compatible is expected_compatibility


@pytest.mark.parametrize(
    'subject_name,version,match_str,status_code,error_code',
    [
        ('notfound', 'latest', 'Subject not found', 404, 40401),
        ('invalid', 'latest', 'Invalid Schema', 422, 42201),
        ('invalid', '422', 'Invalid version', 422, 42202),
        ('notfound', 404, 'Version not found', 404, 40402),
    ]
)
def test_test_compatibility_with_error(
    mock_schema_registry, load_avsc, subject_name, version, match_str, status_code, error_code
):
    conf = {'url': TEST_URL}
    sr = mock_schema_registry(conf)
    schema = Schema(load_avsc('basic_schema.avsc'), schema_type='AVRO')

    with pytest.raises(SchemaRegistryError, match=match_str) as e:
        sr.test_compatibility(subject_name, schema, version)
    assert e.value.http_status_code == status_code
    assert e.value.error_code == error_code
