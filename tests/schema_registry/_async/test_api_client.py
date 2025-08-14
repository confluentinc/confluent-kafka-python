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
import pytest
import asyncio
from concurrent.futures import ThreadPoolExecutor, wait

from confluent_kafka.schema_registry.common.schema_registry_client import SchemaVersion
from confluent_kafka.schema_registry.error import SchemaRegistryError
from confluent_kafka.schema_registry.schema_registry_client import Schema, \
    AsyncSchemaRegistryClient
from tests.schema_registry.conftest import USERINFO, \
    SCHEMA_ID, SCHEMA, SUBJECTS, COUNTER, VERSION, VERSIONS

"""
    Basic AsyncSchemaRegistryClient API functionality tests.

    These tests cover the following criteria using the mock AsyncSchemaRegistryClient:
        - Proper request/response handling:
            The right data sent to the right place in the right format
        - Error handling: (SR error codes are converted to a
            SchemaRegistryError correctly)
        - Caching: Caching of schema_ids and schemas works as expected.

    See ./conftest.py for details on mock AsyncSchemaRegistryClient usage.
"""
TEST_URL = 'http://SchemaRegistry:65534'
TEST_USERNAME = 'sr_user'
TEST_USER_PASSWORD = 'sr_user_secret'


def cmp_schema(schema1: Schema, schema2: Schema) -> bool:
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


async def test_basic_auth_unauthorized(mock_schema_registry, load_avsc):
    conf = {'url': TEST_URL,
            'basic.auth.user.info': "user:secret"}
    sr = AsyncSchemaRegistryClient(conf)

    with pytest.raises(SchemaRegistryError, match="401 Unauthorized"):
        await sr.get_subjects()


async def test_basic_auth_authorized(mock_schema_registry, load_avsc):
    conf = {'url': TEST_URL,
            'basic.auth.user.info': USERINFO}
    sr = AsyncSchemaRegistryClient(conf)

    result = await sr.get_subjects()

    assert result == SUBJECTS


async def test_register_schema(mock_schema_registry, load_avsc):
    conf = {'url': TEST_URL}
    sr = AsyncSchemaRegistryClient(conf)
    schema = Schema(load_avsc('basic_schema.avsc'), schema_type='AVRO')

    result = await sr.register_schema('test-key', schema)
    assert result == SCHEMA_ID


async def test_register_schema_incompatible(mock_schema_registry, load_avsc):
    conf = {'url': TEST_URL}
    sr = AsyncSchemaRegistryClient(conf)
    schema = Schema(load_avsc('basic_schema.avsc'), schema_type='AVRO')

    with pytest.raises(SchemaRegistryError, match="Incompatible Schema") as e:
        await sr.register_schema('conflict', schema)

    assert e.value.http_status_code == 409
    assert e.value.error_code == -1


async def test_register_schema_invalid(mock_schema_registry, load_avsc):
    conf = {'url': TEST_URL}
    sr = AsyncSchemaRegistryClient(conf)
    schema = Schema(load_avsc('invalid_schema.avsc'), schema_type='AVRO')

    with pytest.raises(SchemaRegistryError, match="Invalid Schema") as e:
        await sr.register_schema('invalid', schema)

    assert e.value.http_status_code == 422
    assert e.value.error_code == 42201


async def test_register_schema_cache(mock_schema_registry, load_avsc):
    conf = {'url': TEST_URL}
    sr = AsyncSchemaRegistryClient(conf)
    schema = load_avsc('basic_schema.avsc')

    count_before = COUNTER['POST'].get(
        '/subjects/test-cache/versions', 0)

    # Caching only starts after the first response is handled.
    # A possible improvement would be to add request caching to the http client
    # to catch in-flight requests as well.
    await sr.register_schema('test-cache', Schema(schema, 'AVRO'))

    def register():
        return asyncio.run(sr.register_schema('test-cache', schema))

    fs = []
    with ThreadPoolExecutor(max_workers=10) as executor:
        for _ in range(0, 1000):
            fs.append(executor.submit(register))
    wait(fs)

    count_after = COUNTER['POST'].get(
        '/subjects/test-cache/versions')

    assert count_after - count_before == 1


async def test_get_schema(mock_schema_registry, load_avsc):
    conf = {'url': TEST_URL}
    sr = AsyncSchemaRegistryClient(conf)

    expected = Schema(load_avsc(SCHEMA), schema_type='AVRO')
    actual = await sr.get_schema(47)

    assert cmp_schema(expected, actual)


async def test_get_schema_not_found(mock_schema_registry):
    conf = {'url': TEST_URL}
    sr = AsyncSchemaRegistryClient(conf)

    with pytest.raises(SchemaRegistryError, match="Schema not found") as e:
        await sr.get_schema(404)
    assert e.value.http_status_code == 404
    assert e.value.error_code == 40403


async def test_get_schema_cache(mock_schema_registry):
    conf = {'url': TEST_URL}
    sr = AsyncSchemaRegistryClient(conf)

    count_before = COUNTER['GET'].get(
        '/schemas/ids/47', 0)

    # Caching only starts after the first response is handled.
    # A possible improvement would be to add request caching to the http client
    # to catch in-flight requests as well.
    await sr.get_schema(47)

    def get():
        return asyncio.run(sr.get_schema(47))

    fs = []
    with ThreadPoolExecutor(max_workers=10) as executor:
        for _ in range(0, 1000):
            fs.append(executor.submit(get))
    wait(fs)

    count_after = COUNTER['GET'].get(
        '/schemas/ids/47')

    assert count_after - count_before == 1


async def test_get_schema_string_success(mock_schema_registry, load_avsc):
    conf = {'url': TEST_URL}
    sr = AsyncSchemaRegistryClient(conf)

    expected = json.loads(load_avsc(SCHEMA))
    actual = await sr.get_schema_string(47)
    assert expected == actual


async def test_get_schema_types(mock_schema_registry):
    conf = {'url': TEST_URL}
    sr = AsyncSchemaRegistryClient(conf)

    expected = ['AVRO', 'JSON', 'PROTOBUF']
    actual = await sr.get_schema_types()
    assert expected == actual


async def test_get_subjects_by_schema_id(mock_schema_registry):
    conf = {'url': TEST_URL}
    sr = AsyncSchemaRegistryClient(conf)

    expected = SUBJECTS
    actual = await sr.get_subjects_by_schema_id(47)
    assert expected == actual


async def test_get_schema_versions(mock_schema_registry):
    conf = {'url': TEST_URL}
    sr = AsyncSchemaRegistryClient(conf)

    expected = [SchemaVersion(subject='subject1', version=1), SchemaVersion(subject='subject2', version=2)]
    actual = await sr.get_schema_versions(47)
    assert expected == actual


async def test_get_registration(mock_schema_registry, load_avsc):
    conf = {'url': TEST_URL}
    sr = AsyncSchemaRegistryClient(conf)

    subject = 'get_registration'
    schema = Schema(load_avsc(SCHEMA), schema_type='AVRO')

    response = await sr.lookup_schema(subject, schema)

    assert response.subject == subject
    assert response.version == VERSION
    assert response.schema_id == SCHEMA_ID
    assert cmp_schema(response.schema, schema)


async def test_get_registration_subject_not_found(mock_schema_registry, load_avsc):
    conf = {'url': TEST_URL}
    sr = AsyncSchemaRegistryClient(conf)

    subject = 'notfound'
    schema = Schema(load_avsc(SCHEMA), schema_type='AVRO')

    with pytest.raises(SchemaRegistryError, match="Subject not found") as e:
        await sr.lookup_schema(subject, schema)
    assert e.value.http_status_code == 404
    assert e.value.error_code == 40401


async def test_get_registration_schema_not_found(mock_schema_registry, load_avsc):
    conf = {'url': TEST_URL}
    sr = AsyncSchemaRegistryClient(conf)

    subject = 'schemanotfound'
    schema = Schema(load_avsc(SCHEMA), schema_type='AVRO')

    with pytest.raises(SchemaRegistryError, match="Schema not found") as e:
        await sr.lookup_schema(subject, schema)
    assert e.value.http_status_code == 404
    assert e.value.error_code == 40403


async def test_get_subjects(mock_schema_registry):
    conf = {'url': TEST_URL}
    sr = AsyncSchemaRegistryClient(conf)

    result = await sr.get_subjects()

    assert result == SUBJECTS


async def test_delete(mock_schema_registry):
    conf = {'url': TEST_URL}
    sr = AsyncSchemaRegistryClient(conf)

    result = await sr.delete_subject("delete_subject")
    assert result == VERSIONS


async def test_delete_subject_not_found(mock_schema_registry):
    conf = {'url': TEST_URL}
    sr = AsyncSchemaRegistryClient(conf)

    with pytest.raises(SchemaRegistryError, match="Subject not found") as e:
        await sr.delete_subject("notfound")
    assert e.value.http_status_code == 404
    assert e.value.error_code == 40401


async def test_get_version(mock_schema_registry, load_avsc):
    conf = {'url': TEST_URL}
    sr = AsyncSchemaRegistryClient(conf)

    subject = "get_version"
    version = 3
    schema = Schema(load_avsc(SCHEMA), schema_type='AVRO')

    result = await sr.get_version(subject, version)
    assert result.subject == subject
    assert result.version == version
    assert cmp_schema(result.schema, schema)
    assert result.schema_id == SCHEMA_ID


async def test_get_version_no_version(mock_schema_registry):
    conf = {'url': TEST_URL}
    sr = AsyncSchemaRegistryClient(conf)

    subject = "get_version"
    version = 404

    with pytest.raises(SchemaRegistryError, match="Version not found") as e:
        await sr.get_version(subject, version)
    assert e.value.http_status_code == 404
    assert e.value.error_code == 40402


async def test_get_version_invalid(mock_schema_registry):
    conf = {'url': TEST_URL}
    sr = AsyncSchemaRegistryClient(conf)

    subject = "get_version"
    version = 422

    with pytest.raises(SchemaRegistryError, match="Invalid version") as e:
        await sr.get_version(subject, version)
    assert e.value.http_status_code == 422
    assert e.value.error_code == 42202


async def test_get_version_subject_not_found(mock_schema_registry):
    conf = {'url': TEST_URL}
    sr = AsyncSchemaRegistryClient(conf)

    subject = "notfound"
    version = 3

    with pytest.raises(SchemaRegistryError, match="Subject not found") as e:
        await sr.get_version(subject, version)
    assert e.value.http_status_code == 404
    assert e.value.error_code == 40401


async def test_delete_version(mock_schema_registry):
    conf = {'url': TEST_URL}
    sr = AsyncSchemaRegistryClient(conf)

    result = await sr.delete_version("delete_version", 3)

    assert result == 3


async def test_delete_version_not_found(mock_schema_registry):
    conf = {'url': TEST_URL}
    sr = AsyncSchemaRegistryClient(conf)

    with pytest.raises(SchemaRegistryError, match="Version not found") as e:
        await sr.delete_version("delete_version", 404)
    assert e.value.http_status_code == 404
    assert e.value.error_code == 40402


async def test_delete_version_subject_not_found(mock_schema_registry):
    conf = {'url': TEST_URL}
    sr = AsyncSchemaRegistryClient(conf)

    with pytest.raises(SchemaRegistryError, match="Subject not found") as e:
        await sr.delete_version("notfound", 3)
    assert e.value.http_status_code == 404
    assert e.value.error_code == 40401


async def test_delete_version_invalid(mock_schema_registry):
    conf = {'url': TEST_URL}
    sr = AsyncSchemaRegistryClient(conf)

    with pytest.raises(SchemaRegistryError, match="Invalid version") as e:
        await sr.delete_version("invalid_version", 422)
    assert e.value.http_status_code == 422
    assert e.value.error_code == 42202


async def test_get_version_schema_string(mock_schema_registry, load_avsc):
    conf = {'url': TEST_URL}
    sr = AsyncSchemaRegistryClient(conf)

    expected = json.loads(load_avsc(SCHEMA))
    actual = await sr.get_version_schema_string("get_version", 3)
    assert expected == actual


async def test_get_referenced_by(mock_schema_registry):
    conf = {'url': TEST_URL}
    sr = AsyncSchemaRegistryClient(conf)

    assert await sr.get_referenced_by("get_version", 3) == [1, 2]


async def test_set_compatibility(mock_schema_registry):
    conf = {'url': TEST_URL}
    sr = AsyncSchemaRegistryClient(conf)

    result = await sr.set_compatibility(level="FULL")
    assert result == {'compatibility': 'FULL'}


async def test_set_compatibility_invalid(mock_schema_registry):
    conf = {'url': TEST_URL}
    sr = AsyncSchemaRegistryClient(conf)
    with pytest.raises(SchemaRegistryError, match="Invalid compatibility level") as e:
        await sr.set_compatibility(level="INVALID")
    e.value.http_status_code = 422
    e.value.error_code = 42203


async def test_get_compatibility_subject_not_found(mock_schema_registry):
    conf = {'url': TEST_URL}
    sr = AsyncSchemaRegistryClient(conf)

    with pytest.raises(SchemaRegistryError, match="Subject not found") as e:
        await sr.get_compatibility("notfound")
    assert e.value.http_status_code == 404
    assert e.value.error_code == 40401


async def test_schema_equivilence(load_avsc):
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
async def test_test_compatibility_no_error(
    mock_schema_registry, load_avsc, subject_name, version, expected_compatibility
):
    conf = {'url': TEST_URL}
    sr = AsyncSchemaRegistryClient(conf)
    schema = Schema(load_avsc('basic_schema.avsc'), schema_type='AVRO')

    is_compatible = await sr.test_compatibility(subject_name, schema)
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
async def test_test_compatibility_with_error(
    mock_schema_registry, load_avsc, subject_name, version, match_str, status_code, error_code
):
    conf = {'url': TEST_URL}
    sr = AsyncSchemaRegistryClient(conf)
    schema = Schema(load_avsc('basic_schema.avsc'), schema_type='AVRO')

    with pytest.raises(SchemaRegistryError, match=match_str) as e:
        await sr.test_compatibility(subject_name, schema, version)
    assert e.value.http_status_code == status_code
    assert e.value.error_code == error_code


async def test_test_compatibility_all_versions(mock_schema_registry, load_avsc):
    conf = {'url': TEST_URL}
    sr = AsyncSchemaRegistryClient(conf)
    schema = Schema(load_avsc('basic_schema.avsc'), schema_type='AVRO')

    is_compatible = await sr.test_compatibility_all_versions('subject-all-versions', schema)
    assert is_compatible is True


async def test_get_global_mode(mock_schema_registry):
    conf = {'url': TEST_URL}
    sr = AsyncSchemaRegistryClient(conf)

    mode = await sr.get_global_mode()
    assert mode == 'READWRITE'


async def test_set_global_mode(mock_schema_registry):
    conf = {'url': TEST_URL}
    sr = AsyncSchemaRegistryClient(conf)

    result = await sr.update_global_mode('READONLY')
    assert result == 'READONLY'
