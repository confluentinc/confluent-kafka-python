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
import os
import re
from collections import defaultdict

import pytest
import requests_mock

from confluent_kafka.schema_registry.schema_registry_client import \
    SchemaRegistryClient

work_dir = os.path.dirname(os.path.realpath(__file__))


@pytest.fixture()
def mock_schema_registry():
    return MockSchemaRegistryClient


class MockSchemaRegistryClient(SchemaRegistryClient):
    """
    Schema Registry mock.

    The MockSchemaRegistry client uses special uri paths to invoke specific
    behavior such as coercing an error. They are listed in the table below.
    The paths are formed using special keywords referred to as triggers.

    Triggers are used to inform the MockSchemaRegistry how to behave when
    receiving a request. For instance the `notfound` trigger word when placed
    in the subject field of the path will return a http status code of 404 and
    the appropriate Schema Registry Error(40401 Schema not found).

    Whenever the response includes content from the request body it will return
    the same data from the request.

    For example the following request will return 123:
        DELETE  /subjects/notfound/versions/123
        or
        SchemaRegistryClient.delete_version("delete_version", 123)

    All response items which can't be fulfilled with the contents of the request
    are populated with constants. Which may be referenced when validating the
    response.

        - SCHEMA_ID = 47
        - VERSION = 3
        - VERSIONS = [1, 2, 3, 4]
        - SCHEMA = 'basic_schema.avsc'
        - SUBJECTS = ['subject1', 'subject2'].

    Trigger keywords may also be used in the body of the requests. At this time
    the only endpoint which supports this is /config which will return an
    `Invalid compatibility level` error.


    Request paths to trigger exceptions:
    +--------+----------------------------------+-------+------------------------------+
    | Method |         Request Path             | Code  |      Description             |
    +========+==================================+=======+==============================+
    | GET    | /schemas/ids/404                 | 40403 | Schema not found             |
    +--------+----------------------------------+-------+------------------------------+
    | GET    | /subjects/notfound/versions      | 40401 | Subject not found            |
    +--------+----------------------------------+-------+------------------------------+
    | GET    | /subjects/notfound/versions/[0-9]| 40401 | Subject not found            |
    +--------+----------------------------------+-------+------------------------------+
    | GET    | /subjects/notfound/versions/404  | 40402 | Version not found            |
    +--------+----------------------------------+-------+------------------------------+
    | GET    | /subjects/notfound/versions/422  | 42202 | Invalid version              |
    +--------+----------------------------------+-------+------------------------------+
    | DELETE | /subjects/notfound               | 40401 | Subject not found            |
    +--------+----------------------------------+-------+------------------------------+
    | POST   | /subjects/conflict/versions      | 409*  | Incompatible Schema          |
    +--------+----------------------------------+-------+------------------------------+
    | POST   | /subjects/invalid/versions       | 42201 | Invalid Schema               |
    +--------+----------------------------------+-------+------------------------------+
    | POST   | /subjects/notfound               | 40401 | Subject not found            |
    +--------+----------------------------------+-------+------------------------------+
    | POST   | /subjects/schemanotfound         | 40403 | Schema not found             |
    +--------+----------------------------------+-------+------------------------------+
    | DELETE | /subjects/notfound               | 40401 | Subject not found            |
    +--------+----------------------------------+-------+------------------------------+
    | DELETE | /subjects/notfound/versions/[0-9]| 40401 | Subject not found            |
    +--------+----------------------------------+-------+------------------------------+
    | DELETE | /subjects/notfound/versions/404  | 40402 | Version not found            |
    +--------+----------------------------------+-------+------------------------------+
    | DELETE | /subjects/notfound/versions/422  | 42202 | Invalid version              |
    +--------+----------------------------------+-------+------------------------------+
    | GET    | /config/notconfig                | 40401 | Subject not found            |
    +--------+----------------------------------+-------+------------------------------+
    | PUT    | /config**                        | 42203 | Invalid compatibility level  |
    +--------+----------------------------------+-------+------------------------------+
    * POST /subjects/{}/versions does not follow the documented API error.
    ** PUT /config reacts to a trigger in the body: - {"compatibility": "FULL"}

    When evaluating special paths with overlapping trigger words the right most
    keyword will take precedence.

    i.e. Version not found will be returned for the following path.
        /subjects/notfound/versions/404

    The config endpoint has a special compatibility level "INVALID". This should
    be used to verify the handling of in valid compatibility settings.

    """
    # request paths
    schemas = re.compile("/schemas/ids/([0-9]*)$")
    subjects = re.compile("/subjects/?(.*)$")
    subject_versions = re.compile("/subjects/(.*)/versions/?(.*)$")
    compatibility = re.compile("/config/?(.*)$")

    # constants
    SCHEMA_ID = 47
    VERSION = 3
    VERSIONS = [1, 2, 3, 4]
    SCHEMA = 'basic_schema.avsc'
    SUBJECTS = ['subject1', 'subject2']

    # Counts requests handled per path by HTTP method
    # {HTTP method: { path : count}}
    counter = {'DELETE': defaultdict(int),
               'GET': defaultdict(int),
               'POST': defaultdict(int),
               'PUT': defaultdict(int)}

    def __init__(self, conf):
        super(MockSchemaRegistryClient, self).__init__(conf)

        adapter = requests_mock.Adapter()
        adapter.register_uri('GET', self.compatibility,
                             json=self.get_compatibility_callback)
        adapter.register_uri('PUT', self.compatibility,
                             json=self.put_compatibility_callback)

        adapter.register_uri('GET', self.schemas,
                             json=self.get_schemas_callback)

        adapter.register_uri('DELETE', self.subjects,
                             json=self.delete_subject_callback)
        adapter.register_uri('GET', self.subjects,
                             json=self.get_subject_callback)
        adapter.register_uri('POST', self.subjects,
                             json=self.post_subject_callback)

        adapter.register_uri('GET', self.subject_versions,
                             json=self.get_subject_version_callback)
        adapter.register_uri('DELETE', self.subject_versions,
                             json=self.delete_subject_version_callback)
        adapter.register_uri('POST', self.subject_versions,
                             json=self.post_subject_version_callback)

        self._rest_client.session.mount('http://', adapter)

    @staticmethod
    def _load_avsc(name):
        with open(os.path.join(work_dir, '..', 'integration', 'schema_registry',
                               'data', name)) as fd:
            return fd.read()

    def get_compatibility_callback(self, request, context):
        self.counter['GET'][request.path] += 1

        path_match = re.match(self.compatibility, request.path)
        subject = path_match.group(1)

        if subject == "notfound":
            context.status_code = 404
            return {'error_code': 40401,
                    'message': "Subject not found"}

        context.status_code = 200
        return {'compatibilityLevel': 'FULL'}

    def put_compatibility_callback(self, request, context):
        self.counter['PUT'][request.path] += 1

        level = request.json().get('compatibility')

        if level == "INVALID":
            context.status_code = 422
            return {'error_code': 42203,
                    'message': "Invalid compatibility level"}

        context.status_code = 200
        return request.json()

    def delete_subject_callback(self, request, context):
        self.counter['DELETE'][request.path] += 1

        path_match = re.match(self.subjects, request.path)
        subject = path_match.group(1)

        if subject == "notfound":
            context.status_code = 404
            return {'error_code': 40401,
                    'message': "Subject not found"}

        context.status_code = 200
        return self.VERSIONS

    def get_subject_callback(self, request, context):
        self.counter['GET'][request.path] += 1

        context.status_code = 200
        return self.SUBJECTS

    def post_subject_callback(self, request, context):
        self.counter['POST'][request.path] += 1

        path_match = re.match(self.subjects, request.path)
        subject = path_match.group(1)

        if subject == 'notfound':
            context.status_code = 404
            return {'error_code': 40401,
                    'message': "Subject not found"}
        if subject == 'schemanotfound':
            context.status_code = 404
            return {'error_code': 40403,
                    'message': "Schema not found"}

        context.status_code = 200
        return {'subject': subject,
                "id": self.SCHEMA_ID,
                "version": self.VERSION,
                "schema": request.json()['schema']}

    def get_schemas_callback(self, request, context):
        self.counter['GET'][request.path] += 1

        path_match = re.match(self.schemas, request.path)
        schema_id = path_match.group(1)

        if int(schema_id) == 404:
            context.status_code = 404
            return {'error_code': 40403,
                    'message': "Schema not found"}

        context.status_code = 200
        return {'schema': self._load_avsc(self.SCHEMA)}

    def get_subject_version_callback(self, request, context):
        self.counter['GET'][request.path] += 1

        path_match = re.match(self.subject_versions, request.path)
        subject = path_match.group(1)
        version = path_match.group(2)

        if int(version) == 404:
            context.status_code = 404
            return {'error_code': 40402,
                    'message': "Version not found"}
        if int(version) == 422:
            context.status_code = 422
            return {'error_code': 42202,
                    'message': "Invalid version"}
        if subject == 'notfound':
            context.status_code = 404
            return {'error_code': 40401,
                    'message': "Subject not found"}
        context.status_code = 200
        return {'subject': subject,
                'id': self.SCHEMA_ID,
                'version': int(version),
                'schema': self._load_avsc(self.SCHEMA)}

    def delete_subject_version_callback(self, request, context):
        self.counter['DELETE'][request.path] += 1

        path_match = re.match(self.subject_versions, request.path)
        subject = path_match.group(1)
        version = path_match.group(2)

        if int(version) == 404:
            context.status_code = 404
            return {"error_code": 40402,
                    "message": "Version not found"}

        if int(version) == 422:
            context.status_code = 422
            return {"error_code": 42202,
                    "message": "Invalid version"}

        if subject == "notfound":
            context.status_code = 404
            return {"error_code": 40401,
                    "message": "Subject not found"}

        context.status_code = 200
        return int(version)

    def post_subject_version_callback(self, request, context):
        self.counter['POST'][request.path] += 1

        path_match = re.match(self.subject_versions, request.path)
        subject = path_match.group(1)
        if subject == "conflict":
            context.status_code = 409
            # oddly the Schema Registry does not send a proper error for this.
            return "Incompatible Schema"

        if subject == "invalid":
            context.status_code = 422
            return {'error_code': 42201,
                    'message': "Invalid Schema"}
        else:
            context.status_code = 200
            return {'id': self.SCHEMA_ID}


@pytest.fixture("package")
def load_avsc():
    def get_handle(name):
        with open(os.path.join(work_dir, '..', 'integration', 'schema_registry',
                               'data', name)) as fd:
            return fd.read()

    return get_handle
