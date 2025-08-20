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
import os
import re
from base64 import b64decode
from collections import defaultdict

import pytest
import respx
from httpx import Response

work_dir = os.path.dirname(os.path.realpath(__file__))


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

To coerce Authentication errors configure credentials to
not match MockSchemaRegistryClient.USERINFO.

Request paths to trigger exceptions:
+--------+-------------------------------------------------+-------+------------------------------+
| Method |         Request Path                            | Code  |      Description             |
+========+=================================================+=======+==============================+
| GET    | /schemas/ids/404                                | 40403 | Schema not found             |
+--------+-------------------------------------------------+-------+------------------------------+
| GET    | /subjects/notfound/versions                     | 40401 | Subject not found            |
+--------+-------------------------------------------------+-------+------------------------------+
| GET    | /subjects/notfound/versions/[0-9]               | 40401 | Subject not found            |
+--------+-------------------------------------------------+-------+------------------------------+
| GET    | /subjects/notfound/versions/404                 | 40402 | Version not found            |
+--------+-------------------------------------------------+-------+------------------------------+
| GET    | /subjects/notfound/versions/422                 | 42202 | Invalid version              |
+--------+-------------------------------------------------+-------+------------------------------+
| DELETE | /subjects/notfound                              | 40401 | Subject not found            |
+--------+-------------------------------------------------+-------+------------------------------+
| POST   | /subjects/conflict/versions                     | 409*  | Incompatible Schema          |
+--------+-------------------------------------------------+-------+------------------------------+
| POST   | /subjects/invalid/versions                      | 42201 | Invalid Schema               |
+--------+-------------------------------------------------+-------+------------------------------+
| POST   | /subjects/notfound                              | 40401 | Subject not found            |
+--------+-------------------------------------------------+-------+------------------------------+
| POST   | /subjects/schemanotfound                        | 40403 | Schema not found             |
+--------+-------------------------------------------------+-------+------------------------------+
| DELETE | /subjects/notfound                              | 40401 | Subject not found            |
+--------+-------------------------------------------------+-------+------------------------------+
| DELETE | /subjects/notfound/versions/[0-9]               | 40401 | Subject not found            |
+--------+-------------------------------------------------+-------+------------------------------+
| DELETE | /subjects/notfound/versions/404                 | 40402 | Version not found            |
+--------+-------------------------------------------------+-------+------------------------------+
| DELETE | /subjects/notfound/versions/422                 | 42202 | Invalid version              |
+--------+-------------------------------------------------+-------+------------------------------+
| GET    | /config/notfound                                | 40401 | Subject not found            |
+--------+-------------------------------------------------+-------+------------------------------+
| PUT    | /config**                                       | 42203 | Invalid compatibility level  |
+--------+-------------------------------------------------+-------+------------------------------+
| DELETE | /config/notfound                                | 40401 | Subject not found            |
+--------+-------------------------------------------------+-------+------------------------------+
| POST   | /compatibility/subjects/notfound/versions/[0-9] | 40401 | Subject not found            |
+--------+-------------------------------------------------+-------+------------------------------+
| POST   | /compatibility/subjects/invalid/versions/[0-9]  | 42201 | Invalid Schema               |
+--------+-------------------------------------------------+-------+------------------------------+
| POST   | /compatibility/subjects/notfound/versions/404   | 40402 | Version not found            |
+--------+-------------------------------------------------+-------+------------------------------+
| POST   | /compatibility/subjects/invalid/versions/bad    | 42202 | Invalid version              |
+--------+-------------------------------------------------+-------+------------------------------+
| PUT    | /mode/invalid_mode                              | 42204 | Invalid mode                 |
+--------+-------------------------------------------------+-------+------------------------------+
| PUT    | /mode/operation_not_permitted                   | 42205 | Operation not permitted      |
+--------+-------------------------------------------------+-------+------------------------------+
* POST /subjects/{}/versions does not follow the documented API error.
** PUT /config reacts to a trigger in the body: - {"compatibility": "FULL"}

When evaluating special paths with overlapping trigger words the right most
keyword will take precedence.

i.e. Version not found will be returned for the following path.
    /subjects/notfound/versions/404

The config endpoint has a special compatibility level "INVALID". This should
be used to verify the handling of in valid compatibility settings.

"""


@pytest.fixture()
def mock_schema_registry():
    with (respx.mock as respx_mock):
        respx_mock.route().mock(side_effect=_auth_matcher)

        respx_mock.post(COMPATIBILITY_SUBJECTS_VERSIONS_RE).mock(
            side_effect=post_compatibility_subjects_versions_callback)
        respx_mock.post(COMPATIBILITY_SUBJECTS_ALL_VERSIONS_RE).mock(
            side_effect=post_compatibility_subjects_all_versions_callback)

        respx_mock.get(CONFIG_RE).mock(side_effect=get_config_callback)
        respx_mock.put(CONFIG_RE).mock(side_effect=put_config_callback)
        respx_mock.delete(CONFIG_RE).mock(side_effect=delete_config_callback)

        respx_mock.get(CONTEXTS_RE).mock(side_effect=get_contexts_callback)

        respx_mock.get(MODE_GLOBAL_RE).mock(side_effect=get_global_mode_callback)
        respx_mock.put(MODE_GLOBAL_RE).mock(side_effect=put_global_mode_callback)
        respx_mock.get(MODE_RE).mock(side_effect=get_mode_callback)
        respx_mock.put(MODE_RE).mock(side_effect=put_mode_callback)
        respx_mock.delete(MODE_RE).mock(side_effect=delete_mode_callback)

        respx_mock.get(SCHEMAS_RE).mock(side_effect=get_schemas_callback)
        respx_mock.get(SCHEMAS_VERSIONS_RE).mock(side_effect=get_schema_versions_callback)
        respx_mock.get(SCHEMAS_SUBJECTS_RE).mock(side_effect=get_schema_subjects_callback)
        respx_mock.get(SCHEMAS_TYPES_RE).mock(side_effect=get_schema_types_callback)

        respx_mock.get(SUBJECTS_VERSIONS_REFERENCED_BY_RE).mock(side_effect=get_subject_version_referenced_by_callback)
        respx_mock.get(SUBJECTS_VERSIONS_RE).mock(side_effect=get_subject_version_callback)
        respx_mock.delete(SUBJECTS_VERSIONS_RE).mock(side_effect=delete_subject_version_callback)
        respx_mock.post(SUBJECTS_VERSIONS_RE).mock(side_effect=post_subject_version_callback)

        respx_mock.delete(SUBJECTS_RE).mock(side_effect=delete_subject_callback)
        respx_mock.get(SUBJECTS_RE).mock(side_effect=get_subject_callback)
        respx_mock.post(SUBJECTS_RE).mock(side_effect=post_subject_callback)

        yield respx_mock


# request paths
SCHEMAS_RE = re.compile("/schemas/ids/([0-9]*)$")
SCHEMAS_VERSIONS_RE = re.compile(r"/schemas/ids/([0-9]*)/versions(\?.*)?$")
SCHEMAS_SUBJECTS_RE = re.compile(r"/schemas/ids/([0-9]*)/subjects(\?.*)?$")
SCHEMAS_TYPES_RE = re.compile("/schemas/types$")

SUBJECTS_RE = re.compile("/subjects/?(.*)$")
SUBJECTS_VERSIONS_RE = re.compile("/subjects/(.*)/versions/?(.*)$")
SUBJECTS_VERSIONS_SCHEMA_RE = re.compile(r"/subjects/(.*)/versions/(.*)/schema(\?.*)?$")
SUBJECTS_VERSIONS_REFERENCED_BY_RE = re.compile(r"/subjects/(.*)/versions/(.*)/referencedby(\?.*)?$")

CONFIG_RE = re.compile("/config/?(.*)$")

COMPATIBILITY_SUBJECTS_VERSIONS_RE = re.compile("/compatibility/subjects/(.*)/versions/?(.*)$")
COMPATIBILITY_SUBJECTS_ALL_VERSIONS_RE = re.compile("/compatibility/subjects/(.*)/versions")

MODE_GLOBAL_RE = re.compile(r"/mode(\?.*)?$")
MODE_RE = re.compile("/mode/(.*)$")

CONTEXTS_RE = re.compile(r"/contexts(\?.*)?$")

# constants
SCHEMA_ID = 47
VERSION = 3
VERSIONS = [1, 2, 3, 4]
SCHEMA = 'basic_schema.avsc'
SUBJECTS = ['subject1', 'subject2']
USERINFO = 'mock_user:mock_password'

# Counts requests handled per path by HTTP method
# {HTTP method: { path : count}}
COUNTER = {'DELETE': defaultdict(int),
           'GET': defaultdict(int),
           'POST': defaultdict(int),
           'PUT': defaultdict(int)}


def _auth_matcher(request):
    headers = request.headers

    authinfo = headers.get('Authorization', None)
    # Pass request to downstream matchers
    if authinfo is None:
        return None

    # We only support the BASIC scheme today
    scheme, userinfo = authinfo.split(" ")
    if b64decode(userinfo).decode('utf-8') == USERINFO:
        return None

    unauthorized = {'error_code': 401,
                    'message': "401 Unauthorized"}
    return Response(401, json=unauthorized)


def _load_avsc(name) -> str:
    with open(os.path.join(work_dir, '..', 'integration', 'schema_registry',
                           'data', name)) as fd:
        return fd.read()


def get_config_callback(request, route):
    COUNTER['GET'][request.url.path] += 1

    path_match = re.match(CONFIG_RE, request.url.path)
    subject = path_match.group(1)

    if subject == "notfound":
        return Response(404, json={'error_code': 40401,
                                   'message': "Subject not found"})

    return Response(200, json={'compatibility': 'FULL'})


def put_config_callback(request, route):
    COUNTER['PUT'][request.url.path] += 1

    body = json.loads(request.content.decode('utf-8'))
    level = body.get('compatibility')

    if level == "INVALID":
        return Response(422, json={'error_code': 42203,
                                   'message': "Invalid compatibility level"})

    return Response(200, json=body)


def delete_config_callback(request, route):
    COUNTER['DELETE'][request.url.path] += 1

    path_match = re.match(CONFIG_RE, request.url.path)
    subject = path_match.group(1)

    if subject == "notfound":
        return Response(404, json={'error_code': 40401,
                                   'message': "Subject not found"})

    return Response(200, json={'compatibility': 'FULL'})


def get_contexts_callback(request, route):
    COUNTER['GET'][request.url.path] += 1
    return Response(200, json=['context1', 'context2'])


def delete_subject_callback(request, route):
    COUNTER['DELETE'][request.url.path] += 1

    path_match = re.match(SUBJECTS_RE, request.url.path)
    subject = path_match.group(1)

    if subject == "notfound":
        return Response(404, json={'error_code': 40401,
                                   'message': "Subject not found"})

    return Response(200, json=VERSIONS)


def get_subject_callback(request, route):
    COUNTER['GET'][request.url.path] += 1

    return Response(200, json=SUBJECTS)


def post_subject_callback(request, route):
    COUNTER['POST'][request.url.path] += 1

    path_match = re.match(SUBJECTS_RE, request.url.path)
    subject = path_match.group(1)

    if subject == 'notfound':
        return Response(404, json={'error_code': 40401,
                                   'message': "Subject not found"})
    if subject == 'schemanotfound':
        return Response(404, json={'error_code': 40403,
                                   'message': "Schema not found"})

    body = json.loads(request.content.decode('utf-8'))
    return Response(200, json={'subject': subject,
                               "id": SCHEMA_ID,
                               "version": VERSION,
                               "schema": body['schema']})


def get_schemas_callback(request, route):
    COUNTER['GET'][request.url.path] += 1

    path_match = re.match(SCHEMAS_RE, request.url.path)
    schema_id = path_match.group(1)

    if int(schema_id) == 404:
        return Response(404, json={'error_code': 40403,
                                   'message': "Schema not found"})

    return Response(200, json={'schema': _load_avsc(SCHEMA)})


def get_schema_subjects_callback(request, route):
    COUNTER['GET'][request.url.path] += 1
    return Response(200, json=SUBJECTS)


def get_schema_string_callback(request, route):
    COUNTER['GET'][request.url.path] += 1
    path_match = re.match(SCHEMAS_STRING_RE, request.url.path)
    schema_id = path_match.group(1)
    if int(schema_id) == 404:
        return Response(404, json={'error_code': 40403,
                                   'message': "Schema not found"})
    return Response(200, json=json.loads(_load_avsc(SCHEMA)))


def get_schema_types_callback(request, route):
    COUNTER['GET'][request.url.path] += 1
    return Response(200, json=['AVRO', 'JSON', 'PROTOBUF'])


def get_schema_versions_callback(request, route):
    COUNTER['GET'][request.url.path] += 1
    return Response(200, json=[
        {'subject': 'subject1', 'version': 1},
        {'subject': 'subject2', 'version': 2}
    ])

def get_schema_subjects_callback(request, route):
    COUNTER['GET'][request.url.path] += 1
    return Response(200, json=SUBJECTS)


def get_schema_types_callback(request, route):
    COUNTER['GET'][request.url.path] += 1
    return Response(200, json=['AVRO', 'JSON', 'PROTOBUF'])


def get_schema_versions_callback(request, route):
    COUNTER['GET'][request.url.path] += 1
    return Response(200, json=[
        {'subject': 'subject1', 'version': 1},
        {'subject': 'subject2', 'version': 2}
    ])


def get_subject_version_callback(request, route):
    COUNTER['GET'][request.url.path] += 1

    path_match = re.match(SUBJECTS_VERSIONS_RE, request.url.path)
    subject = path_match.group(1)
    version = path_match.group(2)
    version_num = -1 if version == 'latest' else int(version)

    if version_num == 404:
        return Response(404, json={'error_code': 40402,
                                   'message': "Version not found"})
    if version_num == 422:
        return Response(422, json={'error_code': 42202,
                                   'message': "Invalid version"})
    if subject == 'notfound':
        return Response(404, json={'error_code': 40401,
                                   'message': "Subject not found"})
    return Response(200, json={'subject': subject,
                               'id': SCHEMA_ID,
                               'version': version_num,
                               'schema': _load_avsc(SCHEMA)})


def delete_subject_version_callback(request, route):
    COUNTER['DELETE'][request.url.path] += 1

    path_match = re.match(SUBJECTS_VERSIONS_RE, request.url.path)
    subject = path_match.group(1)
    version = path_match.group(2)
    version_num = -1 if version == 'latest' else int(version)

    if version_num == 404:
        return Response(404, json={'error_code': 40402,
                                   'message': "Version not found"})

    if version_num == 422:
        return Response(422, json={'error_code': 42202,
                                   'message': "Invalid version"})

    if subject == "notfound":
        return Response(404, json={'error_code': 40401,
                                   'message': "Subject not found"})

    return Response(200, json=version_num)


def post_subject_version_callback(request, route):
    COUNTER['POST'][request.url.path] += 1

    path_match = re.match(SUBJECTS_VERSIONS_RE, request.url.path)
    subject = path_match.group(1)
    if subject == "conflict":
        # oddly the Schema Registry does not send a proper error for this.
        return Response(409, json={'error_code': -1,
                                   'message': "Incompatible Schema"})

    if subject == "invalid":
        return Response(422, json={'error_code': 42201,
                                   'message': "Invalid Schema"})
    else:
        return Response(200, json={'id': SCHEMA_ID})


def get_subject_version_schema_callback(request, route):
    COUNTER['GET'][request.url.path] += 1
    return Response(200, json=json.loads(_load_avsc(SCHEMA)))


def get_subject_version_referenced_by_callback(request, route):
    COUNTER['GET'][request.url.path] += 1
    return Response(200, json=[1, 2])

def get_subject_version_schema_callback(request, route):
    COUNTER['GET'][request.url.path] += 1
    return Response(200, json=json.loads(_load_avsc(SCHEMA)))


def get_subject_version_referenced_by_callback(request, route):
    COUNTER['GET'][request.url.path] += 1
    return Response(200, json=[1, 2])


def post_compatibility_subjects_versions_callback(request, route):
    COUNTER['POST'][request.url.path] += 1

    path_match = re.match(COMPATIBILITY_SUBJECTS_VERSIONS_RE, request.url.path)
    subject = path_match.group(1)
    version = path_match.group(2)

    if version == '422':
        return Response(422, json={'error_code': 42202,
                                   'message': 'Invalid version'})

    if version == '404':
        return Response(404, json={'error_code': 40402,
                                   'message': 'Version not found'})

    if subject == 'conflict':
        return Response(200, json={'is_compatible': False})

    if subject == 'notfound':
        return Response(404, json={'error_code': 40401,
                                   'message': 'Subject not found'})

    if subject == 'invalid':
        return Response(422, json={'error_code': 42201,
                                   'message': 'Invalid Schema'})

    return Response(200, json={'is_compatible': True})


def post_compatibility_subjects_all_versions_callback(request, route):
    COUNTER['POST'][request.url.path] += 1
    return Response(200, json={'is_compatible': True})


def get_global_mode_callback(request, route):
    COUNTER['GET'][request.url.path] += 1
    return Response(200, json={'mode': 'READWRITE'})


def put_global_mode_callback(request, route):
    COUNTER['PUT'][request.url.path] += 1
    body = json.loads(request.content.decode('utf-8'))
    print(body)
    return Response(200, json=body)


def get_mode_callback(request, route):
    COUNTER['GET'][request.url.path] += 1
    return Response(200, json={'mode': 'READWRITE'})


def put_mode_callback(request, route):
    COUNTER['PUT'][request.url.path] += 1

    path_match = re.match(MODE_RE, request.url.path)
    subject = path_match.group(1)
    body = json.loads(request.content.decode('utf-8'))
    mode = body.get('mode')

    if subject == 'invalid_mode':
        return Response(422, json={'error_code': 42204,
                                   'message': "Invalid mode"})
    if subject == 'operation_not_permitted':
        return Response(422, json={'error_code': 42205,
                                   'message': "Operation not permitted"})
    return Response(200, json={'mode': mode})


def delete_mode_callback(request, route):
    COUNTER['DELETE'][request.url.path] += 1
    return Response(200, json={'mode': 'READWRITE'})


@pytest.fixture(scope="package")
def load_avsc():
    def get_handle(name):
        with open(os.path.join(work_dir, '..', 'integration', 'schema_registry',
                               'data', name)) as fd:
            return fd.read()

    return get_handle
