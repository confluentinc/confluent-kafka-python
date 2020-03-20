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
import logging
import sys
import urllib
from copy import deepcopy
from threading import Lock

from requests import Session, utils

from .schema import RegisteredSchema, Schema
from ..serialization.error import SchemaRegistryError

# TODO: use six or adopt compat file practicelike requests.
if sys.version < '3':
    def _urlencode(value):
        return urllib.quote(value, safe='')
else:
    def _urlencode(value):
        return urllib.parse.quote(value, safe='')

VALID_AUTH_PROVIDERS = ['URL', 'USER_INFO', 'SASL_INHERIT']

log = logging.getLogger(__name__)


class _RestClient(object):
    """
    Manages SchemaRegistryClient HTTP communication

    Args:
        src_conf (dict): Dictionary containing _RestClient configuration
    """
    default_headers = {'Accept': "application/vnd.schemaregistry.v1+json,"
                                 " application/vnd.schemaregistry+json,"
                                 " application/json"}

    def __init__(self, src_conf):
        self.session = Session()
        conf = self.handle_conf(src_conf)
        self.base_url = conf['url']

    def __del__(self):
        self.session.close()

    def handle_conf(self, src_conf):
        """
        Validates configuration

        Args:
            src_conf (dict): configuration dict
        """
        conf = deepcopy(src_conf)

        base_url = conf.get('url', '').rstrip('/')
        if not base_url.startswith('http'):
            raise ValueError("Invalid URL provided for Schema Registry")

        ca = conf.pop('ssl.ca.location', None)
        if ca is not None:
            self.session.verify = ca

        key = conf.pop('ssl.key.location', None)
        cert = conf.pop('ssl.certificate.location', None)
        if cert is not None and key is not None:
            self.session = (key, cert)

        if cert is not None:
            self.session = (key, cert)

        if key is not None and cert is None:
            raise ValueError("ssl.key.location may not be configured without"
                             'setting ssl.certificate.location')

        auth_provider = conf.pop('basic.auth.credentials.source', 'URL').upper()
        if auth_provider not in VALID_AUTH_PROVIDERS:
            raise ValueError("basic.auth.credentials.source must be one of"
                             .format(VALID_AUTH_PROVIDERS))

        if auth_provider == 'SASL_INHERIT':
            if conf.pop('sasl.mechanism', '').upper() == 'GSSAPI':
                raise ValueError("SASL_INHERIT does not support SASL mechanism GSSAPI")
            self.session.auth = (conf.pop('sasl.username', ''),
                                 conf.pop('sasl.password', ''))

        if auth_provider == 'USER_INFO':
            self.session.auth = tuple(conf.pop('basic.auth.user.info', '').split(':'))
        else:
            self.session.auth = utils.get_auth_from_url(base_url)

        return conf

    def close(self):
        self.session.close()

    def GET(self, url):
        return self.send_request(url, method='GET')

    def POST(self, url, body, **kwargs):

        return self.send_request(url, method='POST',
                                 body=body)

    def DELETE(self, url):
        return self.send_request(url, method='DELETE')

    def PUT(self, url, body=None):
        return self.send_request(url, method='PUT', body=body)

    def send_request(self, url, method, body=None):

        _headers = self.default_headers

        if body is not None:
            body = json.dumps(body, cls=self.JSONDuckEncoder)
            _headers = {'Content-Length': str(len(body)),
                        'Content-Type': "application/vnd.schemaregistry.v1+json"}

        response = self.session.request(
            method, url="/".join([self.base_url, url]),
            headers=_headers, data=body)

        try:
            if 200 <= response.status_code <= 299:
                return response.json()
            raise SchemaRegistryError(response.json().get('message', None),
                                      response.json().get('error_code', None))
        # Schema Registry may return HTML when it hits unexpected errors
        except ValueError as e:
            print(str(e))
            raise SchemaRegistryError(response.status_code,
                                      response.content)

    class JSONDuckEncoder(json.JSONEncoder):
        def default(self, obj):
            """
            Enables JSON encoding for any class with a to_json method.

            Args:
                - obj (object): object implementing ``to_json(self)``

            Returns:
                str: JSON encoded obj

            """
            return obj.to_json()


class _SchemaCache(object):

    def __init__(self):
        self.lock = Lock()

        self.schema_id_index = {}
        self.schema_index = {}

    def add_schema(self, schema_id, schema):
        """
        Add a Schema identified by schema_id to the cache.

        Args:
            schema_id (int): Schema's registration id
            schema (Schema): Schema instance

        Returns:
            int: The schema_id
        """
        # Don't overwrite existing keys
        schema = self.get_schema(schema_id)
        if schema is not None:
            return

        with self.lock:
            # Pessimistic
            if schema_id not in self.schema_id_index:
                self.schema_id_index[schema_id] = schema

    def get_schema(self, schema_id):
        """
        Get the schema instance associated  with schema_id from the cache.

        Args:
            schema_id (int): Id used to identify a schema
        Returns:
            Schema: The schema if known; else None

        """
        return self.schema_id_index.get(schema_id, None)

    def add_schema_id(self, schema, schema_id):
        """
        Add this Schema's id to the cache.

        Args:
            schema (Schema): Schema associated with schema_id
            schema_id (int): schema_id to cache
        """

        schema_id = self.get_schema_id(schema)
        if schema_id is not None:
            return

        with self.lock:
            # Pessimistic
            if schema not in self.schema_index:
                self.schema_index[schema] = schema_id

    def get_schema_id(self, schema):
        """
        Get the schema_id associated with this schema

        Args:
            schema (Schema): The schema associated with this schema_id

        Returns:
            int: Schema ID if known; else None

        """
        return self.schema_index.get(schema, None)


class SchemaRegistryClient(object):
    """
    A client that talks to a Schema Registry over HTTP

    See Also:
        http://confluent.io/docs/current/schema-registry/docs/intro.html

    Arguments:
        conf (SchemaRegistryConfig): Schema Registry config object
    """

    def __init__(self, conf):
        self._rest_client = _RestClient(conf)
        self.cache = _SchemaCache()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        if self._rest_client is not None:
            self._rest_client.close()

    def register_schema(self, subject_name, schema):
        """
        Registers a schema under ``subject_name``.
        Results are cached in the ``schema`` so subsequent requests will not
        require another trip the Schema Registry.

        Args:
            subject_name (str): subject to register a schema under
            schema (Schema): Schema (in JSON string format) to register.

        Returns:
            int: Registration id

        Raises:
            SchemaRegistryClientError: if Schema violates this subject's
            Compatibility policy or is otherwise invalid.

        .. _Schema Registry API Reference:
            https://docs.confluent.io/current/schema-registry/develop/api.html#post--subjects-(string-%20subject)-versions

        """  # noqa: E501
        schema_id = self.cache.get_schema_id(schema)
        if schema_id is not None:
            return schema_id

        response = self._rest_client.POST(
            'subjects/{}/versions'.format(_urlencode(subject_name)),
            body=schema)

        schema_id = response['id']
        self.cache.add_schema(schema_id, schema)

        return schema_id

    def get_schema(self, schema_id):
        """
        Fetches the schema associated with ``schema_id`` from the
        Schema Registry. The result is cached so subsequent attempts will not
        require an additional trip to the Schema Registry.

        Args:
            schema_id (int): Schema id

        Returns:
            Schema: Schema string identified by the ``schema_id``

        Raises:
            SchemaRegistryClientError: If schema can't be found

        .. _Schema Registry API Reference:
            https://docs.confluent.io/current/schema-registry/develop/api.html#get--'schemas/ids/{}?format=serialized'-ids-int-%20id

        """  # noqa: E501
        schema = self.cache.get_schema(schema_id)
        if schema is not None:
            return schema

        response = self._rest_client.GET('schemas/ids/{}'.format(schema_id))
        schema = Schema.from_json(response)

        self.cache.add_schema(schema_id, schema)

        return schema

    def get_registration(self, subject_name, schema):
        """
        Returns ``schema`` registration information for ``subject``.

        Args:
            subject_name (str): Subject name the schema is registered under
            schema (Schema): Registered Schema

        Returns:
            RegisteredSchema: Subject registration information for this schema.

        Raises:
            SchemaRegistryClientError: If schema or subject can't be found

        .. _Schema Registry API Reference:
            https://docs.confluent.io/current/schema-registry/develop/api.html#post--subjects-(string-%20subject)

        """  # noqa: E501
        response = self._rest_client.POST('subjects/' + _urlencode(subject_name),
                                          body=schema)
        return RegisteredSchema.from_json(response)

    def get_subjects(self):
        """
        List all subjects registered with the Schema Registry

        Returns:
            [str]: Registered subject names

        Raises:
            SchemaRegistryClientError: if subjects can't be found

        .. _Schema Registry API Reference:
            https://docs.confluent.io/current/schema-registry/develop/api.html#get--subjects

        """  # noqa: E501
        return self._rest_client.GET('subjects')

    def delete_subject(self, subject):
        """
        Deletes the specified subject and its associated compatibility level if
        registered. It is recommended to use this API only when a topic needs
        to be recycled or in development environments.

        Args:
            subject (str): subject name
        Returns:
            list(int): Versions deleted under this subject

        Raises:
            SchemaRegistryClientError: if the request was unsuccessful.

        .. _Schema Registry API Reference:
            https://docs.confluent.io/current/schema-registry/develop/api.html#delete--subjects-(string-%20subject)

        """  # noqa: E501
        return self._rest_client.DELETE('subjects/' + subject)

    def get_version(self, subject_name, version='latest'):
        """
        Retrieves a specific schema registered under ``subject_name``.

        .. _note:
            "latest” returns the last registered schema under the specified
            subject.

        Args:
            subject_name (str): Subject name.
            version (int, optional): version number. Defaults to "latest".

        Returns:
            RegisteredSchema: Registration information for this version.

        Raises:
            SchemaRegistryClientError if the version can't be found or
            is invalid.

        .. _Schema Registry API Reference:
            https://docs.confluent.io/current/schema-registry/develop/api.html#get--subjects-(string-%20subject)-versions-(versionId-%20version)

        """  # noqa: E501
        response = self._rest_client.GET('subjects/{}/versions/{}'
                                         .format(_urlencode(subject_name),
                                                 version))

        return RegisteredSchema.from_json(response)

    def list_versions(self, subject_name):
        """
        Args:
            subject_name (str): Subject name.

        Returns:
            [int]: Registered versions

        Raises:
            SchemaRegistryClientError If subject can't be found

        .. _Schema Registry API Reference:
            https://docs.confluent.io/current/schema-registry/develop/api.html#get--subjects-(string-%20subject)-versions

        """  # noqa: E501
        return self._rest_client.GET('subjects/{}/versions'.format(_urlencode(subject_name)))

    def delete_version(self, subject_name, version):
        """
        Deletes a specific version registered to ``subject_name``.

        Args:
            subject_name (str) Subject name
            version (int): Version number

        Returns
            int: Version number which was deleted

        Raises:
            SchemaRegistryClientError if the subject or version cannot be found.

        .. _Schema Registry API Reference:
            https://docs.confluent.io/current/schema-registry/develop/api.html#delete--subjects-(string-%20subject)-versions-(versionId-%20version)

        """  # noqa: E501
        return self._rest_client.DELETE('subjects/{}/versions/{}'.
                                        format(_urlencode(subject_name),
                                               version))

    def set_compatibility(self, subject_name=None, level=None):
        """
        Update global or subject level compatibility policy.

        Args:
            level (CompatibilityType): Compatibility policy
            subject_name (str, optional): Subject name.
                Sets global policy if not set.
            level (CompatibilityType): Compatibility policy

        Returns:
            str: The new level

        Raises:
            SchemaRegistryClientError: if the compatibility level is invalid

        .. _Schema Registry API Reference:
            https://docs.confluent.io/current/schema-registry/develop/api.html#put--config

        """  # noqa: E501
        if level is None:
            raise ValueError("level must be set")

        if subject_name is None:
            return self._rest_client.PUT('config', body={"compatibility": level})

        return self._rest_client.PUT('config/{}'
                                     .format(subject_name),
                                     {"compatibility": level})

    def get_compatibility(self, subject_name=None):
        """
        Get the current global Compatibility level.

        Args:
            - subject_name (str, optional): Subject name. Returns global policy
                if left unset.

        Returns:
            str: Compatibility level for the subject if set, otherwise the
                global compatibility level.

        Raises:
            SchemaRegistryClientError: if the request was unsuccessful.

        .. _Schema Registry API Reference:
            https://docs.confluent.io/current/schema-registry/develop/api.html#get--config

        """  # noqa: E501
        if subject_name is not None:
            url = 'config/{}'.format(subject_name)
        else:
            url = 'config'

        return self._rest_client.GET(url)


class CompatibilityType(object):
    """
    Enum-like object defining compatibility type.

    .. _Schema Registry Compatibility Types:
        https://docs.confluent.io/current/schema-registry/avro.html#compatibility-types

    """  # noqa: E501
    NONE = 'NONE'
    BACKWARD = 'BACKWARD'
    BACKWARD_TRANSITIVE = 'BACKWARD_TRANSITIVE'
    FORWARD = 'FORWARD'
    FORWARD_TRANSITIVE = 'FORWARD_TRANSITIVE'
    FULL = 'FULL'
    FULL_TRANSITIVE = 'FULL_TRANSITIVE'
