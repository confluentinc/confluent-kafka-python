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
import urllib
from copy import deepcopy
from threading import Lock

from requests import Session, utils

from .error import SchemaRegistryError
from .schema import Schema, RegisteredSchema, SchemaReference

# TODO: use six or adopt compat file requests.
try:
    string_type = basestring  # noqa

    def _urlencode(value):
        return urllib.quote(value, safe='')
except NameError:
    string_type = str

    def _urlencode(value):
        return urllib.parse.quote(value, safe='')

log = logging.getLogger(__name__)

_CONF_PREFIX = 'schema.registry.'
VALID_AUTH_PROVIDERS = ['URL', 'USER_INFO', 'SASL_INHERIT']


class _RestClient(object):
    """
    HTTP client for Confluent Schema Registry.

    Args:
        conf (dict): Dictionary containing _RestClient configuration

    """
    default_headers = {'Accept': "application/vnd.schemaregistry.v1+json,"
                                 " application/vnd.schemaregistry+json,"
                                 " application/json"}

    def __init__(self, conf):
        self.session = Session()

        # copy dict to avoid mutating the original
        conf_copy = deepcopy(conf)

        base_url = conf_copy.pop('url', None)
        if not isinstance(base_url, basestring):
            raise ValueError("url must be an instance of str not "
                             + str(type(base_url)))
        if not base_url.startswith('http'):
            raise ValueError("Invalid url {}".format(base_url))
        self.base_url = base_url.rstrip('/')

        # The following configs map Requests Session class properties.
        # See the API docs for specifics.
        # https://requests.readthedocs.io/en/master/api/#request-sessions
        ca = conf_copy.pop('ssl.ca.location', None)
        if ca is not None:
            self.session.verify = ca

        key = conf_copy.pop('ssl.key.location', None)
        cert = conf_copy.pop('ssl.certificate.location', None)

        # If Tuple, (‘cert’, ‘key’) pair.
        if cert is not None and key is not None:
            self.session.cert = (cert, key)

        # if String, path to ssl client cert file (.pem)
        if cert is not None and key is None:
            self.session.cert = cert

        if key is not None and cert is None:
            raise ValueError("ssl.certificate.location required when"
                             " configuring ssl.key.location")

        auth_provider = conf_copy.pop('basic.auth.credentials.source',
                                      'URL').upper()
        userinfo = utils.get_auth_from_url(base_url)
        if auth_provider not in VALID_AUTH_PROVIDERS:
            raise ValueError("basic.auth.credentials.source must be one of"
                             " {} not {}".format(VALID_AUTH_PROVIDERS,
                                                 auth_provider))

        if auth_provider == 'SASL_INHERIT':
            if conf_copy.pop('sasl.mechanism', '').upper() == 'GSSAPI':
                raise ValueError("sasl.mechanism(s) GSSAPI is not supported by"
                                 " basic.auth.credentials.source SASL_INHERIT")
            if userinfo != ('', ''):
                raise ValueError("basic.auth.credentials.source configured for"
                                 " SASL_INHERIT with credentials in the URL."
                                 " Remove userinfo from the url or or configure"
                                 " basic.auth.credentials.source to"
                                 " URL(default)")

            userinfo = (conf_copy.pop('sasl.username', ''),
                        conf_copy.pop('sasl.password', ''))
            # Ensure username is set
            if userinfo[0] == '':
                raise ValueError("sasl.username required when"
                                 " basic.auth.credentials.source configured to"
                                 " SASL_INHERIT")

        if auth_provider == 'USER_INFO':
            if userinfo != ('', ''):
                raise ValueError("basic.auth.credentials.source configured for"
                                 " USER_INFO with credentials in the URL."
                                 " Remove userinfo from the url or or configure"
                                 " basic.auth.credentials.source to"
                                 " URL(default)")

            userinfo = conf_copy.pop('basic.auth.user.info', '').split(':')
            if len(userinfo) != 2:
                raise ValueError("basic.auth.user.info must be in the form"
                                 " of {username}:{password}")

        # Auth handler or (user, pass) tuple. Defaults to Basic handler
        self.session.auth = userinfo

        # Any leftover keys are unknown to _RestClient
        if len(conf_copy) > 0:
            raise ValueError("Unrecognized property(ies) {}".format(conf_copy.keys()))

    @property
    def url(self):
        return self.base_url

    @property
    def _auth(self):
        return tuple(self.session.auth)

    @property
    def _certificate(self):
        return self.session.cert

    def close(self):
        self.session.close()

    def get(self, url):
        return self.send_request(url, method='GET')

    def post(self, url, body, **kwargs):
        return self.send_request(url, method='POST', body=body)

    def delete(self, url):
        return self.send_request(url, method='DELETE')

    def put(self, url, body=None):
        return self.send_request(url, method='PUT', body=body)

    def send_request(self, url, method, body=None, query=None):
        """
        Sends HTTP request to the SchemaRegistry.

        All unsuccessful attempts will raise a SchemaRegistryError with the
        response contents. In most cases this will be accompanied with a
        Schema Registry supplied error code.

        In the event the response is malformed an error_code of -1 will be used.

        Args:
            url (str): Request path
            method (str): HTTP method
            body (str): Request content
            query (dict): Query params to attach to the URL

        Returns:
            dict: Schema Registry response content.
        """
        _headers = self.default_headers

        if body is not None:
            body = json.dumps(body)
            _headers = {'Content-Length': str(len(body)),
                        'Content-Type': "application/vnd.schemaregistry.v1+json"}

        response = self.session.request(
            method, url="/".join([self.base_url, url]),
            headers=_headers, data=body, params=query)

        try:
            if 200 <= response.status_code <= 299:
                return response.json()
            raise SchemaRegistryError(response.status_code,
                                      response.json().get('error_code'),
                                      response.json().get('message'))
        # Schema Registry may return HTML when it hits unexpected errors
        except Exception:
            raise SchemaRegistryError(response.status_code,
                                      -1,
                                      "Unknown Schema Registry Error: "
                                      + response.content)


class _SchemaCache(object):
    """
    Thread-safe Cache for use with the Schema Registry Client.

    This cache maintains two indexes to support the most common queries:
        - schema_id_index
        - schema_index

    """

    def __init__(self):
        self.lock = Lock()
        self.schema_id_index = {}
        self.schema_index = {}

    def set(self, schema_id, schema):
        """
        Add a Schema identified by schema_id to the cache.

        Args:
            schema_id (int): Schema's registration id
            schema (Schema): Schema instance

        Returns:
            int: The schema_id
        """
        with self.lock:
            # Don't overwrite existing keys
            if schema_id not in self.schema_id_index:
                self.schema_id_index[schema_id] = schema
            if schema not in self.schema_index:
                self.schema_index[schema] = schema_id

    def get_schema(self, schema_id):
        """
        Get the schema instance associated with schema_id from the cache.

        Args:
            schema_id (int): Id used to identify a schema
        Returns:
            Schema: The schema if known; else None

        """
        return self.schema_id_index.get(schema_id, None)

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
    Schema Registry Client.

    See Also:
        http://confluent.io/docs/current/schema-registry/docs/intro.html

    Arguments:
        conf (dict): Schema Registry config object

    """
    def __init__(self, conf):
        self._rest_client = _RestClient(conf)
        self.cache = _SchemaCache()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        if self._rest_client is not None:
            self._rest_client.close()

    @property
    def url(self):
        return self._rest_client.url

    @property
    def _auth(self):
        return self._rest_client._auth

    @property
    def _certificate(self):
        return self._rest_client._certificate

    def register_schema(self, subject_name, schema):
        """
        Registers a schema under ``subject_name``.
        Results are cached in the ``schema`` so subsequent requests will not
        require another trip the Schema Registry.

        Args:
            subject_name (str): subject to register a schema under
            schema (Schema): Schema instance to register

        Returns:
            int: Schema id

        Raises:
            SchemaRegistryError: if Schema violates this subject's
            Compatibility policy or is otherwise invalid.

        .. _Schema Registry API Reference:
            https://docs.confluent.io/current/schema-registry/develop/api.html#post--subjects-(string-%20subject)-versions

        """  # noqa: E501
        schema_id = self.cache.get_schema_id(schema)
        if schema_id is not None:
            return schema_id

        response = self._rest_client.post(
            'subjects/{}/versions'.format(_urlencode(subject_name)),
            body={'schema': schema.schema_str})

        schema_id = response['id']
        self.cache.set(schema_id, schema)

        return schema_id

    def get_schema(self, schema_id):
        """
        Fetches the schema associated with ``schema_id`` from the
        Schema Registry. The result is cached so subsequent attempts will not
        require an additional trip to the Schema Registry.

        Args:
            schema_id (int): Schema id

        Returns:
            Schema: Schema instance identified by the ``schema_id``

        Raises:
            SchemaRegistryError: If schema can't be found.

        .. _Schema Registry API Reference:
            https://docs.confluent.io/current/schema-registry/develop/api.html#get--schemas-ids-int-%20id

        """  # noqa: E501
        schema = self.cache.get_schema(schema_id)
        if schema is not None:
            return schema

        response = self._rest_client.get('schemas/ids/{}'.format(schema_id))
        schema = Schema(schema_str=response['schema'],
                        schema_type=response.get('schemaType', 'AVRO'))

        refs = []
        for ref in response.get('references', []):
            refs.append(SchemaReference(name=ref['name'],
                                        subject=ref['subject'],
                                        version=ref['version']))
        schema.references = refs

        self.cache.set(schema_id, schema)

        return schema

    def lookup_schema(self, subject_name, schema):
        """
        Returns ``schema`` registration information for ``subject``.

        Args:
            subject_name (str): Subject name the schema is registered under
            schema (Schema): Schema instance.

        Returns:
            RegisteredSchema: Subject registration information for this schema.

        Raises:
            SchemaRegistryError: If schema or subject can't be found

        .. _Schema Registry API Reference:
            https://docs.confluent.io/current/schema-registry/develop/api.html#post--subjects-(string-%20subject)

        """  # noqa: E501
        response = self._rest_client.post('subjects/{}'
                                          .format(_urlencode(subject_name)),
                                          body={'schema': schema.schema_str})

        schema_type = response.get('schemaType', 'AVRO')

        return RegisteredSchema(schema_id=response['id'],
                                schema_type=schema_type,
                                schema=Schema(response['schema'],
                                              schema_type,
                                              response.get('references', [])),
                                subject=response['subject'],
                                version=response['version'])

    def get_subjects(self):
        """
        List all subjects registered with the Schema Registry

        Returns:
            [str]: Registered subject names

        Raises:
            SchemaRegistryError: if subjects can't be found

        .. _Schema Registry API Reference:
            https://docs.confluent.io/current/schema-registry/develop/api.html#get--subjects

        """  # noqa: E501
        return self._rest_client.get('subjects')

    def delete_subject(self, subject_name):
        """
        Deletes the specified subject and its associated compatibility level if
        registered. It is recommended to use this API only when a topic needs
        to be recycled or in development environments.

        Args:
            subject_name (str): subject name
        Returns:
            list(int): Versions deleted under this subject

        Raises:
            SchemaRegistryError: if the request was unsuccessful.

        .. _Schema Registry API Reference:
            https://docs.confluent.io/current/schema-registry/develop/api.html#delete--subjects-(string-%20subject)

        """  # noqa: E501
        return self._rest_client.delete('subjects/{}'
                                        .format(_urlencode(subject_name)))

    def get_version(self, subject_name, version=None):
        """
        Retrieves a specific schema registered under ``subject_name``.

        Args:
            subject_name (str): Subject name.
            version (int, optional): version number. Defaults to latest version.

        Returns:
            RegisteredSchema: Registration information for this version.

        Raises:
            SchemaRegistryError if the version can't be found or
            is invalid.

        .. _Schema Registry API Reference:
            https://docs.confluent.io/current/schema-registry/develop/api.html#get--subjects-(string-%20subject)-versions-(versionId-%20version)

        """  # noqa: E501
        response = self._rest_client.get('subjects/{}/versions/{}'
                                         .format(_urlencode(subject_name),
                                                 version if not None
                                                 else 'latest'))

        schema_type = response.get('schemaType', 'AVRO')
        return RegisteredSchema(schema_id=response['id'],
                                schema_type=schema_type,
                                schema=Schema(response['schema'],
                                              schema_type,
                                              response.get('references', [])),
                                subject=response['subject'],
                                version=response['version'])

    def get_versions(self, subject_name):
        """
        Get a list of all versions registered with this subject.

        Args:
            subject_name (str): Subject name.

        Returns:
            [int]: Registered versions

        Raises:
            SchemaRegistryError If subject can't be found

        .. _Schema Registry API Reference:
            https://docs.confluent.io/current/schema-registry/develop/api.html#get--subjects-(string-%20subject)-versions

        """  # noqa: E501
        return self._rest_client.get('subjects/{}/versions'.format(_urlencode(subject_name)))

    def delete_version(self, subject_name, version):
        """
        Deletes a specific version registered to ``subject_name``.

        Args:
            subject_name (str) Subject name
            version (int): Version number

        Returns
            int: Version number which was deleted

        Raises:
            SchemaRegistryError if the subject or version cannot be found.

        .. _Schema Registry API Reference:
            https://docs.confluent.io/current/schema-registry/develop/api.html#delete--subjects-(string-%20subject)-versions-(versionId-%20version)

        """  # noqa: E501
        response = self._rest_client.delete('subjects/{}/versions/{}'.
                                            format(_urlencode(subject_name),
                                                   version))
        return response

    def set_compatibility(self, subject_name=None, level=None):
        """
        Update global or subject level compatibility level.

        Args:
            level (str): Compatibility level. See API reference for a list of
                valid values.
            subject_name (str, optional): Subject to update. Sets compatibility
                level policy if not set.

        Returns:
            str: The newly configured compatibility level.

        Raises:
            SchemaRegistryError: If the compatibility level is invalid.

        .. _Schema Registry API Reference:
            https://docs.confluent.io/current/schema-registry/develop/api.html#put--config

        """  # noqa: E501
        if level is None:
            raise ValueError("level must be set")

        if subject_name is None:
            return self._rest_client.put('config',
                                         body={"compatibility": level.upper()})

        return self._rest_client.put('config/{}'
                                     .format(_urlencode(subject_name)),
                                     body={"compatibility": level.upper()})

    def get_compatibility(self, subject_name=None):
        """
        Get the current compatibility level.

        Args:
            - subject_name (str, optional): Subject name. Returns global policy
                if left unset.

        Returns:
            str: Compatibility level for the subject if set, otherwise the
                global compatibility level.

        Raises:
            SchemaRegistryError: if the request was unsuccessful.

        .. _Schema Registry API Reference:
            https://docs.confluent.io/current/schema-registry/develop/api.html#get--config

        """  # noqa: E501
        if subject_name is not None:
            url = 'config/{}'.format(_urlencode(subject_name))
        else:
            url = 'config'

        return self._rest_client.get(url)
