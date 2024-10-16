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
from attrs import define as _attrs_define
from attrs import field as _attrs_field
from collections import defaultdict
from enum import Enum
from threading import Lock
from typing import List, Dict, Any, Type, TypeVar, \
    cast, Optional

from cachetools import TTLCache
from requests import (Session,
                      utils)

from .error import SchemaRegistryError


# TODO: consider adding `six` dependency or employing a compat file
# Python 2.7 is officially EOL so compatibility issue will be come more the norm.
# We need a better way to handle these issues.
# Six is one possibility but the compat file pattern used by requests
# is also quite nice.
#
# six: https://pypi.org/project/six/
# compat file : https://github.com/psf/requests/blob/master/requests/compat.py
try:
    string_type = basestring  # noqa

    def _urlencode(value):
        return urllib.quote(value, safe='')
except NameError:
    string_type = str

    def _urlencode(value):
        return urllib.parse.quote(value, safe='')

log = logging.getLogger(__name__)
VALID_AUTH_PROVIDERS = ['URL', 'USER_INFO']


class _RestClient(object):
    """
    HTTP client for Confluent Schema Registry.

    See SchemaRegistryClient for configuration details.

    Args:
        conf (dict): Dictionary containing _RestClient configuration
    """

    def __init__(self, conf):
        self.session = Session()

        # copy dict to avoid mutating the original
        conf_copy = conf.copy()

        base_url = conf_copy.pop('url', None)
        if base_url is None:
            raise ValueError("Missing required configuration property url")
        if not isinstance(base_url, string_type):
            raise TypeError("url must be an instance of str, not "
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

        if cert is not None and key is not None:
            self.session.cert = (cert, key)

        if cert is not None and key is None:
            self.session.cert = cert

        if key is not None and cert is None:
            raise ValueError("ssl.certificate.location required when"
                             " configuring ssl.key.location")

        userinfo = utils.get_auth_from_url(base_url)
        if 'basic.auth.user.info' in conf_copy:
            if userinfo != ('', ''):
                raise ValueError("basic.auth.user.info configured with"
                                 " userinfo credentials in the URL."
                                 " Remove userinfo credentials from the url or"
                                 " remove basic.auth.user.info from the"
                                 " configuration")

            userinfo = tuple(conf_copy.pop('basic.auth.user.info', '').split(':', 1))

            if len(userinfo) != 2:
                raise ValueError("basic.auth.user.info must be in the form"
                                 " of {username}:{password}")

        self.session.auth = userinfo if userinfo != ('', '') else None

        # Any leftover keys are unknown to _RestClient
        if len(conf_copy) > 0:
            raise ValueError("Unrecognized properties: {}"
                             .format(", ".join(conf_copy.keys())))

    def _close(self):
        self.session.close()

    def get(self, url, query=None):
        return self.send_request(url, method='GET', query=query)

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

        headers = {'Accept': "application/vnd.schemaregistry.v1+json,"
                             " application/vnd.schemaregistry+json,"
                             " application/json"}

        if body is not None:
            body = json.dumps(body)
            headers = {'Content-Length': str(len(body)),
                       'Content-Type': "application/vnd.schemaregistry.v1+json"}

        response = self.session.request(
            method, url="/".join([self.base_url, url]),
            headers=headers, data=body, params=query)

        try:
            if 200 <= response.status_code <= 299:
                return response.json()
            raise SchemaRegistryError(response.status_code,
                                      response.json().get('error_code'),
                                      response.json().get('message'))
        # Schema Registry may return malformed output when it hits unexpected errors
        except (ValueError, KeyError, AttributeError):
            raise SchemaRegistryError(response.status_code,
                                      -1,
                                      "Unknown Schema Registry Error: "
                                      + str(response.content))


class _SchemaCache(object):
    """
    Thread-safe cache for use with the Schema Registry Client.

    This cache may be used to retrieve schema ids, schemas or to check
    known subject membership.
    """

    def __init__(self):
        self.lock = Lock()
        self.schema_id_index = {}
        self.schema_index = {}
        self.subject_schemas = defaultdict(set)

    def set(self, schema_id, schema, subject_name=None):
        """
        Add a Schema identified by schema_id to the cache.

        Args:
            schema_id (int): Schema's registration id

            schema (Schema): Schema instance

            subject_name(str): Optional, subject schema is registered under
        """

        with self.lock:
            self.schema_id_index[schema_id] = schema
            self.schema_index[schema] = schema_id
            if subject_name is not None:
                self.subject_schemas[subject_name].add(schema)

    def get_schema(self, schema_id):
        """
        Get the schema instance associated with schema_id from the cache.

        Args:
            schema_id (int): Id used to identify a schema

        Returns:
            Schema: The schema if known; else None
        """

        with self.lock:
            return self.schema_id_index.get(schema_id, None)

    def get_schema_id_by_subject(self, subject, schema):
        """
        Get the schema_id associated with this schema registered under subject.

        Args:
            subject (str): The subject this schema is associated with

            schema (Schema): The schema associated with this schema_id

        Returns:
            int: Schema ID if known; else None
        """

        with self.lock:
            if schema in self.subject_schemas[subject]:
                return self.schema_index.get(schema, None)

    def remove_by_subject(self, subject_name):
        """
        Remove a Schema from the cache.

        Args:
            subject_name (str): Subject name the schema is registered under.
        """

        with self.lock:
            if subject_name in self.subject_schemas:
                for schema in self.subject_schemas[subject_name]:
                    schema_id = self.schema_index.pop(schema, None)
                    if schema_id is not None:
                        self.schema_id_index.pop(schema_id, None)

                del self.subject_schemas[subject_name]

    def clear(self):
        """
        Clear the cache.
        """

        with self.lock:
            self.schema_id_index.clear()
            self.schema_index.clear()
            self.subject_schemas.clear()


class _RegisteredSchemaCache(object):
    """
    Thread-safe cache for use with the Schema Registry Client.

    This cache may be used to retrieve registered schemas based on subject_name/version/schema
    - Get registered schema based on subject name + version
    - Get registered schema based on subject name + schema
    """

    def __init__(self):
        self.lock = Lock()
        self.schema_version_index = defaultdict(dict)
        self.schema_index = defaultdict(dict)

    def set(self, subject_name, schema, version, registered_schema):
        """
        Add a Schema identified by schema_id to the cache.

        Args:
            subject_name (str): The subject name this registered schema is associated with

            schema (Schema): The schema this registered schema is associated with

            version (int): The version this registered schema is associated with

            registered_schema (RegisteredSchema): The registered schema instance
        """

        with self.lock:
            if schema is not None:
                self.schema_index[subject_name][schema] = registered_schema
            elif version is not None:
                self.schema_version_index[subject_name][version] = registered_schema

    def get_registered_schema_by_version(self, subject_name, version):
        """
        Get the registered schema instance associated with version from the cache.

        Args:
            subject_name (str): The subject name this registered schema is associated with

            version (int): The version this registered schema is associated with

        Returns:
            RegisteredSchema: The registered schema if known; else None
        """

        with self.lock:
            return self.schema_version_index.get(subject_name, {}).get(version, None)

    def get_registered_schema_by_schema(self, subject_name, schema):
        """
        Get the registered schema instance associated with schema from the cache.

        Args:
            subject_name (str): The subject name this registered schema is associated with

            schema (Schema): The schema this registered schema is associated with

        Returns:
            RegisteredSchema: The registered schema if known; else None
        """

        with self.lock:
            return self.schema_index.get(subject_name, {}).get(schema, None)

    def clear(self):
        """
        Clear the cache.
        """

        with self.lock:
            self.schema_version_index.clear()
            self.schema_index.clear()


class SchemaRegistryClient(object):
    """
    A Confluent Schema Registry client.

    Configuration properties (* indicates a required field):

    +------------------------------+------+-------------------------------------------------+
    | Property name                | type | Description                                     |
    +==============================+======+=================================================+
    | ``url`` *                    | str  | Schema Registry URL.                            |
    +------------------------------+------+-------------------------------------------------+
    |                              |      | Path to CA certificate file used                |
    | ``ssl.ca.location``          | str  | to verify the Schema Registry's                 |
    |                              |      | private key.                                    |
    +------------------------------+------+-------------------------------------------------+
    |                              |      | Path to client's private key                    |
    |                              |      | (PEM) used for authentication.                  |
    | ``ssl.key.location``         | str  |                                                 |
    |                              |      | ``ssl.certificate.location`` must also be set.  |
    +------------------------------+------+-------------------------------------------------+
    |                              |      | Path to client's public key (PEM) used for      |
    |                              |      | authentication.                                 |
    | ``ssl.certificate.location`` | str  |                                                 |
    |                              |      | May be set without ssl.key.location if the      |
    |                              |      | private key is stored within the PEM as well.   |
    +------------------------------+------+-------------------------------------------------+
    |                              |      | Client HTTP credentials in the form of          |
    |                              |      | ``username:password``.                          |
    | ``basic.auth.user.info``     | str  |                                                 |
    |                              |      | By default userinfo is extracted from           |
    |                              |      | the URL if present.                             |
    +------------------------------+------+-------------------------------------------------+

    Args:
        conf (dict): Schema Registry client configuration.

    See Also:
        `Confluent Schema Registry documentation <http://confluent.io/docs/current/schema-registry/docs/intro.html>`_
    """  # noqa: E501

    def __init__(self, conf):
        self._rest_client = _RestClient(conf)
        self._cache = _SchemaCache()
        self._metadata_cache = _RegisteredSchemaCache()
        # TODO RAY cache ttl
        self._latest_version_cache = TTLCache(1000, 60)
        self._latest_with_metadata_cache = TTLCache(1000, 60)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        if self._rest_client is not None:
            self._rest_client._close()

    def register_schema(self, subject_name, schema, normalize_schemas=False):
        """
        Registers a schema under ``subject_name``.

        Args:
            subject_name (str): subject to register a schema under

            schema (Schema): Schema instance to register

        Returns:
            int: Schema id

        Raises:
            SchemaRegistryError: if Schema violates this subject's
                Compatibility policy or is otherwise invalid.

        See Also:
            `POST Subject API Reference <https://docs.confluent.io/current/schema-registry/develop/api.html#post--subjects-(string-%20subject)-versions>`_
        """  # noqa: E501

        schema_id = self._cache.get_schema_id_by_subject(subject_name, schema)
        if schema_id is not None:
            return schema_id

        request = {'schema': schema.schema_str}

        # CP 5.5 adds new fields (for JSON and Protobuf).
        if len(schema.references) > 0 or schema.schema_type != 'AVRO':
            request['schemaType'] = schema.schema_type
            request['references'] = [{'name': ref.name,
                                      'subject': ref.subject,
                                      'version': ref.version}
                                     for ref in schema.references]

        response = self._rest_client.post(
            'subjects/{}/versions?normalize={}'.format(_urlencode(subject_name), normalize_schemas),
            body=request)

        schema_id = response['id']
        self._cache.set(schema_id, schema, subject_name)

        return schema_id

    def get_schema(self, schema_id, subject_name=None, format=None):
        """
        Fetches the schema associated with ``schema_id`` from the
        Schema Registry. The result is cached so subsequent attempts will not
        require an additional round-trip to the Schema Registry.

        Args:
            schema_id (int): Schema id

        Returns:
            Schema: Schema instance identified by the ``schema_id``

        Raises:
            SchemaRegistryError: If schema can't be found.

        See Also:
         `GET Schema API Reference <https://docs.confluent.io/current/schema-registry/develop/api.html#get--schemas-ids-int-%20id>`_
        """  # noqa: E501

        schema = self._cache.get_schema(schema_id)
        if schema is not None:
            return schema

        query = {'subject': subject_name} if subject_name is not None else None
        if format is not None:
            if query is not None:
                query['format'] = format
            else:
                query = {'format': format}
        response = self._rest_client.get('schemas/ids/{}'.format(schema_id), query)
        schema = Schema(schema_str=response['schema'],
                        schema_type=response.get('schemaType', 'AVRO'))

        schema.references = [
            SchemaReference(name=ref['name'], subject=ref['subject'], version=ref['version'])
            for ref in response.get('references', [])
        ]
        metadata: Optional[Dict[str, Any]] = response.get("metadata")
        if metadata is not None:
            schema.metadata = Metadata.from_dict(metadata)
        rule_set: Optional[Dict[str, Any]] = response.get("ruleSet")
        if rule_set is not None:
            schema.rule_set = RuleSet.from_dict(rule_set)

        self._cache.set(schema_id, schema)

        return schema

    def lookup_schema(self, subject_name, schema, normalize_schemas=False):
        """
        Returns ``schema`` registration information for ``subject``.

        Args:
            subject_name (str): Subject name the schema is registered under

            schema (Schema): Schema instance.

        Returns:
            RegisteredSchema: Subject registration information for this schema.

        Raises:
            SchemaRegistryError: If schema or subject can't be found

        See Also:
            `POST Subject API Reference <https://docs.confluent.io/current/schema-registry/develop/api.html#post--subjects-(string-%20subject)-versions>`_
        """  # noqa: E501

        registered_schema = self._metadata_cache.get_registered_schema_by_schema(subject_name, schema)
        if registered_schema is not None:
            return registered_schema

        request = {'schema': schema.schema_str}

        # CP 5.5 adds new fields (for JSON and Protobuf).
        if len(schema.references) > 0 or schema.schema_type != 'AVRO':
            request['schemaType'] = schema.schema_type
            request['references'] = [{'name': ref.name,
                                      'subject': ref.subject,
                                      'version': ref.version}
                                     for ref in schema.references]

        response = self._rest_client.post('subjects/{}?normalize={}'
                                          .format(_urlencode(subject_name), normalize_schemas),
                                          body=request)

        schema_type = response.get('schemaType', 'AVRO')
        registered_schema = RegisteredSchema(
            schema_id=response['id'],
            schema=Schema(
                response['schema'],
                schema_type,
                [
                    SchemaReference(
                        name=ref['name'],
                        subject=ref['subject'],
                        version=ref['version']
                    ) for ref in response.get('references', [])
                ]
            ),
            subject=response['subject'],
            version=response['version']
        )
        metadata: Optional[Dict[str, Any]] = response.get("metadata")
        if metadata is not None:
            registered_schema.schema.metadata = Metadata.from_dict(metadata)
        rule_set: Optional[Dict[str, Any]] = response.get("ruleSet")
        if rule_set is not None:
            registered_schema.schema.rule_set = RuleSet.from_dict(rule_set)

        self._metadata_cache.set(subject_name, schema, None, registered_schema)

        return registered_schema

    def get_subjects(self):
        """
        List all subjects registered with the Schema Registry

        Returns:
            list(str): Registered subject names

        Raises:
            SchemaRegistryError: if subjects can't be found

        See Also:
            `GET subjects API Reference <https://docs.confluent.io/current/schema-registry/develop/api.html#get--subjects-(string-%20subject)-versions>`_
        """  # noqa: E501

        return self._rest_client.get('subjects')

    def delete_subject(self, subject_name, permanent=False):
        """
        Deletes the specified subject and its associated compatibility level if
        registered. It is recommended to use this API only when a topic needs
        to be recycled or in development environments.

        Args:
            subject_name (str): subject name
            permanent (bool): True for a hard delete, False (default) for a soft delete

        Returns:
            list(int): Versions deleted under this subject

        Raises:
            SchemaRegistryError: if the request was unsuccessful.

        See Also:
            `DELETE Subject API Reference <https://docs.confluent.io/current/schema-registry/develop/api.html#delete--subjects-(string-%20subject)>`_
        """  # noqa: E501

        list = self._rest_client.delete('subjects/{}'
                                        .format(_urlencode(subject_name)))

        if permanent:
            self._rest_client.delete('subjects/{}?permanent=true'
                                     .format(_urlencode(subject_name)))

        self._cache.remove_by_subject(subject_name)

        return list

    def get_latest_version(self, subject_name, format=None):
        """
        Retrieves latest registered version for subject

        Args:
            subject_name (str): Subject name.

        Returns:
            RegisteredSchema: Registration information for this version.

        Raises:
            SchemaRegistryError: if the version can't be found or is invalid.

        See Also:
            `GET Subject Version API Reference <https://docs.confluent.io/current/schema-registry/develop/api.html#get--subjects-(string-%20subject)-versions-(versionId-%20version)>`_
        """  # noqa: E501

        registered_schema = self._latest_version_cache.get(subject_name, None)
        if registered_schema is not None:
            return registered_schema

        query = {'format': format} if format is not None else None
        response = self._rest_client.get('subjects/{}/versions/{}'
                                         .format(_urlencode(subject_name),
                                                 'latest'), query)

        schema_type = response.get('schemaType', 'AVRO')
        registered_schema = RegisteredSchema(
            schema_id=response['id'],
            schema=Schema(
                response['schema'],
                schema_type,
                [
                    SchemaReference(
                        name=ref['name'],
                        subject=ref['subject'],
                        version=ref['version']
                    ) for ref in response.get('references', [])
                ]
            ),
            subject=response['subject'],
            version=response['version']
        )
        metadata: Optional[Dict[str, Any]] = response.get("metadata")
        if metadata is not None:
            registered_schema.schema.metadata = Metadata.from_dict(metadata)
        rule_set: Optional[Dict[str, Any]] = response.get("ruleSet")
        if rule_set is not None:
            registered_schema.schema.rule_set = RuleSet.from_dict(rule_set)
        self._latest_version_cache[subject_name] = registered_schema
        return registered_schema

    def get_latest_with_metadata(self, subject_name, metadata, format=None):
        """
        Retrieves latest registered version for subject with the given metadata

        Args:
            subject_name (str): Subject name.
            metadata (dict): The key-value pairs for the metadata.

        Returns:
            RegisteredSchema: Registration information for this version.

        Raises:
            SchemaRegistryError: if the version can't be found or is invalid.
        """  # noqa: E501

        cache_key = (subject_name, frozenset(metadata.items()))
        registered_schema = self._latest_with_metadata_cache.get(cache_key, None)
        if registered_schema is not None:
            return registered_schema

        query = {'format': format} if format is not None else None
        response = self._rest_client.get('subjects/{}/versions/{}'
                                         .format(_urlencode(subject_name),
                                                 'latest'), query)

        schema_type = response.get('schemaType', 'AVRO')
        registered_schema = RegisteredSchema(
            schema_id=response['id'],
            schema=Schema(
                response['schema'],
                schema_type,
                [
                    SchemaReference(
                        name=ref['name'],
                        subject=ref['subject'],
                        version=ref['version']
                    ) for ref in response.get('references', [])
                ]
            ),
            subject=response['subject'],
            version=response['version']
        )
        metadata: Optional[Dict[str, Any]] = response.get("metadata")
        if metadata is not None:
            registered_schema.schema.metadata = Metadata.from_dict(metadata)
        rule_set: Optional[Dict[str, Any]] = response.get("ruleSet")
        if rule_set is not None:
            registered_schema.schema.rule_set = RuleSet.from_dict(rule_set)
        self._latest_with_metadata_cache[cache_key] = registered_schema
        return registered_schema

    def get_version(self, subject_name, version, format=None):
        """
        Retrieves a specific schema registered under ``subject_name``.

        Args:
            subject_name (str): Subject name.

            version (int): version number. Defaults to latest version.

        Returns:
            RegisteredSchema: Registration information for this version.

        Raises:
            SchemaRegistryError: if the version can't be found or is invalid.

        See Also:
            `GET Subject Version API Reference <https://docs.confluent.io/current/schema-registry/develop/api.html#get--subjects-(string-%20subject)-versions-(versionId-%20version)>`_
        """  # noqa: E501

        registered_schema = self._metadata_cache.get_registered_schema_by_version(subject_name, version)
        if registered_schema is not None:
            return registered_schema

        query = {'format': format} if format is not None else None
        response = self._rest_client.get('subjects/{}/versions/{}'
                                         .format(_urlencode(subject_name),
                                                 version), query)

        schema_type = response.get('schemaType', 'AVRO')
        registered_schema = RegisteredSchema(
            schema_id=response['id'],
            schema=Schema(
                response['schema'],
                schema_type,
                [
                    SchemaReference(
                        name=ref['name'],
                        subject=ref['subject'],
                        version=ref['version']
                    ) for ref in response.get('references', [])
                ]
            ),
            subject=response['subject'],
            version=response['version']
        )
        metadata: Optional[Dict[str, Any]] = response.get("metadata")
        if metadata is not None:
            registered_schema.schema.metadata = Metadata.from_dict(metadata)
        rule_set: Optional[Dict[str, Any]] = response.get("ruleSet")
        if rule_set is not None:
            registered_schema.schema.rule_set = RuleSet.from_dict(rule_set)
        self._metadata_cache.set(subject_name, None, version, registered_schema)

        return registered_schema

    def get_versions(self, subject_name):
        """
        Get a list of all versions registered with this subject.

        Args:
            subject_name (str): Subject name.

        Returns:
            list(int): Registered versions

        Raises:
            SchemaRegistryError: If subject can't be found

        See Also:
            `GET Subject Versions API Reference <https://docs.confluent.io/current/schema-registry/develop/api.html#post--subjects-(string-%20subject)-versions>`_
        """  # noqa: E501

        return self._rest_client.get('subjects/{}/versions'.format(_urlencode(subject_name)))

    def delete_version(self, subject_name, version):
        """
        Deletes a specific version registered to ``subject_name``.

        Args:
            subject_name (str) Subject name

            version (int): Version number

        Returns:
            int: Version number which was deleted

        Raises:
            SchemaRegistryError: if the subject or version cannot be found.

        See Also:
            `Delete Subject Version API Reference <https://docs.confluent.io/current/schema-registry/develop/api.html#delete--subjects-(string-%20subject)-versions-(versionId-%20version)>`_
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

        See Also:
            `PUT Subject Compatibility API Reference <https://docs.confluent.io/current/schema-registry/develop/api.html#put--config-(string-%20subject)>`_
        """  # noqa: E501

        if level is None:
            raise ValueError("level must be set")

        if subject_name is None:
            return self._rest_client.put('config',
                                         body={'compatibility': level.upper()})

        return self._rest_client.put('config/{}'
                                     .format(_urlencode(subject_name)),
                                     body={'compatibility': level.upper()})

    def get_compatibility(self, subject_name=None):
        """
        Get the current compatibility level.

        Args:
            subject_name (str, optional): Subject name. Returns global policy
                if left unset.

        Returns:
            str: Compatibility level for the subject if set, otherwise the global compatibility level.

        Raises:
            SchemaRegistryError: if the request was unsuccessful.

        See Also:
            `GET Subject Compatibility API Reference <https://docs.confluent.io/current/schema-registry/develop/api.html#get--config-(string-%20subject)>`_
        """  # noqa: E501

        if subject_name is not None:
            url = 'config/{}'.format(_urlencode(subject_name))
        else:
            url = 'config'

        result = self._rest_client.get(url)
        return result['compatibilityLevel']

    def test_compatibility(self, subject_name, schema, version="latest"):
        """Test the compatibility of a candidate schema for a given subject and version

        Args:
            subject_name (str): Subject name the schema is registered under

            schema (Schema): Schema instance.

            version (int or str, optional): Version number, or the string "latest". Defaults to "latest".

        Returns:
            bool: True if the schema is compatible with the specified version

        Raises:
            SchemaRegistryError: if the request was unsuccessful.

        See Also:
            `POST Test Compatibility API Reference <https://docs.confluent.io/current/schema-registry/develop/api.html#post--compatibility-subjects-(string-%20subject)-versions-(versionId-%20version)>`_
        """  # noqa: E501

        request = {"schema": schema.schema_str}
        if schema.schema_type != "AVRO":
            request['schemaType'] = schema.schema_type

        if schema.references:
            request['references'] = [
                {'name': ref.name, 'subject': ref.subject, 'version': ref.version}
                for ref in schema.references
            ]

        response = self._rest_client.post(
            'compatibility/subjects/{}/versions/{}'.format(subject_name, version), body=request
        )

        return response['is_compatible']

    def clear_latest_caches(self):
        self._latest_version_cache.clear()
        self._latest_with_metadata_cache.clear()

    def clear_caches(self):
        self._latest_version_cache.clear()
        self._latest_with_metadata_cache.clear()
        self._cache.clear()
        self._metadata_cache.clear()



T = TypeVar("T")


class RuleKind(str, Enum):
    CONDITION = "CONDITION"
    TRANSFORM = "TRANSFORM"

    def __str__(self) -> str:
        return str(self.value)


class RuleMode(str, Enum):
    UPGRADE = "UPGRADE"
    DOWNGRADE = "DOWNGRADE"
    UPDOWN = "UPDOWN"
    READ = "READ"
    WRITE = "WRITE"
    WRITEREAD = "WRITEREAD"

    def __str__(self) -> str:
        return str(self.value)


@_attrs_define
class RuleParams:
    params: Dict[str, str] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        field_dict: Dict[str, Any] = {}
        field_dict.update(self.params)

        return field_dict

    @classmethod
    def from_dict(cls: Type[T], src_dict: Dict[str, Any]) -> T:
        d = src_dict.copy()
        rule_params = cls()

        rule_params.params = d
        return rule_params

    @property
    def additional_keys(self) -> List[str]:
        return list(self.params.keys())

    def __getitem__(self, key: str) -> str:
        return self.params[key]

    def __setitem__(self, key: str, value: str) -> None:
        self.params[key] = value

    def __delitem__(self, key: str) -> None:
        del self.params[key]

    def __contains__(self, key: str) -> bool:
        return key in self.params


@_attrs_define
class Rule:
    name: Optional[str]
    doc: Optional[str]
    kind: Optional[RuleKind]
    mode: Optional[RuleMode]
    type: Optional[str]
    tags: Optional[List[str]]
    params: Optional[RuleParams]
    expr: Optional[str]
    on_success: Optional[str]
    on_failure: Optional[str]
    disabled: Optional[bool]

    def to_dict(self) -> Dict[str, Any]:
        name = self.name

        doc = self.doc

        kind: Optional[str] = None
        if self.kind is not None:
            kind = self.kind.value

        mode: Optional[str] = None
        if self.mode is not None:
            mode = self.mode.value

        type = self.type

        tags = self.tags

        params: Optional[Dict[str, Any]] = None
        if self.params is not None:
            params = self.params.to_dict()

        expr = self.expr

        on_success = self.on_success

        on_failure = self.on_failure

        disabled = self.disabled

        field_dict: Dict[str, Any] = {}
        field_dict.update({})
        if name is not None:
            field_dict["name"] = name
        if doc is not None:
            field_dict["doc"] = doc
        if kind is not None:
            field_dict["kind"] = kind
        if mode is not None:
            field_dict["mode"] = mode
        if type is not None:
            field_dict["type"] = type
        if tags is not None:
            field_dict["tags"] = tags
        if params is not None:
            field_dict["params"] = params
        if expr is not None:
            field_dict["expr"] = expr
        if on_success is not None:
            field_dict["onSuccess"] = on_success
        if on_failure is not None:
            field_dict["onFailure"] = on_failure
        if disabled is not None:
            field_dict["disabled"] = disabled

        return field_dict

    @classmethod
    def from_dict(cls: Type[T], src_dict: Dict[str, Any]) -> T:

        d = src_dict.copy()
        name = d.pop("name", None)

        doc = d.pop("doc", None)

        _kind = d.pop("kind", None)
        kind: Optional[RuleKind] = None
        if _kind is not None:
            kind = RuleKind(_kind)

        _mode = d.pop("mode", None)
        mode: Optional[RuleMode] = None
        if _mode is not None:
            mode = RuleMode(_mode)

        type = d.pop("type", None)

        tags = cast(List[str], d.pop("tags", None))

        _params: Optional[Dict[str, Any]] = d.pop("params", None)
        params: Optional[RuleParams] = None
        if _params is not None:
            params = RuleParams.from_dict(_params)

        expr = d.pop("expr", None)

        on_success = d.pop("onSuccess", None)

        on_failure = d.pop("onFailure", None)

        disabled = d.pop("disabled", None)

        rule = cls(
            name=name,
            doc=doc,
            kind=kind,
            mode=mode,
            type=type,
            tags=tags,
            params=params,
            expr=expr,
            on_success=on_success,
            on_failure=on_failure,
            disabled=disabled,
        )

        return rule


@_attrs_define
class RuleSet:
    migration_rules: Optional[List["Rule"]]
    domain_rules: Optional[List["Rule"]]

    def to_dict(self) -> Dict[str, Any]:
        migration_rules: Optional[List[Dict[str, Any]]] = None
        if self.migration_rules is not None:
            migration_rules = []
            for migration_rules_item_data in self.migration_rules:
                migration_rules_item = migration_rules_item_data.to_dict()
                migration_rules.append(migration_rules_item)

        domain_rules: Optional[List[Dict[str, Any]]] = None
        if self.domain_rules is not None:
            domain_rules = []
            for domain_rules_item_data in self.domain_rules:
                domain_rules_item = domain_rules_item_data.to_dict()
                domain_rules.append(domain_rules_item)

        field_dict: Dict[str, Any] = {}
        field_dict.update({})
        if migration_rules is not None:
            field_dict["migrationRules"] = migration_rules
        if domain_rules is not None:
            field_dict["domainRules"] = domain_rules

        return field_dict

    @classmethod
    def from_dict(cls: Type[T], src_dict: Dict[str, Any]) -> T:

        d = src_dict.copy()
        migration_rules = []
        _migration_rules = d.pop("migrationRules", None)
        for migration_rules_item_data in _migration_rules or []:
            migration_rules_item = Rule.from_dict(migration_rules_item_data)
            migration_rules.append(migration_rules_item)

        domain_rules = []
        _domain_rules = d.pop("domainRules", None)
        for domain_rules_item_data in _domain_rules or []:
            domain_rules_item = Rule.from_dict(domain_rules_item_data)
            domain_rules.append(domain_rules_item)

        rule_set = cls(
            migration_rules=migration_rules,
            domain_rules=domain_rules,
        )

        return rule_set


@_attrs_define
class MetadataTags:
    tags: Dict[str, List[str]] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        field_dict: Dict[str, Any] = {}
        for prop_name, prop in self.tags.items():
            field_dict[prop_name] = prop

        return field_dict

    @classmethod
    def from_dict(cls: Type[T], src_dict: Dict[str, Any]) -> T:
        d = src_dict.copy()
        metadata_tags = cls()

        tags = {}
        for prop_name, prop_dict in d.items():
            tag = cast(List[str], prop_dict)

            tags[prop_name] = tag

        metadata_tags.tags = tags
        return metadata_tags


@_attrs_define
class MetadataProperties:
    properties: Dict[str, str] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        field_dict: Dict[str, Any] = {}
        field_dict.update(self.properties)

        return field_dict

    @classmethod
    def from_dict(cls: Type[T], src_dict: Dict[str, Any]) -> T:
        d = src_dict.copy()
        metadata_properties = cls()

        metadata_properties.properties = d
        return metadata_properties


@_attrs_define
class Metadata:
    tags: Optional[MetadataTags]
    properties: Optional[MetadataProperties]
    sensitive: Optional[List[str]]

    def to_dict(self) -> Dict[str, Any]:
        tags: Optional[Dict[str, Any]] = None
        if self.tags is not None:
            tags = self.tags.to_dict()

        properties: Optional[Dict[str, Any]] = None
        if self.properties is not None:
            properties = self.properties.to_dict()

        sensitive: Optional[List[str]] = None
        if self.sensitive is not None:
            sensitive = []
            for sensitive_item in self.sensitive:
                sensitive.append(sensitive_item)

        field_dict: Dict[str, Any] = {}
        if tags is not None:
            field_dict["tags"] = tags
        if properties is not None:
            field_dict["properties"] = properties
        if sensitive is not None:
            field_dict["sensitive"] = sensitive

        return field_dict

    @classmethod
    def from_dict(cls: Type[T], src_dict: Dict[str, Any]) -> T:

        d = src_dict.copy()
        _tags: Optional[Dict[str, Any]] = d.pop("tags", None)
        tags: Optional[MetadataTags] = None
        if _tags is not None:
            tags = MetadataTags.from_dict(_tags)

        _properties: Optional[Dict[str, Any]] = d.pop("properties", None)
        properties: Optional[MetadataProperties] = None
        if _properties is not None:
            properties = MetadataProperties.from_dict(_properties)

        sensitive = []
        _sensitive = d.pop("sensitive", None)
        for sensitive_item in _sensitive or []:
            sensitive.append(sensitive_item)

        metadata = cls(
            tags=tags,
            properties=properties,
            sensitive=sensitive,
        )

        return metadata


class SchemaReference(object):
    """
    Reference to a Schema registered with the Schema Registry.

    As of Confluent Platform 5.5 Schema's may now hold references to other
    registered schemas. As long as there is a references to a schema it may not
    be deleted.

    Args:
        name (str): Schema name

        subject (str): Subject this Schema is registered with

        version (int): This Schema's version
    """

    def __init__(self, name: str, subject: str, version: int):
        self.name = name
        self.subject = subject
        self.version = version


class Schema(object):
    """
    An unregistered Schema.

    Args:
        schema_str (str): String representation of the schema.

        schema_type (str): The schema type: AVRO, PROTOBUF or JSON.

        references ([SchemaReference]): SchemaReferences used in this schema.
    """
    __slots__ = ['schema_str', 'schema_type', 'references', 'metadata', 'rule_set', '_hash']

    def __init__(self, schema_str: str, schema_type: str, references:List[SchemaReference] = [],
        metadata: Metadata = None, rule_set: RuleSet = None):
        self.schema_str = schema_str
        self.schema_type = schema_type
        self.references = references
        self.metadata = metadata
        self.rule_set = rule_set
        # TODO RAY - fix hash
        self._hash = hash(schema_str)

    def __eq__(self, other):
        return all([self.schema_str == other.schema_str,
                    self.schema_type == other.schema_type,
                    self.references == other.references,
                    self.metadata == other.metadata,
                    self.rule_set == other.rule_set])

    def __hash__(self):
        return self._hash


class RegisteredSchema(object):
    """
    Schema registration information.

    Represents a  Schema registered with a subject. Use this class when you need
    a specific version of a subject such as forming a SchemaReference.

    Args:
        schema_id (int): Registered Schema id

        schema (Schema): Registered Schema

        subject (str): Subject this schema is registered under

        version (int): Version of this subject this schema is registered to
    """

    def __init__(self, schema_id: int, schema: Schema, subject: str, version: int):
        self.schema_id = schema_id
        self.schema = schema
        self.subject = subject
        self.version = version
