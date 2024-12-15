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
import random
import time
import urllib
from urllib.parse import unquote, urlparse

import httpx
from attrs import define as _attrs_define
from attrs import field as _attrs_field
from collections import defaultdict
from enum import Enum
from threading import Lock
from typing import List, Dict, Type, TypeVar, \
    cast, Optional, Union, Any, Tuple

from cachetools import TTLCache, LRUCache
from httpx import Response

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

    def _urlencode(value: str) -> str:
        return urllib.quote(value, safe='')
except NameError:
    string_type = str

    def _urlencode(value: str) -> str:
        return urllib.parse.quote(value, safe='')

log = logging.getLogger(__name__)
VALID_AUTH_PROVIDERS = ['URL', 'USER_INFO']


class _BaseRestClient(object):

    def __init__(self, conf: dict):
        # copy dict to avoid mutating the original
        conf_copy = conf.copy()

        base_url = conf_copy.pop('url', None)
        if base_url is None:
            raise ValueError("Missing required configuration property url")
        if not isinstance(base_url, string_type):
            raise TypeError("url must be a str, not " + str(type(base_url)))
        base_urls = []
        for url in base_url.split(','):
            url = url.strip().rstrip('/')
            if not url.startswith('http') and not url.startswith('mock'):
                raise ValueError("Invalid url {}".format(url))
            base_urls.append(url)
        if not base_urls:
            raise ValueError("Missing required configuration property url")
        self.base_urls = base_urls

        self.verify = True
        ca = conf_copy.pop('ssl.ca.location', None)
        if ca is not None:
            self.verify = ca

        key: Optional[str] = conf_copy.pop('ssl.key.location', None)
        client_cert: Optional[str] = conf_copy.pop('ssl.certificate.location', None)
        self.cert: Union[str, Tuple[str, str], None] = None

        if client_cert is not None and key is not None:
            self.cert = (client_cert, key)

        if client_cert is not None and key is None:
            self.cert = client_cert

        if key is not None and client_cert is None:
            raise ValueError("ssl.certificate.location required when"
                             " configuring ssl.key.location")

        parsed = urlparse(self.base_urls[0])
        try:
            userinfo = (unquote(parsed.username), unquote(parsed.password))
        except (AttributeError, TypeError):
            userinfo = ("", "")
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

        self.auth = userinfo if userinfo != ('', '') else None

        # The following adds support for proxy config
        # If specified: it uses the specified proxy details when making requests
        self.proxy = None
        proxy = conf_copy.pop('proxy', None)
        if proxy is not None:
            self.proxy = proxy

        self.timeout = None
        timeout = conf_copy.pop('timeout', None)
        if timeout is not None:
            self.timeout = timeout

        self.cache_capacity = 1000
        cache_capacity = conf_copy.pop('cache.capacity', None)
        if cache_capacity is not None:
            if not isinstance(cache_capacity, (int, float)):
                raise TypeError("cache.capacity must be a number, not " + str(type(cache_capacity)))
            self.cache_capacity = cache_capacity

        self.cache_latest_ttl_sec = None
        cache_latest_ttl_sec = conf_copy.pop('cache.latest.ttl.sec', None)
        if cache_latest_ttl_sec is not None:
            if not isinstance(cache_latest_ttl_sec, (int, float)):
                raise TypeError("cache.latest.ttl.sec must be a number, not " + str(type(cache_latest_ttl_sec)))
            self.cache_latest_ttl_sec = cache_latest_ttl_sec

        self.max_retries = 3
        max_retries = conf_copy.pop('max.retries', None)
        if max_retries is not None:
            if not isinstance(timeout, (int, float)):
                raise TypeError("max.retries must be a number, not " + str(type(max_retries)))
            self.max_retries = max_retries

        self.retries_wait_ms = 1000
        retries_wait_ms = conf_copy.pop('retries.wait.ms', None)
        if retries_wait_ms is not None:
            if not isinstance(retries_wait_ms, (int, float)):
                raise TypeError("retries.wait.ms must be a number, not "
                                + str(type(retries_wait_ms)))
            self.retries_wait_ms = retries_wait_ms

        self.retries_max_wait_ms = 20000
        retries_max_wait_ms = conf_copy.pop('retries.max.wait.ms', None)
        if retries_max_wait_ms is not None:
            if not isinstance(retries_max_wait_ms, (int, float)):
                raise TypeError("retries.max.wait.ms must be a number, not "
                                + str(type(retries_max_wait_ms)))
            self.retries_max_wait_ms = retries_max_wait_ms

        # Any leftover keys are unknown to _RestClient
        if len(conf_copy) > 0:
            raise ValueError("Unrecognized properties: {}"
                             .format(", ".join(conf_copy.keys())))

    def get(self, url: str, query: dict = None) -> Any:
        raise NotImplementedError()

    def post(self, url: str, body: dict, **kwargs) -> Any:
        raise NotImplementedError()

    def delete(self, url: str) -> Any:
        raise NotImplementedError()

    def put(self, url: str, body: dict = None) -> Any:
        raise NotImplementedError()


class _RestClient(_BaseRestClient):
    """
    HTTP client for Confluent Schema Registry.

    See SchemaRegistryClient for configuration details.

    Args:
        conf (dict): Dictionary containing _RestClient configuration
    """

    def __init__(self, conf: dict):
        super().__init__(conf)

        self.session = httpx.Client(
            verify=self.verify,
            cert=self.cert,
            auth=self.auth,
            proxy=self.proxy,
            timeout=self.timeout
        )

    def get(self, url: str, query: dict = None) -> Any:
        return self.send_request(url, method='GET', query=query)

    def post(self, url: str, body: dict, **kwargs) -> Any:
        return self.send_request(url, method='POST', body=body)

    def delete(self, url: str) -> Any:
        return self.send_request(url, method='DELETE')

    def put(self, url: str, body: dict = None) -> Any:
        return self.send_request(url, method='PUT', body=body)

    def send_request(
        self, url: str, method: str, body: dict = None,
        query: dict = None
    ) -> Any:
        """
        Sends HTTP request to the SchemaRegistry, trying each base URL in turn.

        All unsuccessful attempts will raise a SchemaRegistryError with the
        response contents. In most cases this will be accompanied by a
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

        response = None
        for i, base_url in enumerate(self.base_urls):
            try:
                response = self.send_http_request(
                    base_url, url, method, headers, body, query)

                if is_success(response.status_code):
                    return response.json()

                if not is_retriable(response.status_code) or i == len(self.base_urls) - 1:
                    break
            except Exception as e:
                if i == len(self.base_urls) - 1:
                    # Raise the exception since we have no more urls to try
                    raise e

        try:
            raise SchemaRegistryError(response.status_code,
                                      response.json().get('error_code'),
                                      response.json().get('message'))
        # Schema Registry may return malformed output when it hits unexpected errors
        except (ValueError, KeyError, AttributeError):
            raise SchemaRegistryError(response.status_code,
                                      -1,
                                      "Unknown Schema Registry Error: "
                                      + str(response.content))

    def send_http_request(
        self, base_url: str, url: str, method: str, headers: dict,
        body: Optional[str] = None, query: dict = None
    ) -> Response:
        """
        Sends HTTP request to the SchemaRegistry.

        All unsuccessful attempts will raise a SchemaRegistryError with the
        response contents. In most cases this will be accompanied by a
        Schema Registry supplied error code.

        In the event the response is malformed an error_code of -1 will be used.

        Args:
            base_url (str): Schema Registry base URL

            url (str): Request path

            method (str): HTTP method

            headers (dict): Headers

            body (str): Request content

            query (dict): Query params to attach to the URL

        Returns:
            Response: Schema Registry response content.
        """
        for i in range(self.max_retries + 1):
            response = self.session.request(
                method, url="/".join([base_url, url]),
                headers=headers, data=body, params=query)

            if is_success(response.status_code):
                return response

            if not is_retriable(response.status_code) or i >= self.max_retries:
                return response

            time.sleep(full_jitter(self.retries_wait_ms, self.retries_max_wait_ms, i) / 1000)


def is_success(status_code: int) -> bool:
    return 200 <= status_code <= 299


def is_retriable(status_code: int) -> bool:
    return status_code in (408, 429, 500, 502, 503, 504)


def full_jitter(base_delay_ms: int, max_delay_ms: int, retries_attempted: int) -> float:
    no_jitter_delay = base_delay_ms * (2.0 ** retries_attempted)
    return random.random() * min(no_jitter_delay, max_delay_ms)


class _SchemaCache(object):
    """
    Thread-safe cache for use with the Schema Registry Client.

    This cache may be used to retrieve schema ids, schemas or to check
    known subject membership.
    """

    def __init__(self):
        self.lock = Lock()
        self.schemaid_index = {}
        self.schema_index = {}
        self.schema_id_index = defaultdict(dict)
        self.schema_index = defaultdict(dict)
        self.rs_id_index = defaultdict(dict)
        self.rs_version_index = defaultdict(dict)
        self.rs_schema_index = defaultdict(dict)

    def set_schema(self, subject: str, schema_id: int, schema: 'Schema'):
        """
        Add a Schema identified by schema_id to the cache.

        Args:
            subject (str): The subject this schema is associated with

            schema_id (int): Schema's registration id

            schema (Schema): Schema instance
        """

        with self.lock:
            self.schema_id_index[subject][schema_id] = schema
            self.schema_index[subject][schema] = schema_id

    def set_registered_schema(self, registered_schema: 'RegisteredSchema'):
        """
        Add a RegisteredSchema to the cache.

        Args:
            registered_schema (RegisteredSchema): RegisteredSchema instance
        """

        subject = registered_schema.subject
        schema_id = registered_schema.schema_id
        version = registered_schema.version
        schema = registered_schema.schema
        with self.lock:
            self.schema_id_index[subject][schema_id] = schema
            self.schema_index[subject][schema] = schema_id
            self.rs_id_index[subject][schema_id] = registered_schema
            self.rs_version_index[subject][version] = registered_schema
            self.rs_schema_index[subject][schema] = registered_schema

    def get_schema_by_id(self, subject: str, schema_id: int) -> Optional['Schema']:
        """
        Get the schema instance associated with schema id from the cache.

        Args:
            subject (str): The subject this schema is associated with

            schema_id (int): Id used to identify a schema

        Returns:
            Schema: The schema if known; else None
        """

        with self.lock:
            return self.schema_id_index.get(subject, {}).get(schema_id, None)

    def get_id_by_schema(self, subject: str, schema: 'Schema') -> Optional[int]:
        """
        Get the schema id associated with schema instance from the cache.

        Args:
            subject (str): The subject this schema is associated with

            schema (Schema): The schema

        Returns:
            int: The schema id if known; else None
        """

        with self.lock:
            return self.schema_index.get(subject, {}).get(schema, None)

    def get_registered_by_subject_schema(self, subject: str, schema: 'Schema') -> Optional['RegisteredSchema']:
        """
        Get the schema associated with this schema registered under subject.

        Args:
            subject (str): The subject this schema is associated with

            schema (Schema): The schema associated with this schema

        Returns:
            RegisteredSchema: The registered schema if known; else None
        """

        with self.lock:
            return self.rs_schema_index.get(subject, {}).get(schema, None)

    def get_registered_by_subject_id(self, subject: str, schema_id: int) -> Optional['RegisteredSchema']:
        """
        Get the schema associated with this id registered under subject.

        Args:
            subject (str): The subject this schema is associated with

            schema_id (int): The schema id associated with this schema

        Returns:
            RegisteredSchema: The registered schema if known; else None
        """

        with self.lock:
            return self.rs_id_index.get(subject, {}).get(schema_id, None)

    def get_registered_by_subject_version(self, subject: str, version: int) -> Optional['RegisteredSchema']:
        """
        Get the schema associated with this version registered under subject.

        Args:
            subject (str): The subject this schema is associated with

            version (int): The version associated with this schema

        Returns:
            RegisteredSchema: The registered schema if known; else None
        """

        with self.lock:
            return self.rs_version_index.get(subject, {}).get(version, None)

    def remove_by_subject(self, subject: str):
        """
        Remove schemas with the given subject.

        Args:
            subject (str): The subject
        """

        with self.lock:
            if subject in self.schema_id_index:
                del self.schema_id_index[subject]
            if subject in self.schema_index:
                del self.schema_index[subject]
            if subject in self.rs_id_index:
                del self.rs_id_index[subject]
            if subject in self.rs_version_index:
                del self.rs_version_index[subject]
            if subject in self.rs_schema_index:
                del self.rs_schema_index[subject]

    def remove_by_subject_version(self, subject: str, version: int):
        """
        Remove schemas with the given subject.

        Args:
            subject (str): The subject

            version (int) The version
        """

        with self.lock:
            if subject in self.rs_id_index:
                for schema_id, registered_schema in self.rs_id_index[subject].items():
                    if registered_schema.version == version:
                        del self.rs_schema_index[subject][schema_id]
            if subject in self.rs_schema_index:
                for schema, registered_schema in self.rs_schema_index[subject].items():
                    if registered_schema.version == version:
                        del self.rs_schema_index[subject][schema]
            rs = None
            if subject in self.rs_version_index:
                if version in self.rs_version_index[subject]:
                    rs = self.rs_version_index[subject][version]
                    del self.rs_version_index[subject][version]
            if rs is not None:
                if subject in self.schema_id_index:
                    if rs.schema_id in self.schema_id_index[subject]:
                        del self.schema_id_index[subject][rs.schema_id]
                    if rs.schema in self.schema_index[subject]:
                        del self.schema_index[subject][rs.schema]

    def clear(self):
        """
        Clear the cache.
        """

        with self.lock:
            self.schema_id_index.clear()
            self.schema_index.clear()
            self.rs_id_index.clear()
            self.rs_version_index.clear()
            self.rs_schema_index.clear()


class SchemaRegistryClient(object):
    """
    A Confluent Schema Registry client.

    Configuration properties (* indicates a required field):

    +------------------------------+------+-------------------------------------------------+
    | Property name                | type | Description                                     |
    +==============================+======+=================================================+
    | ``url`` *                    | str  | Comma-separated list of Schema Registry URLs.   |
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
    |                              |      |                                                 |
    | ``proxy``                    | str  | Proxy such as http://localhost:8030.            |
    |                              |      |                                                 |
    +------------------------------+------+-------------------------------------------------+
    |                              |      |                                                 |
    | ``timeout``                  | int  | Request timeout.                                |
    |                              |      |                                                 |
    +------------------------------+------+-------------------------------------------------+
    |                              |      |                                                 |
    | ``cache.capacity``           | int  | Cache capacity.  Defaults to 1000.              |
    |                              |      |                                                 |
    +------------------------------+------+-------------------------------------------------+
    |                              |      |                                                 |
    | ``cache.latest.ttl.sec``     | int  | TTL in seconds for caching the latest schema.   |
    |                              |      |                                                 |
    +------------------------------+------+-------------------------------------------------+
    |                              |      |                                                 |
    | ``max.retries``              | int  | Maximum retries for a request.  Defaults to 2.  |
    |                              |      |                                                 |
    +------------------------------+------+-------------------------------------------------+
    |                              |      | Maximum time to wait for the first retry.       |
    |                              |      | When jitter is applied, the actual wait may     |
    | ``retries.wait.ms``          | int  | be less.                                        |
    |                              |      |                                                 |
    |                              |      | Defaults to 1000.                               |
    +------------------------------+------+-------------------------------------------------+

    Args:
        conf (dict): Schema Registry client configuration.

    See Also:
        `Confluent Schema Registry documentation <http://confluent.io/docs/current/schema-registry/docs/intro.html>`_
    """  # noqa: E501

    def __init__(self, conf: dict):
        self._conf = conf
        self._rest_client = _RestClient(conf)
        self._cache = _SchemaCache()
        cache_capacity = self._rest_client.cache_capacity
        cache_ttl = self._rest_client.cache_latest_ttl_sec
        if cache_ttl is not None:
            self._latest_version_cache = TTLCache(cache_capacity, cache_ttl)
            self._latest_with_metadata_cache = TTLCache(cache_capacity, cache_ttl)
        else:
            self._latest_version_cache = LRUCache(cache_capacity)
            self._latest_with_metadata_cache = LRUCache(cache_capacity)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        if self._rest_client is not None:
            self._rest_client.session.close()

    def config(self):
        return self._conf

    def register_schema(
        self, subject_name: str, schema: 'Schema',
        normalize_schemas: bool = False
    ) -> int:
        """
        Registers a schema under ``subject_name``.

        Args:
            subject_name (str): subject to register a schema under
            schema (Schema): Schema instance to register
            normalize_schemas (bool): Normalize schema before registering

        Returns:
            int: Schema id

        Raises:
            SchemaRegistryError: if Schema violates this subject's
                Compatibility policy or is otherwise invalid.

        See Also:
            `POST Subject API Reference <https://docs.confluent.io/current/schema-registry/develop/api.html#post--subjects-(string-%20subject)-versions>`_
        """  # noqa: E501

        registered_schema = self.register_schema_full_response(subject_name, schema, normalize_schemas)
        return registered_schema.schema_id

    def register_schema_full_response(
        self, subject_name: str, schema: 'Schema',
        normalize_schemas: bool = False
    ) -> 'RegisteredSchema':
        """
        Registers a schema under ``subject_name``.

        Args:
            subject_name (str): subject to register a schema under
            schema (Schema): Schema instance to register
            normalize_schemas (bool): Normalize schema before registering

        Returns:
            int: Schema id

        Raises:
            SchemaRegistryError: if Schema violates this subject's
                Compatibility policy or is otherwise invalid.

        See Also:
            `POST Subject API Reference <https://docs.confluent.io/current/schema-registry/develop/api.html#post--subjects-(string-%20subject)-versions>`_
        """  # noqa: E501

        schema_id = self._cache.get_id_by_schema(subject_name, schema)
        if schema_id is not None:
            return RegisteredSchema(schema_id, schema, subject_name, None)

        request = schema.to_dict()

        response = self._rest_client.post(
            'subjects/{}/versions?normalize={}'.format(_urlencode(subject_name), normalize_schemas),
            body=request)

        registered_schema = RegisteredSchema.from_dict(response)

        self._cache.set_schema(subject_name, registered_schema.schema_id, registered_schema.schema)

        return registered_schema

    def get_schema(
        self, schema_id: int, subject_name: str = None, fmt: str = None
    ) -> 'Schema':
        """
        Fetches the schema associated with ``schema_id`` from the
        Schema Registry. The result is cached so subsequent attempts will not
        require an additional round-trip to the Schema Registry.

        Args:
            schema_id (int): Schema id
            subject_name (str): Subject name the schema is registered under
            fmt (str): Format of the schema

        Returns:
            Schema: Schema instance identified by the ``schema_id``

        Raises:
            SchemaRegistryError: If schema can't be found.

        See Also:
         `GET Schema API Reference <https://docs.confluent.io/current/schema-registry/develop/api.html#get--schemas-ids-int-%20id>`_
        """  # noqa: E501

        schema = self._cache.get_schema_by_id(subject_name, schema_id)
        if schema is not None:
            return schema

        query = {'subject': subject_name} if subject_name is not None else None
        if fmt is not None:
            if query is not None:
                query['format'] = fmt
            else:
                query = {'format': fmt}
        response = self._rest_client.get('schemas/ids/{}'.format(schema_id), query)

        schema = Schema.from_dict(response)

        self._cache.set_schema(subject_name, schema_id, schema)

        return schema

    def lookup_schema(
        self, subject_name: str, schema: 'Schema',
        normalize_schemas: bool = False, deleted: bool = False
    ) -> 'RegisteredSchema':
        """
        Returns ``schema`` registration information for ``subject``.

        Args:
            subject_name (str): Subject name the schema is registered under
            schema (Schema): Schema instance.
            normalize_schemas (bool): Normalize schema before registering
            deleted (bool): Whether to include deleted schemas.

        Returns:
            RegisteredSchema: Subject registration information for this schema.

        Raises:
            SchemaRegistryError: If schema or subject can't be found

        See Also:
            `POST Subject API Reference <https://docs.confluent.io/current/schema-registry/develop/api.html#post--subjects-(string-%20subject)-versions>`_
        """  # noqa: E501

        registered_schema = self._cache.get_registered_by_subject_schema(subject_name, schema)
        if registered_schema is not None:
            return registered_schema

        request = schema.to_dict()

        response = self._rest_client.post('subjects/{}?normalize={}&deleted={}'
                                          .format(_urlencode(subject_name), normalize_schemas, deleted),
                                          body=request)

        registered_schema = RegisteredSchema.from_dict(response)

        self._cache.set_registered_schema(registered_schema)

        return registered_schema

    def get_subjects(self) -> List[str]:
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

    def delete_subject(self, subject_name: str, permanent: bool = False) -> List[int]:
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

        if permanent:
            versions = self._rest_client.delete('subjects/{}?permanent=true'
                                                .format(_urlencode(subject_name)))
            self._cache.remove_by_subject(subject_name)
        else:
            versions = self._rest_client.delete('subjects/{}'
                                                .format(_urlencode(subject_name)))

        return versions

    def get_latest_version(
        self, subject_name: str, fmt: str = None
    ) -> 'RegisteredSchema':
        """
        Retrieves latest registered version for subject

        Args:
            subject_name (str): Subject name.
            fmt (str): Format of the schema

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

        query = {'format': fmt} if fmt is not None else None
        response = self._rest_client.get('subjects/{}/versions/{}'
                                         .format(_urlencode(subject_name),
                                                 'latest'), query)

        registered_schema = RegisteredSchema.from_dict(response)

        self._latest_version_cache[subject_name] = registered_schema

        return registered_schema

    def get_latest_with_metadata(
        self, subject_name: str, metadata: Dict[str, str],
        deleted: bool = False, fmt: str = None
    ) -> 'RegisteredSchema':
        """
        Retrieves latest registered version for subject with the given metadata

        Args:
            subject_name (str): Subject name.
            metadata (dict): The key-value pairs for the metadata.
            deleted (bool): Whether to include deleted schemas.
            fmt (str): Format of the schema

        Returns:
            RegisteredSchema: Registration information for this version.

        Raises:
            SchemaRegistryError: if the version can't be found or is invalid.
        """  # noqa: E501

        cache_key = (subject_name, frozenset(metadata.items()), deleted)
        registered_schema = self._latest_with_metadata_cache.get(cache_key, None)
        if registered_schema is not None:
            return registered_schema

        query = {'deleted': deleted, 'format': fmt} if fmt is not None else {'deleted': deleted}
        keys = metadata.keys()
        if keys:
            query['key'] = [_urlencode(key) for key in keys]
            query['value'] = [_urlencode(metadata[key]) for key in keys]

        response = self._rest_client.get('subjects/{}/metadata'
                                         .format(_urlencode(subject_name)), query)

        registered_schema = RegisteredSchema.from_dict(response)

        self._latest_with_metadata_cache[cache_key] = registered_schema

        return registered_schema

    def get_version(
        self, subject_name: str, version: int,
        deleted: bool = False, fmt: str = None
    ) -> 'RegisteredSchema':
        """
        Retrieves a specific schema registered under ``subject_name``.

        Args:
            subject_name (str): Subject name.
            version (int): version number. Defaults to latest version.
            deleted (bool): Whether to include deleted schemas.
            fmt (str): Format of the schema

        Returns:
            RegisteredSchema: Registration information for this version.

        Raises:
            SchemaRegistryError: if the version can't be found or is invalid.

        See Also:
            `GET Subject Version API Reference <https://docs.confluent.io/current/schema-registry/develop/api.html#get--subjects-(string-%20subject)-versions-(versionId-%20version)>`_
        """  # noqa: E501

        registered_schema = self._cache.get_registered_by_subject_version(subject_name, version)
        if registered_schema is not None:
            return registered_schema

        query = {'deleted': deleted, 'format': fmt} if fmt is not None else {'deleted': deleted}
        response = self._rest_client.get('subjects/{}/versions/{}'
                                         .format(_urlencode(subject_name),
                                                 version), query)

        registered_schema = RegisteredSchema.from_dict(response)

        self._cache.set_registered_schema(registered_schema)

        return registered_schema

    def get_versions(self, subject_name: str) -> List[int]:
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

    def delete_version(self, subject_name: str, version: int, permanent: bool = False) -> int:
        """
        Deletes a specific version registered to ``subject_name``.

        Args:
            subject_name (str) Subject name

            version (int): Version number

            permanent (bool): True for a hard delete, False (default) for a soft delete

        Returns:
            int: Version number which was deleted

        Raises:
            SchemaRegistryError: if the subject or version cannot be found.

        See Also:
            `Delete Subject Version API Reference <https://docs.confluent.io/current/schema-registry/develop/api.html#delete--subjects-(string-%20subject)-versions-(versionId-%20version)>`_
        """  # noqa: E501

        if permanent:
            response = self._rest_client.delete('subjects/{}/versions/{}?permanent=true'
                                                .format(_urlencode(subject_name),
                                                        version))
            self._cache.remove_by_subject_version(subject_name, version)
        else:
            response = self._rest_client.delete('subjects/{}/versions/{}'
                                                .format(_urlencode(subject_name),
                                                        version))

        return response

    def set_compatibility(self, subject_name: Optional[str] = None, level: str = None) -> str:
        """
        Update global or subject level compatibility level.

        Args:
            level (str): Compatibility level. See API reference for a list of
                valid values.

            subject_name (str, optional): Subject to update. Sets global compatibility
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

    def get_compatibility(self, subject_name: Optional[str] = None) -> str:
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

    def test_compatibility(
        self, subject_name: str, schema: 'Schema',
        version: Union[int, str] = "latest"
    ) -> bool:
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

        request = schema.to_dict()

        response = self._rest_client.post(
            'compatibility/subjects/{}/versions/{}'.format(_urlencode(subject_name), version), body=request
        )

        return response['is_compatible']

    def set_config(self, subject_name: Optional[str] = None, config: 'ServerConfig' = None) -> 'ServerConfig':
        """
        Update global or subject config.

        Args:
            config (ServerConfig): Config. See API reference for a list of
                valid values.

            subject_name (str, optional): Subject to update. Sets global config
                if not set.

        Returns:
            str: The newly configured config.

        Raises:
            SchemaRegistryError: If the config is invalid.

        See Also:
            `PUT Subject Config API Reference <https://docs.confluent.io/current/schema-registry/develop/api.html#put--config-(string-%20subject)>`_
        """  # noqa: E501

        if config is None:
            raise ValueError("config must be set")

        if subject_name is None:
            return self._rest_client.put('config',
                                         body=config.to_dict())

        return self._rest_client.put('config/{}'
                                     .format(_urlencode(subject_name)),
                                     body=config.to_dict())

    def get_config(self, subject_name: Optional[str] = None) -> 'ServerConfig':
        """
        Get the current config.

        Args:
            subject_name (str, optional): Subject name. Returns global config
                if left unset.

        Returns:
            ServerConfig: Config for the subject if set, otherwise the global config.

        Raises:
            SchemaRegistryError: if the request was unsuccessful.

        See Also:
            `GET Subject Config API Reference <https://docs.confluent.io/current/schema-registry/develop/api.html#get--config-(string-%20subject)>`_
        """  # noqa: E501

        if subject_name is not None:
            url = 'config/{}'.format(_urlencode(subject_name))
        else:
            url = 'config'

        result = self._rest_client.get(url)
        return ServerConfig.from_dict(result)

    def clear_latest_caches(self):
        self._latest_version_cache.clear()
        self._latest_with_metadata_cache.clear()

    def clear_caches(self):
        self._latest_version_cache.clear()
        self._latest_with_metadata_cache.clear()
        self._cache.clear()

    @staticmethod
    def new_client(conf: dict) -> 'SchemaRegistryClient':
        from .mock_schema_registry_client import MockSchemaRegistryClient
        url = conf.get("url")
        if url.startswith("mock://"):
            return MockSchemaRegistryClient(conf)
        return SchemaRegistryClient(conf)


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
    params: Dict[str, str] = _attrs_field(factory=dict, hash=False)

    def to_dict(self) -> Dict[str, Any]:
        field_dict: Dict[str, Any] = {}
        field_dict.update(self.params)

        return field_dict

    @classmethod
    def from_dict(cls: Type[T], src_dict: Dict[str, Any]) -> T:
        d = src_dict.copy()

        rule_params = cls(params=d)

        return rule_params

    def __hash__(self):
        return hash(frozenset(self.params.items()))


@_attrs_define(frozen=True)
class Rule:
    name: Optional[str]
    doc: Optional[str]
    kind: Optional[RuleKind]
    mode: Optional[RuleMode]
    type: Optional[str]
    tags: Optional[List[str]] = _attrs_field(hash=False)
    params: Optional[RuleParams]
    expr: Optional[str]
    on_success: Optional[str]
    on_failure: Optional[str]
    disabled: Optional[bool]

    def to_dict(self) -> Dict[str, Any]:
        name = self.name

        doc = self.doc

        kind_str: Optional[str] = None
        if self.kind is not None:
            kind_str = self.kind.value

        mode_str: Optional[str] = None
        if self.mode is not None:
            mode_str = self.mode.value

        rule_type = self.type

        tags = self.tags

        _params: Optional[Dict[str, Any]] = None
        if self.params is not None:
            _params = self.params.to_dict()

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
        if kind_str is not None:
            field_dict["kind"] = kind_str
        if mode_str is not None:
            field_dict["mode"] = mode_str
        if type is not None:
            field_dict["type"] = rule_type
        if tags is not None:
            field_dict["tags"] = tags
        if _params is not None:
            field_dict["params"] = _params
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

        rule_type = d.pop("type", None)

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
            type=rule_type,
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
    migration_rules: Optional[List["Rule"]] = _attrs_field(hash=False)
    domain_rules: Optional[List["Rule"]] = _attrs_field(hash=False)

    def to_dict(self) -> Dict[str, Any]:
        _migration_rules: Optional[List[Dict[str, Any]]] = None
        if self.migration_rules is not None:
            _migration_rules = []
            for migration_rules_item_data in self.migration_rules:
                migration_rules_item = migration_rules_item_data.to_dict()
                _migration_rules.append(migration_rules_item)

        _domain_rules: Optional[List[Dict[str, Any]]] = None
        if self.domain_rules is not None:
            _domain_rules = []
            for domain_rules_item_data in self.domain_rules:
                domain_rules_item = domain_rules_item_data.to_dict()
                _domain_rules.append(domain_rules_item)

        field_dict: Dict[str, Any] = {}
        field_dict.update({})
        if _migration_rules is not None:
            field_dict["migrationRules"] = _migration_rules
        if _domain_rules is not None:
            field_dict["domainRules"] = _domain_rules

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

    def __hash__(self):
        return hash(frozenset((self.migration_rules or []) + (self.domain_rules or [])))


@_attrs_define
class MetadataTags:
    tags: Dict[str, List[str]] = _attrs_field(factory=dict, hash=False)

    def to_dict(self) -> Dict[str, Any]:
        field_dict: Dict[str, Any] = {}
        for prop_name, prop in self.tags.items():
            field_dict[prop_name] = prop

        return field_dict

    @classmethod
    def from_dict(cls: Type[T], src_dict: Dict[str, Any]) -> T:
        d = src_dict.copy()

        tags = {}
        for prop_name, prop_dict in d.items():
            tag = cast(List[str], prop_dict)

            tags[prop_name] = tag

        metadata_tags = cls(tags=tags)

        return metadata_tags

    def __hash__(self):
        return hash(frozenset(self.tags.items()))


@_attrs_define
class MetadataProperties:
    properties: Dict[str, str] = _attrs_field(factory=dict, hash=False)

    def to_dict(self) -> Dict[str, Any]:
        field_dict: Dict[str, Any] = {}
        field_dict.update(self.properties)

        return field_dict

    @classmethod
    def from_dict(cls: Type[T], src_dict: Dict[str, Any]) -> T:
        d = src_dict.copy()

        metadata_properties = cls(properties=d)

        return metadata_properties

    def __hash__(self):
        return hash(frozenset(self.properties.items()))


@_attrs_define(frozen=True)
class Metadata:
    tags: Optional[MetadataTags]
    properties: Optional[MetadataProperties]
    sensitive: Optional[List[str]] = _attrs_field(hash=False)

    def to_dict(self) -> Dict[str, Any]:
        _tags: Optional[Dict[str, Any]] = None
        if self.tags is not None:
            _tags = self.tags.to_dict()

        _properties: Optional[Dict[str, Any]] = None
        if self.properties is not None:
            _properties = self.properties.to_dict()

        sensitive: Optional[List[str]] = None
        if self.sensitive is not None:
            sensitive = []
            for sensitive_item in self.sensitive:
                sensitive.append(sensitive_item)

        field_dict: Dict[str, Any] = {}
        if _tags is not None:
            field_dict["tags"] = _tags
        if _properties is not None:
            field_dict["properties"] = _properties
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


@_attrs_define(frozen=True)
class SchemaReference:
    name: Optional[str]
    subject: Optional[str]
    version: Optional[int]

    def to_dict(self) -> Dict[str, Any]:
        name = self.name

        subject = self.subject

        version = self.version

        field_dict: Dict[str, Any] = {}
        if name is not None:
            field_dict["name"] = name
        if subject is not None:
            field_dict["subject"] = subject
        if version is not None:
            field_dict["version"] = version

        return field_dict

    @classmethod
    def from_dict(cls: Type[T], src_dict: Dict[str, Any]) -> T:
        d = src_dict.copy()
        name = d.pop("name", None)

        subject = d.pop("subject", None)

        version = d.pop("version", None)

        schema_reference = cls(
            name=name,
            subject=subject,
            version=version,
        )

        return schema_reference


class ConfigCompatibilityLevel(str, Enum):
    BACKWARD = "BACKWARD"
    BACKWARD_TRANSITIVE = "BACKWARD_TRANSITIVE"
    FORWARD = "FORWARD"
    FORWARD_TRANSITIVE = "FORWARD_TRANSITIVE"
    FULL = "FULL"
    FULL_TRANSITIVE = "FULL_TRANSITIVE"
    NONE = "NONE"

    def __str__(self) -> str:
        return str(self.value)


@_attrs_define
class ServerConfig:
    compatibility: Optional[ConfigCompatibilityLevel] = None
    compatibility_level: Optional[ConfigCompatibilityLevel] = None
    compatibility_group: Optional[str] = None
    default_metadata: Optional[Metadata] = None
    override_metadata: Optional[Metadata] = None
    default_rule_set: Optional[RuleSet] = None
    override_rule_set: Optional[RuleSet] = None

    def to_dict(self) -> Dict[str, Any]:
        _compatibility: Optional[str] = None
        if self.compatibility is not None:
            _compatibility = self.compatibility.value

        _compatibility_level: Optional[str] = None
        if self.compatibility_level is not None:
            _compatibility_level = self.compatibility_level.value

        compatibility_group = self.compatibility_group

        _default_metadata: Optional[Dict[str, Any]]
        if isinstance(self.default_metadata, Metadata):
            _default_metadata = self.default_metadata.to_dict()
        else:
            _default_metadata = self.default_metadata

        _override_metadata: Optional[Dict[str, Any]]
        if isinstance(self.override_metadata, Metadata):
            _override_metadata = self.override_metadata.to_dict()
        else:
            _override_metadata = self.override_metadata

        _default_rule_set: Optional[Dict[str, Any]]
        if isinstance(self.default_rule_set, RuleSet):
            _default_rule_set = self.default_rule_set.to_dict()
        else:
            _default_rule_set = self.default_rule_set

        _override_rule_set: Optional[Dict[str, Any]]
        if isinstance(self.override_rule_set, RuleSet):
            _override_rule_set = self.override_rule_set.to_dict()
        else:
            _override_rule_set = self.override_rule_set

        field_dict: Dict[str, Any] = {}
        if _compatibility is not None:
            field_dict["compatibility"] = _compatibility
        if _compatibility_level is not None:
            field_dict["compatibilityLevel"] = _compatibility_level
        if compatibility_group is not None:
            field_dict["compatibilityGroup"] = compatibility_group
        if _default_metadata is not None:
            field_dict["defaultMetadata"] = _default_metadata
        if _override_metadata is not None:
            field_dict["overrideMetadata"] = _override_metadata
        if _default_rule_set is not None:
            field_dict["defaultRuleSet"] = _default_rule_set
        if _override_rule_set is not None:
            field_dict["overrideRuleSet"] = _override_rule_set

        return field_dict

    @classmethod
    def from_dict(cls: Type[T], src_dict: Dict[str, Any]) -> T:
        d = src_dict.copy()
        _compatibility = d.pop("compatibility", None)
        compatibility: Optional[ConfigCompatibilityLevel]
        if _compatibility is None:
            compatibility = None
        else:
            compatibility = ConfigCompatibilityLevel(_compatibility)

        _compatibility_level = d.pop("compatibilityLevel", None)
        compatibility_level: Optional[ConfigCompatibilityLevel]
        if _compatibility_level is None:
            compatibility_level = None
        else:
            compatibility_level = ConfigCompatibilityLevel(_compatibility_level)

        compatibility_group = d.pop("compatibilityGroup", None)

        def _parse_default_metadata(data: object) -> Optional[Metadata]:
            if data is None:
                return data
            if not isinstance(data, dict):
                raise TypeError()
            return Metadata.from_dict(data)

        default_metadata = _parse_default_metadata(d.pop("defaultMetadata", None))

        def _parse_override_metadata(data: object) -> Optional[Metadata]:
            if data is None:
                return data
            if not isinstance(data, dict):
                raise TypeError()
            return Metadata.from_dict(data)

        override_metadata = _parse_override_metadata(d.pop("overrideMetadata", None))

        def _parse_default_rule_set(data: object) -> Optional[RuleSet]:
            if data is None:
                return data
            if not isinstance(data, dict):
                raise TypeError()
            return RuleSet.from_dict(data)

        default_rule_set = _parse_default_rule_set(d.pop("defaultRuleSet", None))

        def _parse_override_rule_set(data: object) -> Optional[RuleSet]:
            if data is None:
                return data
            if not isinstance(data, dict):
                raise TypeError()
            return RuleSet.from_dict(data)

        override_rule_set = _parse_override_rule_set(d.pop("overrideRuleSet", None))

        config = cls(
            compatibility=compatibility,
            compatibility_level=compatibility_level,
            compatibility_group=compatibility_group,
            default_metadata=default_metadata,
            override_metadata=override_metadata,
            default_rule_set=default_rule_set,
            override_rule_set=override_rule_set,
        )

        return config


@_attrs_define(frozen=True, cache_hash=True)
class Schema:
    """
    An unregistered schema.
    """

    schema_str: Optional[str]
    schema_type: Optional[str] = "AVRO"
    references: Optional[List[SchemaReference]] = _attrs_field(factory=list, hash=False)
    metadata: Optional[Metadata] = None
    rule_set: Optional[RuleSet] = None

    def to_dict(self) -> Dict[str, Any]:
        schema = self.schema_str

        schema_type = self.schema_type

        _references: Optional[List[Dict[str, Any]]] = []
        if self.references is not None:
            for references_item_data in self.references:
                references_item = references_item_data.to_dict()
                _references.append(references_item)

        _metadata: Optional[Dict[str, Any]] = None
        if isinstance(self.metadata, Metadata):
            _metadata = self.metadata.to_dict()

        _rule_set: Optional[Dict[str, Any]] = None
        if isinstance(self.rule_set, RuleSet):
            _rule_set = self.rule_set.to_dict()

        field_dict: Dict[str, Any] = {}
        if schema is not None:
            field_dict["schema"] = schema
        if schema_type is not None:
            field_dict["schemaType"] = schema_type
        if _references is not None:
            field_dict["references"] = _references
        if _metadata is not None:
            field_dict["metadata"] = _metadata
        if _rule_set is not None:
            field_dict["ruleSet"] = _rule_set

        return field_dict

    @classmethod
    def from_dict(cls: Type[T], src_dict: Dict[str, Any]) -> T:
        d = src_dict.copy()

        schema = d.pop("schema", None)

        schema_type = d.pop("schemaType", "AVRO")

        references = []
        _references = d.pop("references", None)
        for references_item_data in _references or []:
            references_item = SchemaReference.from_dict(references_item_data)

            references.append(references_item)

        def _parse_metadata(data: object) -> Optional[Metadata]:
            if data is None:
                return data
            if not isinstance(data, dict):
                raise TypeError()
            return Metadata.from_dict(data)

        metadata = _parse_metadata(d.pop("metadata", None))

        def _parse_rule_set(data: object) -> Optional[RuleSet]:
            if data is None:
                return data
            if not isinstance(data, dict):
                raise TypeError()
            return RuleSet.from_dict(data)

        rule_set = _parse_rule_set(d.pop("ruleSet", None))

        schema = cls(
            schema_str=schema,
            schema_type=schema_type,
            references=references,
            metadata=metadata,
            rule_set=rule_set,
        )

        return schema


@_attrs_define(frozen=True, cache_hash=True)
class RegisteredSchema:
    """
    An registered schema.
    """

    schema_id: Optional[int]
    schema: Optional[Schema]
    subject: Optional[str]
    version: Optional[int]

    def to_dict(self) -> Dict[str, Any]:
        schema = self.schema

        schema_id = self.schema_id

        subject = self.subject

        version = self.version

        field_dict: Dict[str, Any] = {}
        if schema is not None:
            field_dict = schema.to_dict()
        if schema_id is not None:
            field_dict["id"] = schema_id
        if subject is not None:
            field_dict["subject"] = subject
        if version is not None:
            field_dict["version"] = version

        return field_dict

    @classmethod
    def from_dict(cls: Type[T], src_dict: Dict[str, Any]) -> T:
        d = src_dict.copy()

        schema = Schema.from_dict(d)

        schema_id = d.pop("id", None)

        subject = d.pop("subject", None)

        version = d.pop("version", None)

        schema = cls(
            schema_id=schema_id,
            schema=schema,
            subject=subject,
            version=version,
        )

        return schema
