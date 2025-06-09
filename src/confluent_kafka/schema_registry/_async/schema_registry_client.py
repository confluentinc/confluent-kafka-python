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
import asyncio
import json
import logging
import time
import urllib
from urllib.parse import unquote, urlparse

import httpx
from typing import List, Dict, Optional, Union, Any, Tuple, Callable

from cachetools import TTLCache, LRUCache
from httpx import Response

from authlib.integrations.httpx_client import AsyncOAuth2Client

from confluent_kafka.schema_registry.error import SchemaRegistryError, OAuthTokenError
from confluent_kafka.schema_registry.common.schema_registry_client import (
    RegisteredSchema,
    ServerConfig,
    is_success,
    is_retriable,
    _BearerFieldProvider,
    full_jitter,
    _SchemaCache,
    Schema,
    _StaticFieldProvider,
)

__all__ = [
    '_urlencode',
    '_AsyncCustomOAuthClient',
    '_AsyncOAuthClient',
    '_AsyncBaseRestClient',
    '_AsyncRestClient',
    'AsyncSchemaRegistryClient',
]

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


class _AsyncCustomOAuthClient(_BearerFieldProvider):
    def __init__(self, custom_function: Callable[[Dict], Dict], custom_config: dict):
        self.custom_function = custom_function
        self.custom_config = custom_config

    async def get_bearer_fields(self) -> dict:
        return await self.custom_function(self.custom_config)


class _AsyncOAuthClient(_BearerFieldProvider):
    def __init__(self, client_id: str, client_secret: str, scope: str, token_endpoint: str, logical_cluster: str,
                 identity_pool: str, max_retries: int, retries_wait_ms: int, retries_max_wait_ms: int):
        self.token = None
        self.logical_cluster = logical_cluster
        self.identity_pool = identity_pool
        self.client = AsyncOAuth2Client(client_id=client_id, client_secret=client_secret, scope=scope)
        self.token_endpoint = token_endpoint
        self.max_retries = max_retries
        self.retries_wait_ms = retries_wait_ms
        self.retries_max_wait_ms = retries_max_wait_ms
        self.token_expiry_threshold = 0.8

    async def get_bearer_fields(self) -> dict:
        return {
            'bearer.auth.token': await self.get_access_token(),
            'bearer.auth.logical.cluster': self.logical_cluster,
            'bearer.auth.identity.pool.id': self.identity_pool
        }

    def token_expired(self) -> bool:
        expiry_window = self.token['expires_in'] * self.token_expiry_threshold

        return self.token['expires_at'] < time.time() + expiry_window

    async def get_access_token(self) -> str:
        if not self.token or self.token_expired():
            await self.generate_access_token()

        return self.token['access_token']

    async def generate_access_token(self) -> None:
        for i in range(self.max_retries + 1):
            try:
                self.token = await self.client.fetch_token(url=self.token_endpoint, grant_type='client_credentials')
                return
            except Exception as e:
                if i >= self.max_retries:
                    raise OAuthTokenError(f"Failed to retrieve token after {self.max_retries} "
                                          f"attempts due to error: {str(e)}")
                await asyncio.sleep(full_jitter(self.retries_wait_ms, self.retries_max_wait_ms, i) / 1000)


class _AsyncBaseRestClient(object):

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
            if not isinstance(max_retries, (int, float)):
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

        self.bearer_field_provider = None
        logical_cluster = None
        identity_pool = None
        self.bearer_auth_credentials_source = conf_copy.pop('bearer.auth.credentials.source', None)
        if self.bearer_auth_credentials_source is not None:
            self.auth = None

            if self.bearer_auth_credentials_source in {'OAUTHBEARER', 'STATIC_TOKEN'}:
                headers = ['bearer.auth.logical.cluster', 'bearer.auth.identity.pool.id']
                missing_headers = [header for header in headers if header not in conf_copy]
                if missing_headers:
                    raise ValueError("Missing required bearer configuration properties: {}"
                                     .format(", ".join(missing_headers)))

                logical_cluster = conf_copy.pop('bearer.auth.logical.cluster')
                if not isinstance(logical_cluster, str):
                    raise TypeError("logical cluster must be a str, not " + str(type(logical_cluster)))

                identity_pool = conf_copy.pop('bearer.auth.identity.pool.id')
                if not isinstance(identity_pool, str):
                    raise TypeError("identity pool id must be a str, not " + str(type(identity_pool)))

            if self.bearer_auth_credentials_source == 'OAUTHBEARER':
                properties_list = ['bearer.auth.client.id', 'bearer.auth.client.secret', 'bearer.auth.scope',
                                   'bearer.auth.issuer.endpoint.url']
                missing_properties = [prop for prop in properties_list if prop not in conf_copy]
                if missing_properties:
                    raise ValueError("Missing required OAuth configuration properties: {}".
                                     format(", ".join(missing_properties)))

                self.client_id = conf_copy.pop('bearer.auth.client.id')
                if not isinstance(self.client_id, string_type):
                    raise TypeError("bearer.auth.client.id must be a str, not " + str(type(self.client_id)))

                self.client_secret = conf_copy.pop('bearer.auth.client.secret')
                if not isinstance(self.client_secret, string_type):
                    raise TypeError("bearer.auth.client.secret must be a str, not " + str(type(self.client_secret)))

                self.scope = conf_copy.pop('bearer.auth.scope')
                if not isinstance(self.scope, string_type):
                    raise TypeError("bearer.auth.scope must be a str, not " + str(type(self.scope)))

                self.token_endpoint = conf_copy.pop('bearer.auth.issuer.endpoint.url')
                if not isinstance(self.token_endpoint, string_type):
                    raise TypeError("bearer.auth.issuer.endpoint.url must be a str, not "
                                    + str(type(self.token_endpoint)))

                self.bearer_field_provider = _AsyncOAuthClient(
                    self.client_id, self.client_secret, self.scope,
                    self.token_endpoint, logical_cluster, identity_pool,
                    self.max_retries, self.retries_wait_ms,
                    self.retries_max_wait_ms)
            elif self.bearer_auth_credentials_source == 'STATIC_TOKEN':
                if 'bearer.auth.token' not in conf_copy:
                    raise ValueError("Missing bearer.auth.token")
                static_token = conf_copy.pop('bearer.auth.token')
                self.bearer_field_provider = _StaticFieldProvider(static_token, logical_cluster, identity_pool)
                if not isinstance(static_token, string_type):
                    raise TypeError("bearer.auth.token must be a str, not " + str(type(static_token)))
            elif self.bearer_auth_credentials_source == 'CUSTOM':
                custom_bearer_properties = ['bearer.auth.custom.provider.function',
                                            'bearer.auth.custom.provider.config']
                missing_custom_properties = [prop for prop in custom_bearer_properties if prop not in conf_copy]
                if missing_custom_properties:
                    raise ValueError("Missing required custom OAuth configuration properties: {}".
                                     format(", ".join(missing_custom_properties)))

                custom_function = conf_copy.pop('bearer.auth.custom.provider.function')
                if not callable(custom_function):
                    raise TypeError("bearer.auth.custom.provider.function must be a callable, not "
                                    + str(type(custom_function)))

                custom_config = conf_copy.pop('bearer.auth.custom.provider.config')
                if not isinstance(custom_config, dict):
                    raise TypeError("bearer.auth.custom.provider.config must be a dict, not "
                                    + str(type(custom_config)))

                self.bearer_field_provider = _AsyncCustomOAuthClient(custom_function, custom_config)
            else:
                raise ValueError('Unrecognized bearer.auth.credentials.source')

        # Any leftover keys are unknown to _RestClient
        if len(conf_copy) > 0:
            raise ValueError("Unrecognized properties: {}"
                             .format(", ".join(conf_copy.keys())))

    async def get(self, url: str, query: Optional[dict] = None) -> Any:
        raise NotImplementedError()

    async def post(self, url: str, body: Optional[dict], **kwargs) -> Any:
        raise NotImplementedError()

    async def delete(self, url: str) -> Any:
        raise NotImplementedError()

    async def put(self, url: str, body: Optional[dict] = None) -> Any:
        raise NotImplementedError()


class _AsyncRestClient(_AsyncBaseRestClient):
    """
    HTTP client for Confluent Schema Registry.

    See SchemaRegistryClient for configuration details.

    Args:
        conf (dict): Dictionary containing _RestClient configuration
    """

    def __init__(self, conf: dict):
        super().__init__(conf)

        self.session = httpx.AsyncClient(
            verify=self.verify,
            cert=self.cert,
            auth=self.auth,
            proxy=self.proxy,
            timeout=self.timeout
        )

    async def handle_bearer_auth(self, headers: dict) -> None:
        bearer_fields = await self.bearer_field_provider.get_bearer_fields()
        required_fields = ['bearer.auth.token', 'bearer.auth.identity.pool.id', 'bearer.auth.logical.cluster']

        missing_fields = []
        for field in required_fields:
            if field not in bearer_fields:
                missing_fields.append(field)

        if missing_fields:
            raise ValueError("Missing required bearer auth fields, needs to be set in config or custom function: {}"
                             .format(", ".join(missing_fields)))

        headers["Authorization"] = "Bearer {}".format(bearer_fields['bearer.auth.token'])
        headers['Confluent-Identity-Pool-Id'] = bearer_fields['bearer.auth.identity.pool.id']
        headers['target-sr-cluster'] = bearer_fields['bearer.auth.logical.cluster']

    async def get(self, url: str, query: Optional[dict] = None) -> Any:
        return await self.send_request(url, method='GET', query=query)

    async def post(self, url: str, body: Optional[dict], **kwargs) -> Any:
        return await self.send_request(url, method='POST', body=body)

    async def delete(self, url: str) -> Any:
        return await self.send_request(url, method='DELETE')

    async def put(self, url: str, body: Optional[dict] = None) -> Any:
        return await self.send_request(url, method='PUT', body=body)

    async def send_request(
        self, url: str, method: str, body: Optional[dict] = None,
        query: Optional[dict] = None
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

        if self.bearer_auth_credentials_source:
            await self.handle_bearer_auth(headers)

        response = None
        for i, base_url in enumerate(self.base_urls):
            try:
                response = await self.send_http_request(
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

    async def send_http_request(
        self, base_url: str, url: str, method: str, headers: Optional[dict],
        body: Optional[str] = None, query: Optional[dict] = None
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
        response = None
        for i in range(self.max_retries + 1):
            response = await self.session.request(
                method, url="/".join([base_url, url]),
                headers=headers, content=body, params=query)

            if is_success(response.status_code):
                return response

            if not is_retriable(response.status_code) or i >= self.max_retries:
                return response

            await asyncio.sleep(full_jitter(self.retries_wait_ms, self.retries_max_wait_ms, i) / 1000)
        return response


class AsyncSchemaRegistryClient(object):
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
        self._rest_client = _AsyncRestClient(conf)
        self._cache = _SchemaCache()
        cache_capacity = self._rest_client.cache_capacity
        cache_ttl = self._rest_client.cache_latest_ttl_sec
        if cache_ttl is not None:
            self._latest_version_cache = TTLCache(cache_capacity, cache_ttl)
            self._latest_with_metadata_cache = TTLCache(cache_capacity, cache_ttl)
        else:
            self._latest_version_cache = LRUCache(cache_capacity)
            self._latest_with_metadata_cache = LRUCache(cache_capacity)

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        if self._rest_client is not None:
            await self._rest_client.session.aclose()

    def config(self):
        return self._conf

    async def register_schema(
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

        registered_schema = await self.register_schema_full_response(subject_name, schema, normalize_schemas)
        return registered_schema.schema_id

    async def register_schema_full_response(
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
            result = self._cache.get_schema_by_id(subject_name, schema_id)
            if result is not None:
                return RegisteredSchema(schema_id, result[0], result[1], subject_name, None)

        request = schema.to_dict()

        response = await self._rest_client.post(
            'subjects/{}/versions?normalize={}'.format(_urlencode(subject_name), normalize_schemas),
            body=request)

        registered_schema = RegisteredSchema.from_dict(response)

        # The registered schema may not be fully populated
        s = registered_schema.schema if registered_schema.schema.schema_str is not None else schema
        self._cache.set_schema(subject_name, registered_schema.schema_id,
                               registered_schema.guid, s)

        return registered_schema

    async def get_schema(
        self, schema_id: int, subject_name: Optional[str] = None, fmt: Optional[str] = None
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

        result = self._cache.get_schema_by_id(subject_name, schema_id)
        if result is not None:
            return result[1]

        query = {'subject': subject_name} if subject_name is not None else None
        if fmt is not None:
            if query is not None:
                query['format'] = fmt
            else:
                query = {'format': fmt}
        response = await self._rest_client.get('schemas/ids/{}'.format(schema_id), query)

        registered_schema = RegisteredSchema.from_dict(response)

        self._cache.set_schema(subject_name, schema_id,
                               registered_schema.guid, registered_schema.schema)

        return registered_schema.schema

    async def get_schema_by_guid(
        self, guid: str, fmt: Optional[str] = None
    ) -> 'Schema':
        """
        Fetches the schema associated with ``guid`` from the
        Schema Registry. The result is cached so subsequent attempts will not
        require an additional round-trip to the Schema Registry.

        Args:
            guid (str): Schema guid
            fmt (str): Format of the schema

        Returns:
            Schema: Schema instance identified by the ``guid``

        Raises:
            SchemaRegistryError: If schema can't be found.

        See Also:
         `GET Schema API Reference <https://docs.confluent.io/current/schema-registry/develop/api.html#get--schemas-ids-int-%20id>`_
        """  # noqa: E501

        schema = self._cache.get_schema_by_guid(guid)
        if schema is not None:
            return schema

        if fmt is not None:
            query = {'format': fmt}
        response = await self._rest_client.get('schemas/guids/{}'.format(guid), query)

        registered_schema = RegisteredSchema.from_dict(response)

        self._cache.set_schema(None, registered_schema.schema_id,
                               registered_schema.guid, registered_schema.schema)

        return registered_schema.schema

    async def lookup_schema(
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

        response = await self._rest_client.post(
            'subjects/{}?normalize={}&deleted={}'.format(
                _urlencode(subject_name), normalize_schemas, deleted),
            body=request
        )

        result = RegisteredSchema.from_dict(response)

        # Ensure the schema matches the input
        registered_schema = RegisteredSchema(
            schema_id=result.schema_id,
            guid=result.guid,
            subject=result.subject,
            version=result.version,
            schema=schema,
        )

        self._cache.set_registered_schema(schema, registered_schema)

        return registered_schema

    async def get_subjects(self) -> List[str]:
        """
        List all subjects registered with the Schema Registry

        Returns:
            list(str): Registered subject names

        Raises:
            SchemaRegistryError: if subjects can't be found

        See Also:
            `GET subjects API Reference <https://docs.confluent.io/current/schema-registry/develop/api.html#get--subjects-(string-%20subject)-versions>`_
        """  # noqa: E501

        return await self._rest_client.get('subjects')

    async def delete_subject(self, subject_name: str, permanent: bool = False) -> List[int]:
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
            versions = await self._rest_client.delete(
                'subjects/{}?permanent=true'.format(_urlencode(subject_name))
            )
            self._cache.remove_by_subject(subject_name)
        else:
            versions = await self._rest_client.delete(
                'subjects/{}'.format(_urlencode(subject_name))
            )

        return versions

    async def get_latest_version(
        self, subject_name: str, fmt: Optional[str] = None
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
        response = await self._rest_client.get(
            'subjects/{}/versions/{}'.format(_urlencode(subject_name), 'latest'), query
        )

        registered_schema = RegisteredSchema.from_dict(response)

        self._latest_version_cache[subject_name] = registered_schema

        return registered_schema

    async def get_latest_with_metadata(
        self, subject_name: str, metadata: Dict[str, str],
        deleted: bool = False, fmt: Optional[str] = None
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

        response = await self._rest_client.get(
            'subjects/{}/metadata'.format(_urlencode(subject_name)), query
        )

        registered_schema = RegisteredSchema.from_dict(response)

        self._latest_with_metadata_cache[cache_key] = registered_schema

        return registered_schema

    async def get_version(
        self, subject_name: str, version: int,
        deleted: bool = False, fmt: Optional[str] = None
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
        response = await self._rest_client.get(
            'subjects/{}/versions/{}'.format(_urlencode(subject_name), version), query
        )

        registered_schema = RegisteredSchema.from_dict(response)

        self._cache.set_registered_schema(registered_schema.schema, registered_schema)

        return registered_schema

    async def get_versions(self, subject_name: str) -> List[int]:
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

        return await self._rest_client.get('subjects/{}/versions'.format(_urlencode(subject_name)))

    async def delete_version(self, subject_name: str, version: int, permanent: bool = False) -> int:
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
            response = await self._rest_client.delete(
                'subjects/{}/versions/{}?permanent=true'.format(_urlencode(subject_name), version)
            )
            self._cache.remove_by_subject_version(subject_name, version)
        else:
            response = await self._rest_client.delete(
                'subjects/{}/versions/{}'.format(_urlencode(subject_name), version)
            )

        return response

    async def set_compatibility(self, subject_name: Optional[str] = None, level: Optional[str] = None) -> str:
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
            return await self._rest_client.put(
                'config', body={'compatibility': level.upper()}
            )

        return await self._rest_client.put(
            'config/{}'.format(_urlencode(subject_name)), body={'compatibility': level.upper()}
        )

    async def get_compatibility(self, subject_name: Optional[str] = None) -> str:
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

        result = await self._rest_client.get(url)
        return result['compatibilityLevel']

    async def test_compatibility(
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

        response = await self._rest_client.post(
            'compatibility/subjects/{}/versions/{}'.format(_urlencode(subject_name), version), body=request
        )

        return response['is_compatible']

    async def set_config(
        self, subject_name: Optional[str] = None,
        config: Optional['ServerConfig'] = None
    ) -> 'ServerConfig':
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
            return await self._rest_client.put(
                'config', body=config.to_dict()
            )

        return await self._rest_client.put(
            'config/{}'.format(_urlencode(subject_name)), body=config.to_dict()
        )

    async def get_config(self, subject_name: Optional[str] = None) -> 'ServerConfig':
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

        result = await self._rest_client.get(url)
        return ServerConfig.from_dict(result)

    def clear_latest_caches(self):
        self._latest_version_cache.clear()
        self._latest_with_metadata_cache.clear()

    def clear_caches(self):
        self._latest_version_cache.clear()
        self._latest_with_metadata_cache.clear()
        self._cache.clear()

    @staticmethod
    def new_client(conf: dict) -> 'AsyncSchemaRegistryClient':
        from .mock_schema_registry_client import AsyncMockSchemaRegistryClient
        url = conf.get("url")
        if url.startswith("mock://"):
            return AsyncMockSchemaRegistryClient(conf)
        return AsyncSchemaRegistryClient(conf)
