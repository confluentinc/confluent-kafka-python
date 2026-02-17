#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright 2025 Confluent Inc.
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

import abc
from urllib.parse import urlparse, urlunparse

__all__ = [
    '_AbstractOAuthBearerFieldProviderBuilder',
    '_AbstractOAuthBearerOIDCFieldProviderBuilder',
    '_StaticOAuthBearerFieldProviderBuilder',
    '_AbstractCustomOAuthBearerFieldProviderBuilder',
    '_AbstractOAuthBearerOIDCAzureIMDSFieldProviderBuilder'
]


class _BearerFieldProvider(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def get_bearer_fields(self) -> dict:
        raise NotImplementedError


class _AbstractOAuthBearerFieldProviderBuilder(metaclass=abc.ABCMeta):
    """Abstract base class for OAuthBearer client builders"""
    required_properties = ['bearer.auth.logical.cluster', 'bearer.auth.identity.pool.id']

    def __init__(self, conf: dict):
        self.conf: dict = conf
        self.logical_cluster: str = None
        self.identity_pool: str = None

    def _validate(self):
        missing_properties = [prop for prop in
                              _AbstractOAuthBearerFieldProviderBuilder.required_properties
                              if prop not in self.conf]
        if missing_properties:
            raise ValueError("Missing required bearer configuration properties: {}"
                             .format(", ".join(missing_properties)))

        self.logical_cluster = self.conf.pop('bearer.auth.logical.cluster')
        if not isinstance(self.logical_cluster, str):
            raise TypeError("logical cluster must be a str, not " +
                            str(type(self.logical_cluster)))

        self.identity_pool = self.conf.pop('bearer.auth.identity.pool.id')
        if not isinstance(self.identity_pool, str):
            raise TypeError("identity pool id must be a str, not " +
                            str(type(self.identity_pool)))

    @abc.abstractmethod
    def build(self, max_retries: int, retries_wait_ms: int, retries_max_wait_ms: int) -> _BearerFieldProvider:
        pass


class _AbstractOAuthBearerOIDCFieldProviderBuilder(_AbstractOAuthBearerFieldProviderBuilder):
    required_properties = ['bearer.auth.client.id', 'bearer.auth.client.secret',
                           'bearer.auth.scope',
                           'bearer.auth.issuer.endpoint.url']

    def __init__(self, conf: dict):
        super().__init__(conf)
        self.client_id: str = None
        self.client_secret: str = None
        self.scope: str = None
        self.token_endpoint: str = None

    def _validate(self):
        super()._validate()

        missing_properties = [prop for prop in
                              _AbstractOAuthBearerOIDCFieldProviderBuilder.required_properties
                              if prop not in self.conf]
        if missing_properties:
            raise ValueError("Missing required OAuth configuration properties: {}".
                             format(", ".join(missing_properties)))

        self.client_id = self.conf.pop('bearer.auth.client.id')
        if not isinstance(self.client_id, str):
            raise TypeError("bearer.auth.client.id must be a str, not " +
                            str(type(self.client_id)))

        self.client_secret = self.conf.pop('bearer.auth.client.secret')
        if not isinstance(self.client_secret, str):
            raise TypeError("bearer.auth.client.secret must be a str, not " +
                            str(type(self.client_secret)))

        self.scope = self.conf.pop('bearer.auth.scope')
        if not isinstance(self.scope, str):
            raise TypeError("bearer.auth.scope must be a str, not " +
                            str(type(self.scope)))

        self.token_endpoint = self.conf.pop('bearer.auth.issuer.endpoint.url')
        if not isinstance(self.token_endpoint, str):
            raise TypeError("bearer.auth.issuer.endpoint.url must be a str, not "
                            + str(type(self.token_endpoint)))


class _AbstractOAuthBearerOIDCAzureIMDSFieldProviderBuilder(_AbstractOAuthBearerFieldProviderBuilder):

    def __init__(self, conf: dict):
        super().__init__(conf)
        self.token_endpoint: str = 'http://169.254.169.254/metadata/identity/oauth2/token'

    def _validate(self):
        super()._validate()

        token_endpoint_override = 'bearer.auth.issuer.endpoint.url' in self.conf
        self.token_endpoint = self.conf.pop('bearer.auth.issuer.endpoint.url', self.token_endpoint)
        if not isinstance(self.token_endpoint, str):
            raise TypeError("bearer.auth.issuer.endpoint.url must be a str, not "
                            + str(type(self.token_endpoint)))

        try:
            parsed_token_endpoint = urlparse(self.token_endpoint)
        except Exception as ex:
            raise ValueError(f'Failed to parse token endpoint URL: {ex}')

        token_query = self.conf.pop('bearer.auth.issuer.endpoint.query', None)
        if token_query:
            if not isinstance(token_query, str):
                raise TypeError("bearer.auth.issuer.endpoint.query must be a str, not "
                                + str(type(token_query)))

            parsed_token_endpoint = parsed_token_endpoint._replace(
                query=token_query,
                fragment=None)
            self.token_endpoint = urlunparse(parsed_token_endpoint)
        elif not token_endpoint_override:
            raise ValueError("bearer.auth.issuer.endpoint.query must be provided "
                             "when bearer.auth.issuer.endpoint.url isn't overridden")


class _StaticFieldProvider(_BearerFieldProvider):
    def __init__(self, token: str, logical_cluster: str, identity_pool: str):
        self.token: str = token
        self.logical_cluster: str = logical_cluster
        self.identity_pool: str = identity_pool

    def get_bearer_fields(self) -> dict:
        return {'bearer.auth.token': self.token, 'bearer.auth.logical.cluster': self.logical_cluster,
                'bearer.auth.identity.pool.id': self.identity_pool}


class _StaticOAuthBearerFieldProviderBuilder(_AbstractOAuthBearerFieldProviderBuilder):

    def __init__(self, conf: dict):
        super().__init__(conf)
        self.static_token: str = None

    def _validate(self):
        super()._validate()

        if 'bearer.auth.token' not in self.conf:
            raise ValueError("Missing bearer.auth.token")
        self.static_token = self.conf.pop('bearer.auth.token')
        if not isinstance(self.static_token, str):
            raise TypeError("bearer.auth.token must be a str, not " +
                            str(type(self.static_token)))

    def build(self, max_retries: int, retries_wait_ms: int, retries_max_wait_ms: int):
        self._validate()
        return _StaticFieldProvider(
            self.static_token,
            self.logical_cluster,
            self.identity_pool
        )


class _AbstractCustomOAuthBearerFieldProviderBuilder:
    required_properties = ['bearer.auth.custom.provider.function',
                           'bearer.auth.custom.provider.config']

    def __init__(self, conf: dict):
        self.conf = conf
        self.custom_function = None
        self.custom_config = None

    def _validate(self):
        missing_properties = [prop for prop in
                              _AbstractCustomOAuthBearerFieldProviderBuilder.required_properties
                              if prop not in self.conf]
        if missing_properties:
            raise ValueError("Missing required custom OAuth configuration properties: {}".
                             format(", ".join(missing_properties)))

        self.custom_function = self.conf.pop('bearer.auth.custom.provider.function')
        if not callable(self.custom_function):
            raise TypeError("bearer.auth.custom.provider.function must be a callable, not "
                            + str(type(self.custom_function)))

        self.custom_config = self.conf.pop('bearer.auth.custom.provider.config')
        if not isinstance(self.custom_config, dict):
            raise TypeError("bearer.auth.custom.provider.config must be a dict, not "
                            + str(type(self.custom_config)))
