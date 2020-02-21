#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright 2016 Confluent Inc.
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


import logging

from requests import Session, utils

from .avro.schema import loads
from .error import ClientError

VALID_LEVELS = ['NONE', 'FULL', 'FORWARD', 'BACKWARD']
VALID_METHODS = ['GET', 'POST', 'PUT', 'DELETE']
VALID_AUTH_PROVIDERS = ['URL', 'USER_INFO', 'SASL_INHERIT']

# Common accept header sent
ACCEPT_HDR = "application/vnd.schemaregistry.v1+json, application/vnd.schemaregistry+json, application/json"
log = logging.getLogger(__name__)


class CachedSchemaRegistryClient(object):
    """
    A client that talks to a Schema Registry over HTTP

    See http://confluent.io/docs/current/schema-registry/docs/intro.html for more information.

    .. deprecated:: 1.1.0

    Use CachedSchemaRegistryClient(dict: config) instead.
    Existing params ca_location, cert_location and key_location will be replaced with their librdkafka equivalents:
    `ssl.ca.location`, `ssl.certificate.location` and `ssl.key.location` respectively.

    Errors communicating to the server will result in a ClientError being raised.

    :param dict conf: schema registry client confguration
    """

    def __init__(self, conf):
        url = conf.pop('url', '')
        if not str(url).startswith('http'):
            raise ValueError("Invalid URL provided for Schema Registry")

        self.url = url.rstrip('/')
        self.id_to_schema = {}

        s = Session()
        ca_path = conf.pop('ssl.ca.location', None)
        if ca_path is not None:
            s.verify = ca_path
        s.cert = self._configure_client_tls(conf)
        s.auth = self._configure_basic_auth(self.url, conf)
        self.url = utils.urldefragauth(self.url)

        self._session = s

        self.auto_register_schemas = conf.pop("auto.register.schemas", True)

        if len(conf) > 0:
            raise ValueError("Unrecognized configuration properties: {}".format(conf.keys()))

    def __del__(self):
        self.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def close(self):
        self._session.close()

    @staticmethod
    def _configure_basic_auth(url, conf):
        auth_provider = conf.pop('basic.auth.credentials.source', 'URL').upper()
        if auth_provider not in VALID_AUTH_PROVIDERS:
            raise ValueError("schema.registry.basic.auth.credentials.source must be one of {}"
                             .format(VALID_AUTH_PROVIDERS))
        if auth_provider == 'SASL_INHERIT':
            if conf.pop('sasl.mechanism', '').upper() is ['GSSAPI']:
                raise ValueError("SASL_INHERIT does not support SASL mechanisms GSSAPI")
            auth = (conf.pop('sasl.username', ''), conf.pop('sasl.password', ''))
        elif auth_provider == 'USER_INFO':
            auth = tuple(conf.pop('basic.auth.user.info', '').split(':'))
        else:
            auth = utils.get_auth_from_url(url)
        return auth

    @staticmethod
    def _configure_client_tls(conf):
        cert = conf.pop('ssl.certificate.location', None), conf.pop('ssl.key.location', None)
        # Both values can be None or no values can be None
        if bool(cert[0]) != bool(cert[1]):
            raise ValueError(
                "Both schema.registry.ssl.certificate.location and schema.registry.ssl.key.location must be set")
        return cert

    def _send_request(self, url, method='GET', body=None, headers={}):
        if method not in VALID_METHODS:
            raise ClientError("Method {} is invalid; valid methods include {}".format(method, VALID_METHODS))

        _headers = {'Accept': ACCEPT_HDR}
        if body:
            _headers["Content-Length"] = str(len(body))
            _headers["Content-Type"] = "application/vnd.schemaregistry.v1+json"
        _headers.update(headers)

        response = self._session.request(method, url, headers=_headers, json=body)
        # Returned by Jetty not SR so the payload is not json encoded
        try:
            if 200 <= response.status_code <= 299:
                return response.json()
            raise ClientError(response.json().get('message'), response.status_code)
        except ValueError:
            return ClientError(response.content, response.status_code)

    def _cache_schema(self, schema):
        # don't overwrite anything
        if schema.id not in self.id_to_schema:
            self.id_to_schema[schema.id] = schema

    def register(self, subject, schema):
        """
        POST /subjects/(string: subject)/versions

        Register a schema with the registry under the given subject
        and receive a schema id.

        Multiple instances of the same schema will result in cache misses.

        :param str subject: subject name
        :param Schema schema: schema to be registered
        :returns: schema_id
        :rtype: int
        """

        if schema is None:
            return None

        # If a schema is already registered with a subject it must have an id
        if subject in schema.subjects:
            return schema.id

        # send it up
        url = '/'.join([self.url, 'subjects', subject, 'versions'])
        # body is { schema : json_string }
        body = {'schema': str(schema)}

        try:
            result = self._send_request(url, method='POST', body=body)
            schema.id = result['id']
            schema.subjects.add(subject)
            return schema.id
        except ClientError as e:
            raise ClientError("Failed to register schema {}: {}".format(e.http_code, e.message))

    def check_registration(self, subject, schema):
        """
        POST /subjects/(string: subject)
        Check if a schema has already been registered under the specified subject.
        If so, returns the schema id. Otherwise, raises a ClientError.

        avro_schema must be a parsed schema from the python avro library

        Multiple instances of the same schema will result in inconsistencies.

        :param str subject: subject name
        :param schema schema: schema to be checked
        :returns: schema_id
        :rtype: int
        """

        # If a schema is already registered with a subject it must have an id
        if subject in schema.subjects:
            return schema.id

        # send it up
        url = '/'.join([self.url, 'subjects', subject])
        # body is { schema : json_string }

        body = {'schema': str(schema)}
        result, code = self._send_request(url, method='POST', body=body)
        if code == 401 or code == 403:
            raise ClientError("Unauthorized access. Error code:" + str(code))
        elif code == 404:
            raise ClientError("Schema or subject not found:" + str(code))
        elif not 200 <= code <= 299:
            raise ClientError("Unable to check schema registration. Error code:" + str(code))
        # result is a dict
        schema.id = result['id']
        schema.subjects.add(subject)
        # cache it
        self._cache_schema(schema)
        return schema.id

    def delete_subject(self, subject):
        """
        DELETE /subjects/(string: subject)
        Deletes the specified subject and its associated compatibility level if registered.
        It is recommended to use this API only when a topic needs to be recycled or in development environments.
        :param subject: subject name
        :returns: version of the schema deleted under this subject
        :rtype: (int)
        """

        url = '/'.join([self.url, 'subjects', subject])

        try:
            return self._send_request(url, method="DELETE")
        except ClientError as e:
            raise ClientError('Unable to delete subject {}: {}'.format(e.http_code, e.http_code))

    def get_by_id(self, schema_id):
        """
        GET /schemas/ids/{int: id}

        Retrieve a parsed Schema by id or None if not found

        :param int schema_id: int value
        :returns: Schema if found
        :rtype: schema
        """

        schema = self.id_to_schema.get(schema_id, None)
        if schema is None:
            # fetch from the registry
            url = '/'.join([self.url, 'schemas', 'ids', str(schema_id)])

            try:
                schema = loads(self._send_request(url).get('schema'))
                schema.id = schema_id
                self._cache_schema(schema)
            except ClientError as e:
                log.error("Failed to fetch schema {}: {}".format(e.http_code, e.message))
        return schema

    def get_latest_schema(self, subject):
        """
        GET /subjects/(string: subject)/versions/(versionId: version)

        Return the latest 3-tuple of:
        (the schema id, the parsed schema, the schema version)
        for a particular subject.

        This call always contacts the registry.

        If the subject is not found, (None,None,None) is returned.
        :param str subject: subject name
        :returns: (schema_id, schema, version)
        :rtype: (string, schema, int)
        """
        url = '/'.join([self.url, 'subjects', subject, 'versions', 'latest'])

        try:
            result = self._send_request(url)

            schema = loads(result['schema'])
            schema.id = result['id']
            schema.subjects.add(subject)
            version = result['version']
            self._cache_schema(schema)
            return schema.id, schema, version
        except ClientError as e:
            log.error("Failed to fetch latest schema {}: {}".format(e.http_code, e.message))
            return None, None, None

    def get_version(self, subject, schema):
        """
        POST /subjects/(string: subject)

        Get the version of a schema for a given subject.
        Returns None if not found.

        :param str subject: subject name
        :param schema schema: schema
        :returns: version
        :rtype: int
        """

        url = '/'.join([self.url, 'subjects', subject])
        body = {'schema': str(schema)}

        try:
            result = self._send_request(url, method='POST', body=body)
            schema.id = result['id']
            schema.subjects.add(subject)
            version = result['version']
            self._cache_schema(schema)
            return version
        except ClientError as e:
            log.error("Failed to fetch schema version {}: {}".format(e.http_code, e.message))

    def test_compatibility(self, subject, schema, version='latest'):
        """
        POST /compatibility/subjects/(string: subject)/versions/(versionId: version)

        Test the compatibility of a candidate parsed schema for a given subject.

        By default the latest version is checked against.
        :param: str subject: subject name
        :param: Schema schema: Schema to test
        :return: True if compatible, False if not compatible
        :rtype: bool
        """

        url = '/'.join([self.url, 'compatibility', 'subjects',
                        subject, 'versions', str(version)])
        body = {'schema': str(schema)}
        try:
            return self._send_request(url, method='POST', body=body).get('is_compatible')
        except ClientError as e:
            log.error("Failed to test compatibility {}".format(e.message), e.http_code)
            return False

    def update_compatibility(self, level, subject=None):
        """
        PUT /config/(string: subject)

        Update the compatibility level for a subject.  Level must be one of:

        :param str level: ex: 'NONE','FULL','FORWARD', or 'BACKWARD'
        :param str subject: Subject name
        """

        if level not in VALID_LEVELS:
            raise ClientError("Invalid level specified: {}".format(level))

        url = '/'.join([self.url, 'config'])
        if subject:
            url += '/' + subject

        body = {"compatibility": level}
        try:
            return self._send_request(url, method='PUT', body=body).get('compatibility')
        except ClientError as e:
            raise ClientError("Unable to update level {}: {}".format(e.http_code, e.message))

    def get_compatibility(self, subject=None):
        """
        GET /config
        Get the current compatibility level for a subject.  Result will be one of:

        :param str subject: subject name
        :raises ClientError: if the request was unsuccessful or an invalid compatibility level was returned
        :returns: one of 'NONE','FULL','FORWARD', or 'BACKWARD'
        :rtype: bool
        """
        url = '/'.join([self.url, 'config'])
        if subject:
            url = '/'.join([url, subject])

        try:
            return self._send_request(url).get('compatibilityLevel', None)
        except ClientError as e:
            raise ClientError('Failed to fetch compatibility level {}: {}'.format(e.http_code, e.message))


def TopicNameStrategy(schema, ctx):
    """

        :param SerializationContext ctx:
        :param Schema schema: unused
        :return:
        """
    return ctx.topic + "-" + str(ctx.field)


def TopicRecordNameStrategy(schema, ctx):
    """

        :param SerializationContext ctx:
        :param Schema schema: unused
        :return:
        """
    return ctx.topic + "-" + schema.name


def RecordNameStrategy(schema, ctx):
    """

        :param SerializationContext ctx:
        :param Schema schema: unused
        :return:
        """
    return schema.name
