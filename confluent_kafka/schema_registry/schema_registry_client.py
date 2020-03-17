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

from requests import Session

from .error import SchemaRegistryClientError
from .schema import SchemaProvider

log = logging.getLogger(__name__)

"""
API Endpoint templates
"""
SCHEMAS = 'schemas/ids/{}?format=serialized'
SUBJECT = 'subjects/{}'
SUBJECTS = 'subjects'
VERSION = 'subjects/{}/versions/{}'
VERSIONS = 'subjects/{}/versions'
CONFIG = 'config/{}'
CONFIGS = 'config'


def url_formatter(template, *args):
    """
    Constructs a url string from a template.

    Args:
        template: url format string
        args: args to use when resolving url string

    """
    return template.format(*args)


class SchemaRegistryClient(object):
    """
    A client that talks to a Schema Registry over HTTP

    See Also:
        http://confluent.io/docs/current/schema-registry/docs/intro.html

    Arguments:
        conf (SchemaRegistryConfig): Schema Registry config object
    """

    def __init__(self, conf):
        self._rest_client = self._RestClient(conf)
        self.id_to_schema = {}

    def __enter__(self):
        return self

    def __exit__(self, *args):
        if self._rest_client is not None:
            self._rest_client.close()

    class _RestClient(object):
        default_headers = {'Accept': "application/vnd.schemaregistry.v1+json,"
                                     " application/vnd.schemaregistry+json,"
                                     " application/json"}

        def __init__(self, conf):
            self.session = Session()
            self.base_url = conf.Url.rstrip('/')

            if conf.Certificate is not (None, None):
                self.session.cert = conf.Certificate

            if conf.CA is not None:
                self.session.verify = conf.CA

            if conf.Credentials:
                self.session.auth = conf.Credentials

        def __del__(self):
            self.session.close()

        def GET(self, url, decoder=None):
            return self.send_request(url, method='GET', decoder=decoder)

        def POST(self, url, body, encoder=None, decoder=None, **kwargs):
            return self.send_request(url, method='POST',
                                     body=body, encoder=encoder, decoder=decoder,
                                     **kwargs)

        def DELETE(self, url):
            return self.send_request(url, method='DELETE')

        def PUT(self, url, body=None, encoder=None, decoder=None):
            return self.send_request(url, method='PUT', body=body,
                                     encoder=encoder, decoder=decoder)

        def send_request(self, url, method, body=None,
                         encoder=None, decoder=None, **kwargs):

            _headers = self.default_headers

            if body is not None:
                body = json.dumps(body, cls=encoder)
                _headers = {'Content-Length': str(len(body)),
                            'Content-Type': "application/vnd.schemaregistry.v1+json"}

            response = self.session.request(
                method, url="/".join([self.base_url, url]),
                headers=_headers, data=body)

            try:
                if 200 <= response.status_code <= 299:
                    return response.json(cls=decoder, **kwargs)
                raise SchemaRegistryClientError(response.status_code,
                                                response.json().get('message'))
            except ValueError:
                raise SchemaRegistryClientError(response.status_code,
                                                response.content)

    def register_schema(self, subject_name, schema):
        """
        POST /subjects/(string: subject)/versions

        Registers a schema under ``subject_name``.
        Results are cached in the ``schema`` so subsequent requests will not
        require another trip the Schema Registry.

        Args:
            - subject_name (str): subject to register a schema under
            - schema (Schema): Schema (in JSON string format) to register.

        Returns:
            Version: Registration information

        Raises:
            SchemaRegistryClientError: if Schema violates this subject's
            Compatibility policy or is otherwise invalid.

        .. _Schema Registry API Reference:
            https://docs.confluent.io/current/schema-registry/develop/api.html#post--subjects-(string-%20subject)-versions

        """  # noqa: E501
        version = schema.subjects.get(subject_name, None)
        if version:
            return version

        self._rest_client.POST(
            url_formatter(VERSIONS, subject_name),
            body=schema, encoder=_SchemaJSONEncoder)

        # get registration information
        return self._rest_client.POST(
            url_formatter(SUBJECT, subject_name),
            body=schema, encoder=_SchemaJSONEncoder, decoder=_VersionJSONDecoder,
            schema=schema)

    def get_schema(self, schema_id):
        """
        GET /schemas/ids/{int: schema_id}

        Fetches the schema associated with ``schema_id`` from the
        Schema Registry. The result is cached so subsequent attempts will not
        require an additional trip to the Schema Registry.

        Args:
            - schema_id (int): Schema id

        Returns:
            Schema: Schema string identified by the ``schema_id``

        Raises:
            SchemaRegistryClientError: If schema can't be found

        .. _Schema Registry API Reference:
            https://docs.confluent.io/current/schema-registry/develop/api.html#get--schemas-ids-int-%20id

        """  # noqa: E501
        version = self.id_to_schema.get(schema_id, None)

        if version is not None:
            return version

        url = url_formatter(SCHEMAS, schema_id)
        schema = self._rest_client.GET(url, decoder=_SchemaJSONDecoder)
        self.id_to_schema[schema_id] = schema

        return schema

    def get_registration(self, subject_name, schema):
        """
        POST /subjects/(string: subject)

        Returns ``schema`` registration information for ``subject``.

        Args:
            - subject_name (str): Subject name the schema is registered under
            - schema (Schema): Registered Schema

        Returns:
            Version: ``schema`` registration information for ``subject``

        Raises:
            SchemaRegistryClientError: If schema or subject can't be found

        .. _Schema Registry API Reference:
            https://docs.confluent.io/current/schema-registry/develop/api.html#post--subjects-(string-%20subject)

        """  # noqa: E501
        version = schema.subjects.get(subject_name, None)
        if version:
            return version

        return self._rest_client.POST(
            url_formatter(SUBJECT, subject_name),
            encoder=_SchemaJSONEncoder,
            decoder=_VersionJSONDecoder,
            schema=schema)

    def get_subjects(self):
        """
        GET /subjects

        List all subjects registered with the Schema Registry

        Returns:
            [str]: Registered subject names

        Raises:
            SchemaRegistryClientError: if subjects can't be found

        .. _Schema Registry API Reference:
            https://docs.confluent.io/current/schema-registry/develop/api.html#get--subjects

        """  # noqa: E501
        return self._rest_client.GET(url_formatter(SUBJECTS))

    def delete_subject(self, subject):
        """
        DELETE /subjects/(string: subject)

        Deletes the specified subject and its associated compatibility level if
        registered. It is recommended to use this API only when a topic needs
        to be recycled or in development environments.

        Args:
            - subject (str): subject name
        Returns:
            list(int): Versions deleted under this subject

        Raises:
            SchemaRegistryClientError: if the request was unsuccessful.

        .. _Schema Registry API Reference:
            https://docs.confluent.io/current/schema-registry/develop/api.html#delete--subjects-(string-%20subject)

        """  # noqa: E501
        self._rest_client.DELETE(url_formatter(SUBJECT, subject))

    def get_version(self, subject_name, version='latest'):
        """
        GET /subjects/(string: subject)/versions/(versionId: version)

        Retrieves a specific schema registered to ``subject_name``.

        .. _note:
            "latest” returns the last registered schema under the specified
            subject. Note that there may be a new latest schema that gets
            registered right after this request is served.

        Args:
            - subject_name (str): Subject name.
            - version (int, optional): version number. Defaults to "latest".

        Returns:
            Version: Registration information for this version.

        Raises:
            SchemaRegistryClientError if the version can't be found or
            is invalid.

        .. _Schema Registry API Reference:
            https://docs.confluent.io/current/schema-registry/develop/api.html#get--subjects-(string-%20subject)-versions-(versionId-%20version)

        """  # noqa: E501
        return self._rest_client.GET(
            url_formatter(VERSION, subject_name, version),
            decoder=_VersionJSONDecoder)

    def list_versions(self, subject_name):
        """
        GET /subjects/(string: subject)/versions

        Args:
            - subject_name (str): Subject name.

        Returns:
            [int]: Registered versions

        Raises:
            SchemaRegistryClientError If subject can't be found

        .. _Schema Registry API Reference:
            https://docs.confluent.io/current/schema-registry/develop/api.html#get--subjects-(string-%20subject)-versions

        """  # noqa: E501
        return self._rest_client.GET(url_formatter(VERSIONS,
                                                   subject_name))

    def delete_version(self, subject_name, version):
        """
        DELETE /subjects/(string: subject)/versions/(versionId: version)

        Deletes a specific version registered to ``subject_name``.

        Args:
            - subject_name (str) Subject name
            - version (int): version number

        Returns
            Version: Version of the deleted schema

        Raises:
            SchemaRegistryClientError if the subject or version cannot be found.

        .. _Schema Registry API Reference:
            https://docs.confluent.io/current/schema-registry/develop/api.html#delete--subjects-(string-%20subject)-versions-(versionId-%20version)

        """  # noqa: E501
        self._rest_client.DELETE(url_formatter(VERSION,
                                               subject_name,
                                               version))

    def set_compatibility(self, subject_name=None, level=None):
        """
        PUT /config/{subject_name}

        Update global or subject level compatibility policy.

        Args:
            - level (CompatibilityType): Compatibility policy

        Keywork Args:
            - subject_name (str, optional): Subject name.
                Sets global policy if not set.
            - level  (CompatibilityType): Compatibility policy
        Raises:
            SchemaRegistryClientError: if the compatibility level is invalid

        .. _Schema Registry API Reference:
            https://docs.confluent.io/current/schema-registry/develop/api.html#put--config

        """  # noqa: E501

        if level is None:
            raise ValueError("level must be set")

        print(subject_name)
        if subject_name is None:
            return self._rest_client.PUT(url_formatter(CONFIGS),
                                         body=level,
                                         encoder=_CompatibilityJSONEncoder,
                                         decoder=_CompatibilityJSONDecoder)

        return self._rest_client.PUT(url_formatter(CONFIG,
                                                   subject_name),
                                     body=level,
                                     encoder=_CompatibilityJSONEncoder,
                                     decoder=_CompatibilityJSONDecoder)

    def get_compatibility(self, subject_name=None):
        """
        GET /config

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
            url = url_formatter(CONFIG, subject_name)
        else:
            url = url_formatter(CONFIGS)

        return self._rest_client.GET(url, decoder=_CompatibilityJSONDecoder)


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


class Version(object):
    """
    Represents a Schema's registration with the Schema Registry

    Args:
        - subject (Subject): Subject this Schema is registered under
        - version (version): Version index(ID)
        - schema (Schema): The registered Schema.

    """
    __slots__ = ['subject', 'version', 'schema', 'schema_id']

    def __init__(self, subject, version, schema_id, schema):
        # register self with schema
        schema.subjects[subject] = self

        self.subject = subject
        self.version = version
        self.schema_id = schema_id
        self.schema = schema

    def __str__(self):
        return "{!s}[{!s}]".format(self.subject, self.version)

    def __repr__(self):
        return str(self)

    def __eq__(self, other):
        return str(self) == str(other)

    def __hash__(self):
        return hash(str(self))


class _VersionJSONDecoder(json.JSONDecoder):
    """
    Decodes Version response JSON objects from the Schema Registry.

    Keyword Args:
        schema (Schema, optional): Schema associated with this version.

    """

    def __init__(self, schema=None, **kwargs):
        self.schema = schema
        json.JSONDecoder.__init__(self, object_hook=self.object_hook)

    def object_hook(self, version_dict):
        """
        Decodes Schema Registry response body.

        Note:
            This decoder alters the contents of ``obj``

        Args:
            - version_dict (dict): Schema Registry response body

        Returns:
            int, string or tuple depending on registry response type.

        .. _Schema Registry API Reference:
            https://docs.confluent.io/current/schema-registry/develop/api.html

        """
        if self.schema is None:
            schema = SchemaProvider(version_dict.get('schema'),
                                    version_dict.get('schemaType', 'AVRO'))
        else:
            schema = self.schema

        subject = version_dict['subject']
        schema_id = version_dict['id']
        version = version_dict['version']

        return Version(subject, version, schema_id, schema)


class _SchemaJSONEncoder(json.JSONEncoder):
    def default(self, schema):
        """
        Encodes Schema object to the Schema Registry request body format

        Args:
            - schema (Schema): Schema object

        Returns:
            str: Schema Registry request

        """
        # omit schema tag for compatibility with older SR instances.
        if schema.schema_type == 'AVRO':
            return {'schema': str(schema)}
        else:
            return {'schema': str(schema), 'schemaType': str(schema.schema_type)}


class _SchemaJSONDecoder(json.JSONDecoder):
    def __init__(self, *args, **kwargs):
        json.JSONDecoder.__init__(self, object_hook=self.object_hook,
                                  *args, **kwargs)

    def object_hook(self, response):
        """
        Instantiates Schema instance from Schema Registry response

        Args:
            - response (dict):

        Returns:
            Schema: A new Schema instance

        """
        return SchemaProvider(response.get('schema'),
                              schema_type=response.get('schemaType', 'AVRO'))


class _CompatibilityJSONEncoder(json.JSONEncoder):
    def encode(self, level):
        """
        Encodes subject compatibility level to JSON

        Args:
            - level (int): Schema object

        Returns:
            dict: Encoded CompatibilityLevel

        """
        return super(_CompatibilityJSONEncoder, self).encode(
            {'compatibility': level})


class _CompatibilityJSONDecoder(json.JSONDecoder):
    def __init__(self, *args, **kwargs):
        json.JSONDecoder.__init__(self, object_hook=self.object_hook, *args, **kwargs)

    def object_hook(self, response):
        """
        Decodes Schema Registry Compatibility response.

        Note:
            This decoder alters the contents of ``obj``

        Args:
            - response (dict): Schema Registry response body

        Returns:
            int, string or tuple depending on registry response type.

        .. _Schema Registry API Reference:
            https://docs.confluent.io/current/schema-registry/develop/api.html

        """
        level = response.get('compatibilityLevel', response.get('compatibility'))

        if level:
            return level

        return response.get('is_compatible')
