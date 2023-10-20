#!/usr/bin/env python
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


#
# derived from https://github.com/verisign/python-confluent-schemaregistry.git
#
import logging
from turtle import pos
from typing import Any, Dict, Optional, Sized, Tuple, TypeVar, Union, cast
import warnings
import urllib3
import json
from collections import defaultdict

from requests import Session, utils, Response

from confluent_kafka.schema_registry.schema_registry_client import Schema

from .error import ClientError
from . import loads, schema

import six

string_type = six.string_types[0]

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
    The support for password protected private key is via the Config only using 'ssl.key.password' field.

    Errors communicating to the server will result in a ClientError being raised.

    :param str|dict url: url(deprecated) to schema registry or dictionary containing client configuration.
    :param str ca_location: File or directory path to CA certificate(s) for verifying the Schema Registry key.
    :param str cert_location: Path to client's public key used for authentication.
    :param str key_location: Path to client's private key used for authentication.
    """

    def __init__(self, url: Union[str, Dict[str, object]], max_schemas_per_subject: int=1000, ca_location: Optional[str]=None, cert_location: Optional[str]=None, key_location: Optional[str]=None):
        # In order to maintain compatibility the url(conf in future versions) param has been preserved for now.        
        if isinstance(url, dict):
            conf = url
        else:
            conf = {
                'url': url,
                'ssl.ca.location': ca_location,
                'ssl.certificate.location': cert_location,
                'ssl.key.location': key_location
            }
            warnings.warn(
                "CachedSchemaRegistry constructor is being deprecated. "
                "Use CachedSchemaRegistryClient(dict: config) instead. "
                "Existing params ca_location, cert_location and key_location will be replaced with their "
                "librdkafka equivalents as keys in the conf dict: `ssl.ca.location`, `ssl.certificate.location` and "
                "`ssl.key.location` respectively",
                category=DeprecationWarning, stacklevel=2)

            """Construct a Schema Registry client"""

        # Ensure URL valid scheme is included; http[s]
        url = cast(str, conf.pop('url', ''))
        if not isinstance(url, string_type):
            raise TypeError("URL must be of type str")

        if not url.startswith('http'):
            raise ValueError("Invalid URL provided for Schema Registry")

        self.url = url.rstrip('/')

        # subj => { schema => id }
        self.subject_to_schema_ids: Dict[str, Dict[schema.Schema, int]] = defaultdict(dict)
        # id => avro_schema
        self.id_to_schema: Dict[int, schema.Schema] = {}
        # subj => { schema => version }
        self.subject_to_schema_versions: Dict[str, Dict[schema.Schema, int]] = defaultdict(dict)

        s = Session()
        ca_path = cast(Optional[str], conf.pop('ssl.ca.location', None))
        if ca_path is not None:
            s.verify = ca_path
        _conf_cert = self._configure_client_tls(conf)
        
        # Logic in _configure_client_tls promises both of the output variables are of the same type
        if _conf_cert == [None, None]:
            s.cert = None
        else:
            s.cert = _conf_cert

        s.auth = self._configure_basic_auth(self.url, conf)
        self.url = utils.urldefragauth(self.url)

        self._session = s
        key_password = conf.pop('ssl.key.password', None)
        self._is_key_password_provided = not key_password
        self._https_session = self._make_https_session(s.cert[0] if s.cert is not None else None, s.cert[1] if s.cert is not None else None, ca_path, s.auth, key_password)

        self.auto_register_schemas = conf.pop("auto.register.schemas", True)

        if len(conf) > 0:
            raise ValueError("Unrecognized configuration properties: {}".format(conf.keys()))

    def __del__(self) -> None:
        self.close()

    def __enter__(self) -> "CachedSchemaRegistryClient":
        return self

    def __exit__(self, *args: object) -> None:
        self.close()

    def close(self) -> None:
        # Constructor exceptions may occur prior to _session being set.
        if hasattr(self, '_session'):
            self._session.close()
        if hasattr(self, '_https_session'):
            self._https_session.clear() # type: ignore[no-untyped-call]

    @staticmethod
    def _make_https_session(cert_location: Optional[str], key_location: Optional[str], ca_certs_path: Optional[str], auth: Tuple[str, str], key_password: object) -> urllib3.PoolManager:
        https_session = urllib3.PoolManager(cert_reqs='CERT_REQUIRED', ca_certs=ca_certs_path,
                                            cert_file=cert_location, key_file=key_location, key_password=key_password)
        https_session.auth = auth # type: ignore[attr-defined]
        return https_session

    def _send_https_session_request(self, url: str, method: str, headers: Dict, body: Any) -> urllib3.response.HTTPResponse:
        request_headers = {'Accept': ACCEPT_HDR}
        auth = self._https_session.auth # type: ignore[attr-defined]
        if body:
            body = json.dumps(body).encode('UTF-8')
            request_headers["Content-Length"] = str(len(body))
            request_headers["Content-Type"] = "application/vnd.schemaregistry.v1+json"
        if auth[0] != '' and auth[1] != '':
            request_headers.update(urllib3.make_headers(basic_auth=auth[0] + ":" + auth[1])) # type:ignore[no-untyped-call]
        request_headers.update(headers)
        response = self._https_session.request(method, url, headers=request_headers, body=body) # type:ignore[no-untyped-call]
        return response

    @staticmethod
    def _configure_basic_auth(url: str, conf: Dict) -> Tuple[str, str]:
        auth_provider = conf.pop('basic.auth.credentials.source', 'URL').upper()
        if auth_provider not in VALID_AUTH_PROVIDERS:
            raise ValueError("schema.registry.basic.auth.credentials.source must be one of {}"
                             .format(VALID_AUTH_PROVIDERS))
        auth: Tuple[str, str]
        if auth_provider == 'SASL_INHERIT':
            if conf.pop('sasl.mechanism', '').upper() == 'GSSAPI':
                raise ValueError("SASL_INHERIT does not support SASL mechanism GSSAPI")
            auth = (cast(str, conf.pop('sasl.username', '')), cast(str, conf.pop('sasl.password', '')))
        elif auth_provider == 'USER_INFO':
            possible_auth = tuple(cast(str, conf.pop('basic.auth.user.info', ':')).split(':'))
            assert len(possible_auth) == 2, possible_auth
            auth = cast(Tuple[str, str], possible_auth)
        else:
            auth = utils.get_auth_from_url(url)
        return auth

    _client_tls_ret = TypeVar("_client_tls_ret", str, None)

    @staticmethod
    def _configure_client_tls(conf: Dict) -> Tuple[_client_tls_ret, _client_tls_ret]:
        cert = cast(Optional[str], conf.pop('ssl.certificate.location', None)), cast(Optional[str], conf.pop('ssl.key.location', None))
        # Both values can be None or no values can be None
        if (cert[0] is None) != (cert[1] is None):
            raise ValueError(
                "Both schema.registry.ssl.certificate.location and schema.registry.ssl.key.location must be set")
        return cert # type: ignore[return-value]

    def _send_request(self, url: str, method: str='GET', body: Optional[Sized]=None, headers: Dict={}) -> Tuple[object, int]:
        if method not in VALID_METHODS:
            raise ClientError("Method {} is invalid; valid methods include {}".format(method, VALID_METHODS))

        if url.startswith('https') and self._is_key_password_provided:
            http_response = self._send_https_session_request(url, method, headers, body)
            try:
                return json.loads(http_response.data), http_response.status
            except ValueError:
                return http_response.data, http_response.status

        _headers = {'Accept': ACCEPT_HDR}
        if body:
            _headers["Content-Length"] = str(len(body))
            _headers["Content-Type"] = "application/vnd.schemaregistry.v1+json"
        _headers.update(headers)

        response = self._session.request(method, url, headers=_headers, json=body)
        # Returned by Jetty not SR so the payload is not json encoded
        try:
            return response.json(), response.status_code
        except ValueError:
            return response.content, response.status_code

    CacheKey = TypeVar("CacheKey")
    SubCacheKey = TypeVar("SubCacheKey")
    SubCacheValue = TypeVar("SubCacheValue")

    @staticmethod
    def _add_to_cache(cache: Dict[CacheKey, Dict[SubCacheKey, SubCacheValue]], subject: CacheKey, schema: SubCacheKey, value: SubCacheValue) -> None:
        sub_cache = cache[subject]
        sub_cache[schema] = value

    def _cache_schema(self, schema: schema.Schema, schema_id: int, subject: Optional[str]=None, version: Optional[int]=None) -> None:
        # don't overwrite anything
        if schema_id in self.id_to_schema:
            schema = self.id_to_schema[schema_id]
        else:
            self.id_to_schema[schema_id] = schema

        if subject:
            self._add_to_cache(self.subject_to_schema_ids,
                               subject, schema, schema_id)
            if version:
                self._add_to_cache(self.subject_to_schema_versions,
                                   subject, schema, version)

    def register(self, subject: str, avro_schema: schema.Schema) -> int:
        """
        POST /subjects/(string: subject)/versions
        Register a schema with the registry under the given subject
        and receive a schema id.

        avro_schema must be a parsed schema from the python avro library

        Multiple instances of the same schema will result in cache misses.

        :param str subject: subject name
        :param schema avro_schema: Avro schema to be registered
        :returns: schema_id
        :rtype: int
        """

        schemas_to_id = self.subject_to_schema_ids[subject]
        schema_id: Optional[int] = schemas_to_id.get(avro_schema, None)
        if schema_id is not None:
            return schema_id
        # send it up
        url = '/'.join([self.url, 'subjects', subject, 'versions'])
        # body is { schema : json_string }

        body = {'schema': str(avro_schema)}
        result, code = self._send_request(url, method='POST', body=body)
        if (code == 401 or code == 403):
            raise ClientError("Unauthorized access. Error code:" + str(code)
                              + " message:" + str(result))
        elif code == 409:
            raise ClientError("Incompatible Avro schema:" + str(code)
                              + " message:" + str(result))
        elif code == 422:
            raise ClientError("Invalid Avro schema:" + str(code)
                              + " message:" + str(result))
        elif not (code >= 200 and code <= 299):
            raise ClientError("Unable to register schema. Error code:" + str(code)
                              + " message:" + str(result))
        # result is a dict
        schema_id = cast(Dict, result)['id']
        # cache it
        self._cache_schema(avro_schema, schema_id, subject)
        return schema_id

    def check_registration(self, subject: str, avro_schema: schema.Schema) -> int:
        """
        POST /subjects/(string: subject)
        Check if a schema has already been registered under the specified subject.
        If so, returns the schema id. Otherwise, raises a ClientError.

        avro_schema must be a parsed schema from the python avro library

        Multiple instances of the same schema will result in inconsistencies.

        :param str subject: subject name
        :param schema avro_schema: Avro schema to be checked
        :returns: schema_id
        :rtype: int
        """

        schemas_to_id = self.subject_to_schema_ids[subject]
        schema_id = schemas_to_id.get(avro_schema, None)
        if schema_id is not None:
            return schema_id
        # send it up
        url = '/'.join([self.url, 'subjects', subject])
        # body is { schema : json_string }

        body = {'schema': str(avro_schema)}
        result, code = self._send_request(url, method='POST', body=body)
        if code == 401 or code == 403:
            raise ClientError("Unauthorized access. Error code:" + str(code))
        elif code == 404:
            raise ClientError("Schema or subject not found:" + str(code))
        elif not 200 <= code <= 299:
            raise ClientError("Unable to check schema registration. Error code:" + str(code))
        # result is a dict
        schema_id = cast(Dict, result)['id']
        # cache it
        self._cache_schema(avro_schema, schema_id, subject)
        return schema_id

    def delete_subject(self, subject: str) -> int:
        """
        DELETE /subjects/(string: subject)
        Deletes the specified subject and its associated compatibility level if registered.
        It is recommended to use this API only when a topic needs to be recycled or in development environments.
        :param subject: subject name
        :returns: version of the schema deleted under this subject
        :rtype: (int)
        """

        url = '/'.join([self.url, 'subjects', subject])

        result, code = self._send_request(url, method="DELETE")
        if not (code >= 200 and code <= 299):
            raise ClientError('Unable to delete subject: {}'.format(result))
        assert isinstance(result, int)
        return result

    def get_by_id(self, schema_id: int) -> Optional[schema.Schema]:
        """
        GET /schemas/ids/{int: id}
        Retrieve a parsed avro schema by id or None if not found
        :param int schema_id: int value
        :returns: Avro schema
        :rtype: schema
        """
        if schema_id in self.id_to_schema:
            return self.id_to_schema[schema_id]
        # fetch from the registry
        url = '/'.join([self.url, 'schemas', 'ids', str(schema_id)])

        result, code = self._send_request(url)
        if code == 404:
            log.error("Schema not found:" + str(code))
            return None
        elif not (code >= 200 and code <= 299):
            log.error("Unable to get schema for the specific ID:" + str(code))
            return None
        else:
            # need to parse the schema
            assert isinstance(result, Dict)
            schema_str = result["schema"]
            assert isinstance(schema_str, str)
            try:
                result = loads(schema_str)
                # cache it
                self._cache_schema(result, schema_id)
                return result
            except ClientError as e:
                # bad schema - should not happen
                raise ClientError("Received bad schema (id %s) from registry: %s" % (schema_id, e))

    def get_latest_schema(self, subject: str) -> Tuple[Optional[int], Optional[schema.Schema], Optional[int]]:
        """
        GET /subjects/(string: subject)/versions/latest

        Return the latest 3-tuple of:
        (the schema id, the parsed avro schema, the schema version)
        for a particular subject.

        This call always contacts the registry.

        If the subject is not found, (None,None,None) is returned.
        :param str subject: subject name
        :returns: (schema_id, schema, version)
        :rtype: (string, schema, int)
        """
        return self.get_by_version(subject, 'latest')

    def get_by_version(self, subject: str, version: object) -> Tuple[Optional[int], Optional[schema.Schema], Optional[int]]:
        """
        GET /subjects/(string: subject)/versions/(versionId: version)

        Return the 3-tuple of:
        (the schema id, the parsed avro schema, the schema version)
        for a particular subject and version.

        This call always contacts the registry.

        If the subject is not found, (None,None,None) is returned.
        :param str subject: subject name
        :param int version: version number
        :returns: (schema_id, schema, version)
        :rtype: (string, schema, int)
        """
        url = '/'.join([self.url, 'subjects', subject, 'versions', str(version)])

        result, code = self._send_request(url)
        if code == 404:
            log.error("Schema not found:" + str(code))
            return (None, None, None)
        elif code == 422:
            log.error("Invalid version:" + str(code))
            return (None, None, None)
        elif not (code >= 200 and code <= 299):
            return (None, None, None)
        assert isinstance(result, Dict)
        schema_id = result['id']
        assert isinstance(schema_id, int)
        version = result['version']
        assert isinstance(version, int)
        if schema_id in self.id_to_schema:
            schema = self.id_to_schema[schema_id]
        else:
            try:
                schema = loads(result['schema'])
            except ClientError:
                # bad schema - should not happen
                raise

        self._cache_schema(schema, schema_id, subject, version)
        return (schema_id, schema, version)

    def get_version(self, subject: str, avro_schema: schema.Schema) -> Optional[int]:
        """
        POST /subjects/(string: subject)

        Get the version of a schema for a given subject.

        Returns None if not found.
        :param str subject: subject name
        :param: schema avro_schema: Avro schema
        :returns: version
        :rtype: int
        """
        schemas_to_version = self.subject_to_schema_versions[subject]
        version = schemas_to_version.get(avro_schema, None)
        if version is not None:
            return version

        url = '/'.join([self.url, 'subjects', subject])
        body = {'schema': str(avro_schema)}

        result, code = self._send_request(url, method='POST', body=body)
        if code == 404:
            log.error("Not found:" + str(code))
            return None
        elif not (code >= 200 and code <= 299):
            log.error("Unable to get version of a schema:" + str(code))
            return None
        assert isinstance(result, Dict)
        schema_id = result['id']
        version = result['version']
        self._cache_schema(avro_schema, schema_id, subject, version)
        return version

    def test_compatibility(self, subject: str, avro_schema: schema.Schema, version: str='latest') -> Optional[bool]:
        """
        POST /compatibility/subjects/(string: subject)/versions/(versionId: version)

        Test the compatibility of a candidate parsed schema for a given subject.

        By default the latest version is checked against.
        :param: str subject: subject name
        :param: schema avro_schema: Avro schema
        :return: True if compatible, False if not compatible
        :rtype: bool
        """
        url = '/'.join([self.url, 'compatibility', 'subjects', subject,
                        'versions', str(version)])
        body = {'schema': str(avro_schema)}
        try:
            result, code = self._send_request(url, method='POST', body=body)
            if code == 404:
                log.error(("Subject or version not found:" + str(code)))
                return False
            elif code == 422:
                log.error(("Invalid subject or schema:" + str(code)))
                return False
            elif code >= 200 and code <= 299:
                assert isinstance(result, Dict)
                return result.get('is_compatible')
            else:
                log.error("Unable to check the compatibility: " + str(code))
                return False
        except Exception as e:
            log.error("_send_request() failed: %s", e)
            return False

    def update_compatibility(self, level: str, subject: Optional[str]=None) -> str:
        """
        PUT /config/(string: subject)

        Update the compatibility level for a subject.  Level must be one of:

        :param str level: ex: 'NONE','FULL','FORWARD', or 'BACKWARD'
        """
        if level not in VALID_LEVELS:
            raise ClientError("Invalid level specified: %s" % (str(level)))

        url = '/'.join([self.url, 'config'])
        if subject:
            url += '/' + subject

        body = {"compatibility": level}
        result, code = self._send_request(url, method='PUT', body=body)
        if code >= 200 and code <= 299:
            assert isinstance(result, Dict)
            return result['compatibility']
        else:
            raise ClientError("Unable to update level: %s. Error code: %d" % (str(level), code))

    def get_compatibility(self, subject: Optional[str]=None) -> bool:
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

        result, code = self._send_request(url)
        is_successful_request = code >= 200 and code <= 299
        if not is_successful_request:
            raise ClientError('Unable to fetch compatibility level. Error code: %d' % code)

        assert isinstance(result, Dict)
        compatibility = result.get('compatibilityLevel', None)
        if compatibility not in VALID_LEVELS:
            if compatibility is None:
                error_msg_suffix = 'No compatibility was returned'
            else:
                error_msg_suffix = str(compatibility)
            raise ClientError('Invalid compatibility level received: %s' % error_msg_suffix)

        # Can't be None, as that's not in VALID_LEVELS 
        return cast(bool, compatibility)
