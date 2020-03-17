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

# Python 2 considers int an instance of str

try:
    string_type = basestring  # noqa
except NameError:
    string_type = str

from requests import utils

from ..config import Config
from .serdes import SubjectNameStrategy, TopicRecordNameStrategy, \
    TopicNameStrategy, RecordNameStrategy

"""
REST Client
"""
URL = 'url'
REST_CLIENT = 'rest.client'
"""
Serializer Config
"""
SUBJECT_NAME_STRATEGY = 'subject.name.strategy'
AUTO_REGISTRATION = 'auto.register.schemas'

"""
TLS configuration properties
"""
CA_LOCATION = 'ssl.ca.location'
CERTIFICATE_LOCATION = 'ssl.certificate.location'
KEY_LOCATION = 'ssl.key.location'

"""
Basic Auth configuration properties
"""
BASIC_AUTH_CREDENTIALS_SOURCE = 'basic.auth.credentials.source'
BASIC_AUTH_USER_INFO = 'basic.auth.user.info'
SASL_USERNAME = 'sasl.username'
SASL_PASSWORD = 'sasl.password'
SASL_MECHANISM = 'sasl.mechanism'


class BasicAuthCredentialSource(object):
    """
    Specifies the configuration property(ies) that provide the basic
        authentication credentials.
    """
    USERINFO = "USERINFO"
    SASL_INHERIT = "SASL_INHERIT"
    URL = "URL"


class SchemaRegistryConfig(Config):
    """

    Keyword Args:
        - url (str): URL for Schema Registry instance.

        - schema.registry.ca.location (str, optional):
        - schema.registry.certificate.location (str, optional):
        - schema.registry.key.location (str, optional):

        - schema.registry.basic.auth.credentials.source (str, optional):
            Specifies the configuration property(ies) that provide the basic
            authentication credentials.

            USER_INFO: Credentials are specified via the
            ``schema.registry.basic.auth.user.info`` config property.

            SASL_INHERIT: Credentials are specified via the ``sasl.username``
            and ``sasl.password`` configuration properties.

            URL(default): Credentials are pulled from the URL if available.

        - schema.registry.basic.auth.user.info (str, optional): Basic auth
            credentials in the form {username}:{password}.
        - schema.registry.sasl.username (str, optional): SASL username for use
            with the PLAIN mechanism.
        - schema.registry.sasl.password (str, optional): SASL password for use
            with the PLAIN mechanism.
        - schema.registry.sasl.mechanism (str, optional): SASL mechanism to
            use for authentication. Only ``PLAIN`` is supported by
            ``basic.auth.credentials.source`` SASL_INHERIT.

    """
    properties = {
        URL: None,
        CA_LOCATION: None,
        CERTIFICATE_LOCATION: None,
        KEY_LOCATION: None,
        BASIC_AUTH_CREDENTIALS_SOURCE: BasicAuthCredentialSource.URL,
        BASIC_AUTH_USER_INFO: None,
        SASL_USERNAME: None,
        SASL_PASSWORD: None,
        SASL_MECHANISM: 'GSSAPI',
    }

    def _err_str(self, val):
        return self.prefix + val

    def validate(self):
        # URL validation test requires a string method
        if not isinstance(self[URL], basestring):
            raise TypeError("{} must be a string".format(self._err_str(URL)))

        if not self[URL] or not self[URL].startswith("http"):
            raise ValueError('Invalid URL {}'.format(self[URL]))

        # Ensure we have both keys if set
        if bool(self[CERTIFICATE_LOCATION]) != bool(self[KEY_LOCATION]):
            raise ValueError("{} and {} must be set together".format(
                self._err_str(CERTIFICATE_LOCATION),
                self._err_str(KEY_LOCATION)))

        if self[BASIC_AUTH_CREDENTIALS_SOURCE] == \
                BasicAuthCredentialSource.SASL_INHERIT:
            if bool(self[SASL_USERNAME]) != bool(self[SASL_PASSWORD]):
                raise ValueError("{} and {} must be set together".format(
                    self._err_str(SASL_USERNAME),
                    self._err_str(SASL_PASSWORD)))

            if self[BASIC_AUTH_CREDENTIALS_SOURCE] == \
                    BasicAuthCredentialSource.SASL_INHERIT and \
                    'GSSAPI' == self[SASL_MECHANISM]:
                raise ValueError("{} does not support GSSAPI".format(
                    self._err_str(BASIC_AUTH_CREDENTIALS_SOURCE)))

        if self[BASIC_AUTH_CREDENTIALS_SOURCE] == \
                BasicAuthCredentialSource.USERINFO:

            if self[BASIC_AUTH_USER_INFO] is None:
                raise ValueError("{} is required when {} is {}".format(
                    self._err_str(BASIC_AUTH_USER_INFO),
                    self._err_str(BASIC_AUTH_CREDENTIALS_SOURCE),
                    BasicAuthCredentialSource.USERINFO))

    @property
    def Url(self):
        return self[URL]

    @property
    def CA(self):
        return self[CA_LOCATION]

    @property
    def Certificate(self):
        if CERTIFICATE_LOCATION in self.data:
            return self[CERTIFICATE_LOCATION], self[KEY_LOCATION]
        return None

    @property
    def Credentials(self):
        if self[BASIC_AUTH_CREDENTIALS_SOURCE] == BasicAuthCredentialSource.SASL_INHERIT:
            return self[SASL_USERNAME], self[SASL_PASSWORD]
        if self[BASIC_AUTH_CREDENTIALS_SOURCE] == BasicAuthCredentialSource.USERINFO:
            return self[BASIC_AUTH_USER_INFO].split(':')
        if self[BASIC_AUTH_CREDENTIALS_SOURCE] == BasicAuthCredentialSource.URL:
            return utils.get_auth_from_url(self[URL])


class RegistrySerializerConfig(Config):
    include = [SchemaRegistryConfig]

    properties = {
        SUBJECT_NAME_STRATEGY: SubjectNameStrategy.TopicNameStrategy,
        AUTO_REGISTRATION: True,
    }

    def validate(self):
        if not isinstance(self.SubjectNameStrategy, SubjectNameStrategy):
            raise TypeError("{} must be an instance of {}".format(
                [SUBJECT_NAME_STRATEGY], str(SubjectNameStrategy)))
        if not isinstance(self[AUTO_REGISTRATION], bool):
            raise TypeError("{} must be an instance of {}".format(
                self.prefix + AUTO_REGISTRATION, bool))

    @property
    def SchemaIDStrategy(self):
        return self[AUTO_REGISTRATION]

    @property
    def SubjectNameStrategy(self):
        if self[SUBJECT_NAME_STRATEGY] \
                is SubjectNameStrategy.TopicNameStrategy:
            return TopicNameStrategy()
        if self[SUBJECT_NAME_STRATEGY] \
                is SubjectNameStrategy.TopicRecordNameStrategy:
            return TopicRecordNameStrategy()
        if self[SUBJECT_NAME_STRATEGY] \
                is SubjectNameStrategy.RecordNameStrategy:
            return RecordNameStrategy()
        raise TypeError("Invalid {}: {}".format(
            SUBJECT_NAME_STRATEGY, str(self[SUBJECT_NAME_STRATEGY])))
