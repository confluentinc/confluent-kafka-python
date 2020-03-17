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

import pytest

from confluent_kafka import IntegerSerializer, SerializingProducer, \
    KafkaException
from confluent_kafka.config import Config, ClientConfig
from confluent_kafka.schema_registry import SchemaRegistryConfig
from confluent_kafka.schema_registry.config import RegistrySerializerConfig, \
    AUTO_REGISTRATION, BasicAuthCredentialSource
from confluent_kafka.serialization import StringSerializer


def test_conf_unknown_property():
    """
    Unknown configs should raise ValueError

    """

    class ConfTest(Config):
        include = [SchemaRegistryConfig.defer(prefix='schema.registry.'),
                   RegistrySerializerConfig.defer()]

    with pytest.raises(ValueError) as ve:
        ConfTest({'schema.registry.url': 'http://localhost:9092',
                  AUTO_REGISTRATION: True,
                  'whatamIdoingHere': 'noidea'})
    assert 'whatamIdoingHere' in str(ve)


@pytest.mark.parametrize("conf, error, contains", [
    ({'url': 'htt://localhost:8081'}, ValueError, 'Invalid URL'),
    ({'url': "http://localhost:8091",
      'ssl.certificate.location': '/no/keys/here'}, ValueError, 'set together'),
    ({'url': "http://localhost:8091",
      'basic.auth.credentials.source': BasicAuthCredentialSource.USERINFO},
     ValueError, 'basic.auth.user.info'),
    ({'url': "http://localhost:8091",
      'basic.auth.credentials.source': BasicAuthCredentialSource.SASL_INHERIT},
     ValueError, 'GSSAPI'),
    ({'url': "http://localhost:8091",
      'subject.name.strategy': dict()}, TypeError, 'Invalid subject.name.strategy'),
    ({'url': "http://localhost:8091",
      'auto.register.schemas': 1}, TypeError, 'must be an instance of <type \'bool\'>')
])
def test_conf_validations(conf, error, contains):
    """
    Ensures validations are executed and enforced.

    """

    class ConfTest(Config):
        include = [SchemaRegistryConfig,
                   RegistrySerializerConfig]

    with pytest.raises(error) as e:
        ConfTest(conf)
    assert contains in str(e)


def test_conf_RegistryClient():
    """
    Config objects act as a super simplistic vehicle for dependency injection.

    This test ensures we get a proper SR instance from the configuration.

    """

    class ConfTest(Config):
        include = [SchemaRegistryConfig.defer(prefix='schema.registry.')]

    conf = ConfTest({'schema.registry.url': 'http://localhost:18911'})

    sr = conf.SchemaRegistryConfig.SchemaRegistry

    with pytest.raises(IOError) as ce:
        sr.get_subjects()
    assert '/subjects' in str(ce)


def test_ssl_properties():
    """
    Ensure property set ssl configs are accessible via conf property

    """

    class ConfTest(Config):
        include = [SchemaRegistryConfig]

    conf = ConfTest({'url': 'http://localhost:18911',
                     'ssl.certificate.location': '/secure/certs',
                     'ssl.key.location': '/secure/keys',
                     'ssl.ca.location': '/secure/ca'})

    assert conf.SchemaRegistryConfig.Certificate == ('/secure/certs',
                                                     '/secure/keys')

    assert conf.SchemaRegistryConfig.CA == '/secure/ca'


def test_client_conf():
    class ConfTest(Config):
        include = [SchemaRegistryConfig.defer(),
                   ClientConfig.defer()]

    key_serializer = StringSerializer()
    value_serializer = IntegerSerializer()

    conf = ConfTest({'bootstrap.servers': 'localhost:8912',
                     'url': 'http://localhost:18911',
                     'key.serializer': key_serializer,
                     'value.serializer': value_serializer})

    key = u'Jämtland'
    value = 123

    assert conf.ClientConfig.KeySerializer == key_serializer
    assert conf.ClientConfig.ValueSerializer == value_serializer

    assert conf.ClientConfig.KeySerializer(key, None) == key_serializer(
        key, None)
    assert conf.ClientConfig.ValueSerializer(value, None) == value_serializer(
        value, None)

    SerializingProducer(conf.ClientConfig)

    with pytest.raises(KafkaException):
        SerializingProducer(ClientConfig({'bootstrap.servers': 'localhost:8912',
                                          'bad.config': 'throws KafkaError'}))
