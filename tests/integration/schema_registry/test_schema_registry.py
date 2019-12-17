#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
#
# Copyright 2019 Confluent Inc.
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

from uuid import uuid4

import pytest

from .test_avro import run_avro_loop


@pytest.mark.parametrize("schema_def", ['basic_schema', 'adv_schema',
                                        'primitive_float', 'primitive_string',
                                        'user_v1', 'user_v2'])
def test_schema_registry_client(sr_fixture,
                                schema_fixture, schema_def):
    from confluent_kafka import avro

    sr = avro.CachedSchemaRegistryClient({'url': sr_fixture.schema_registry})

    subject = str(uuid4())

    schema = schema_fixture(schema_def)

    schema_id = sr.register(subject, schema)
    assert schema == sr.get_by_id(schema_id)
    latest_id, latest_schema, latest_version = sr.get_latest_schema(subject)
    assert schema == latest_schema
    assert sr.get_version(subject, schema) == latest_version
    sr.update_compatibility("FULL", subject)
    assert sr.get_compatibility(subject) == "FULL"
    assert sr.test_compatibility(subject, schema)
    assert sr.delete_subject(subject) == [1]


# TODO: Add SSL support to trivup. Until then this is a manual setup
@pytest.mark.xfail
def test_avro_https(sr_fixture,
                    error_cb_fixture, print_commit_callback_fixture):
    base_conf = sr_fixture.client_conf

    base_conf.update({'error_cb': error_cb_fixture})

    consumer_conf = dict(base_conf, **{'group.id': uuid4(),
                                       'session.timeout.ms': 6000,
                                       'enable.auto.commit': False,
                                       'on_commit': print_commit_callback_fixture,
                                       'auto.offset.reset': 'earliest'})

    run_avro_loop(base_conf, consumer_conf)


# TODO: Add SSL/HTTP Auth support to trivup. Until then this is a manual setup
@pytest.mark.xfail
@pytest.mark.parametrize('auth_conf', [{'schema.registry.basic.auth.credentials.source': 'URL'},
                                       {
                                           'schema.registry.basic.auth.credentials.source': 'USER_INFO',
                                           'schema.registry.basic.auth.user.info': 'tester:secret'
                                       },
                                       {
                                           'schema.registry.basic.auth.credentials.source': 'sasl_inherit',
                                           'schema.registry.sasl.username': 'tester',
                                           'schema.registry.sasl.password': 'secret'
                                       }
                                       ])
def test_avro_basic_auth(sr_fixture, schema_fixture, error_cb_fixture, auth_conf):
    base_conf = sr_fixture.client_conf
    base_conf.update({'error_cb': error_cb_fixture})

    consumer_conf = dict({'group.id': uuid4(),
                          'session.timeout.ms': 6000,
                          'enable.auto.commit': False,
                          'auto.offset.reset': 'earliest'
                          }, **base_conf)

    run_avro_loop(dict(base_conf, **auth_conf), dict(consumer_conf, **auth_conf), schema_fixture)
