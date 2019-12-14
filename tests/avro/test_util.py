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
# limit
#


#
# derived from https://github.com/verisign/python-confluent-schemaregistry.git
#

import pytest
from avro import schema
from confluent_kafka import avro


def test_primitive_string(schema_fixture):
    parsed = schema_fixture('basic_schema')
    assert isinstance(parsed, schema.Schema)


def test_schema_from_file(schema_fixture):
    parsed = schema_fixture('adv_schema')
    assert isinstance(parsed, schema.Schema)


def test_schema_load_parse_error(schema_fixture):
    with pytest.raises(avro.ClientError) as excinfo:
        schema_fixture("invalid_schema")
    assert 'Schema parse failed:' in str(excinfo.value)
