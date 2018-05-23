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

import unittest
import pytest
from avro import schema
from tests.avro import data_gen
from confluent_kafka import avro


WRONG_SCHEMA = """\
{
  "type" : "record",
  "name" : "string_key",
  "namespace" : "OrbitDbProducer",
  "fields" : [ {
    "name" : "key",
    "type" : "array",
    "items": "string"
  } ]
} """


class TestUtil(unittest.TestCase):

    def test_schema_from_string(self):
        parsed = avro.loads(data_gen.BASIC_SCHEMA)
        self.assertTrue(isinstance(parsed, schema.Schema))

    def test_schema_from_file(self):
        parsed = avro.load(data_gen.get_schema_path('adv_schema.avsc'))
        self.assertTrue(isinstance(parsed, schema.Schema))

    def test_schema_load_parse_error(self):
        with pytest.raises(avro.ClientError) as excinfo:
            avro.loads(WRONG_SCHEMA)
        assert 'Schema parse failed: Type property "array" not a valid Avro schema' in str(excinfo.value)
