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
import os
import sys

from confluent_kafka import avro

if sys.version_info[0] < 3:
    import unittest
else:
    import unittest2 as unittest
from confluent_kafka.avro import AvroProducer

avsc_dir = os.path.dirname(os.path.realpath(__file__))


class TestAvroProducer(unittest.TestCase):
    def setUp(self):
        pass

    def test_instantiation(self):
        obj = AvroProducer({'schema.registry.url': 'http://127.0.0.1:0'})
        self.assertTrue(isinstance(obj, AvroProducer))
        self.assertNotEqual(obj, None)

    def test_Produce(self):
        producer = AvroProducer({'schema.registry.url': 'http://127.0.0.1:0'})
        valueSchema = avro.load(os.path.join(avsc_dir, "basic_schema.avsc"))
        try:
            producer.produce(topic='test', value={"name": 'abc"'}, value_schema=valueSchema, key='mykey')
            self.fail("Should expect key_schema")
        except Exception as e:
            pass

    def test_produce_arguments(self):
        value_schema = avro.load(os.path.join(avsc_dir, "basic_schema.avsc"))
        producer = AvroProducer({'schema.registry.url': 'http://127.0.0.1:0'}, default_value_schema=value_schema)

        try:
            producer.produce(topic='test', value={"name": 'abc"'})
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            if exc_type.__name__ == 'SerializerError':
                self.fail()

    def test_produce_arguments_list(self):
        producer = AvroProducer({'schema.registry.url': 'http://127.0.0.1:0'})
        try:
            producer.produce(topic='test', value={"name": 'abc"'}, key='mykey')
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            if exc_type.__name__ == 'SerializerError':
                pass


def suite():
    return unittest.TestLoader().loadTestsFromTestCase(TestAvroProducer)
