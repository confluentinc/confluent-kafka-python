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

from tests.avro import mock_registry
from tests.avro import data_gen
from confluent_kafka.avro.cached_schema_registry_client import CachedSchemaRegistryClient
from confluent_kafka import avro


class TestCacheSchemaRegistryClient(unittest.TestCase):
    def setUp(self):
        self.server = mock_registry.ServerThread(0)
        self.server.start()
        self.client = CachedSchemaRegistryClient('http://127.0.0.1:' + str(self.server.server.server_port))

    def tearDown(self):
        self.server.shutdown()
        self.server.join()

    def test_register(self):
        parsed = avro.loads(data_gen.BASIC_SCHEMA)
        client = self.client
        schema_id = client.register('test', parsed)
        self.assertTrue(schema_id > 0)
        self.assertEqual(len(client.id_to_schema), 1)

    def test_multi_subject_register(self):
        parsed = avro.loads(data_gen.BASIC_SCHEMA)
        client = self.client
        schema_id = client.register('test', parsed)
        self.assertTrue(schema_id > 0)

        # register again under different subject
        dupe_id = client.register('other', parsed)
        self.assertEqual(schema_id, dupe_id)
        self.assertEqual(len(client.id_to_schema), 1)

    def test_dupe_register(self):
        parsed = avro.loads(data_gen.BASIC_SCHEMA)
        subject = 'test'
        client = self.client
        schema_id = client.register(subject, parsed)
        self.assertTrue(schema_id > 0)
        latest = client.get_latest_schema(subject)

        # register again under same subject
        dupe_id = client.register(subject, parsed)
        self.assertEqual(schema_id, dupe_id)
        dupe_latest = client.get_latest_schema(subject)
        self.assertEqual(latest, dupe_latest)

    def assertLatest(self, meta_tuple, sid, schema, version):
        self.assertNotEqual(sid, -1)
        self.assertNotEqual(version, -1)
        self.assertEqual(meta_tuple[0], sid)
        self.assertEqual(meta_tuple[1], schema)
        self.assertEqual(meta_tuple[2], version)

    def test_getters(self):
        parsed = avro.loads(data_gen.BASIC_SCHEMA)
        client = self.client
        subject = 'test'
        version = client.get_version(subject, parsed)
        self.assertEqual(version, None)
        schema = client.get_by_id(1)
        self.assertEqual(schema, None)
        latest = client.get_latest_schema(subject)
        self.assertEqual(latest, (None, None, None))

        # register
        schema_id = client.register(subject, parsed)
        latest = client.get_latest_schema(subject)
        version = client.get_version(subject, parsed)
        self.assertLatest(latest, schema_id, parsed, version)

        fetched = client.get_by_id(schema_id)
        self.assertEqual(fetched, parsed)

    def test_multi_register(self):
        basic = avro.loads(data_gen.BASIC_SCHEMA)
        adv = avro.loads(data_gen.ADVANCED_SCHEMA)
        subject = 'test'
        client = self.client

        id1 = client.register(subject, basic)
        latest1 = client.get_latest_schema(subject)
        v1 = client.get_version(subject, basic)
        self.assertLatest(latest1, id1, basic, v1)

        id2 = client.register(subject, adv)
        latest2 = client.get_latest_schema(subject)
        v2 = client.get_version(subject, adv)
        self.assertLatest(latest2, id2, adv, v2)

        self.assertNotEqual(id1, id2)
        self.assertNotEqual(latest1, latest2)
        # ensure version is higher
        self.assertTrue(latest1[2] < latest2[2])

        client.register(subject, basic)
        latest3 = client.get_latest_schema(subject)
        # latest should not change with a re-reg
        self.assertEqual(latest2, latest3)

    def hash_func(self):
        return hash(str(self))

    def test_cert_no_key(self):
        with self.assertRaises(ValueError):
            self.client = CachedSchemaRegistryClient(url='https://127.0.0.1:65534',
                                                     cert_location='/path/to/cert')

    def test_cert_with_key(self):
        self.client = CachedSchemaRegistryClient(url='https://127.0.0.1:65534',
                                                 cert_location='/path/to/cert',
                                                 key_location='/path/to/key')
        self.assertTupleEqual(('/path/to/cert', '/path/to/key'), self.client._session.cert)

    def test_cert_path(self):
            self.client = CachedSchemaRegistryClient(url='https://127.0.0.1:65534',
                                                     ca_location='/path/to/ca')
            self.assertEqual('/path/to/ca', self.client._session.verify)

    def test_context(self):
            with self.client as c:
                parsed = avro.loads(data_gen.BASIC_SCHEMA)
                schema_id = c.register('test', parsed)
                self.assertTrue(schema_id > 0)
                self.assertEqual(len(c.id_to_schema), 1)
