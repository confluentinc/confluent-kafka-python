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

import os
import random

import pytest

from confluent_kafka import avro
from confluent_kafka.avro import MessageSerializer
from tests.avro.mock_registry import MockServer
from tests.avro.mock_schema_registry_client import MockSchemaRegistryClient


@pytest.fixture(scope="module")
def mock_schema_registry_cluster_fixture():
    mock_schema_registry_server = MockServer()
    yield mock_schema_registry_server


@pytest.fixture(scope="module")
def mock_schema_registry_client_fixture():
    mock_schema_registry_client = MockSchemaRegistryClient()
    yield mock_schema_registry_client


@pytest.fixture(scope="session")
def message_serializer_fixture():
    def serializer_init_fixture(client):
        return MessageSerializer(client)

    return serializer_init_fixture


@pytest.fixture(scope="package")
def schema_fixture():
    return _load_schema


def _load_schema(avsc_name):
    avsc_dir = os.path.join(os.path.dirname(__file__), 'data')
    return avro.load(os.path.join(avsc_dir, ".".join([avsc_name, "avsc"])))


NAMES = ['stefan', 'melanie', 'nick', 'darrel', 'kent', 'simon']
AGES = list(range(1, 10)) + [None]


def create_basic_item(i):
    return {
        'name': random.choice(NAMES) + '-' + str(i),
        'number': random.choice(AGES)
    }


BASIC_ITEMS = map(create_basic_item, range(1, 20))


def create_adv_item(i):
    friends = map(create_basic_item, range(1, 3))
    family = map(create_basic_item, range(1, 3))
    basic = create_basic_item(i)
    basic['family'] = dict(map(lambda bi: (bi['name'], bi), family))
    basic['friends'] = dict(map(lambda bi: (bi['name'], bi), friends))
    return basic


ADVANCED_ITEMS = map(create_adv_item, range(1, 20))
