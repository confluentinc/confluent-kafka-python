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

import pytest
from trivup.clusters.KafkaCluster import KafkaCluster

from confluent_kafka import avro


@pytest.fixture(scope="package")
def schema_registry_cluster_fixture():
    global kafka_cluster

    kafka_cluster = KafkaCluster(with_sr=True)
    kafka_cluster.sr.wait_operational()
    kafka_cluster._client_conf['schema.registry.url'] = kafka_cluster.sr.get('url')

    yield kafka_cluster

    kafka_cluster.stop(force=False, timeout=60)


@pytest.fixture(scope="package")
def schema_fixture():
    return _load_schema


def _load_schema(avsc_name):
    avsc_dir = os.path.join(os.path.dirname(__file__), 'data')
    return avro.load(os.path.join(avsc_dir, ".".join([avsc_name, "avsc"])))
