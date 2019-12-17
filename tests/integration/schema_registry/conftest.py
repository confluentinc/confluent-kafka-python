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
from ..cluster_fixture import ClusterFixture


from confluent_kafka import avro


@pytest.fixture(scope="package")
def sr_fixture():
    return ClusterFixture(with_sr=True)


@pytest.fixture(scope="package")
def schema_fixture():
    return _load_schema


def _load_schema(avsc_name):
    avsc_dir = os.path.join(os.path.dirname(__file__), 'data')
    return avro.load(os.path.join(avsc_dir, ".".join([avsc_name, "avsc"])))
