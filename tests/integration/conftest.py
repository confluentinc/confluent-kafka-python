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
import os

import pytest

from tests.integration.cluster_fixture import TrivupFixture

work_dir = os.path.dirname(os.path.realpath(__file__))


@pytest.fixture(scope="package")
def kafka_cluster():

    cluster = TrivupFixture({'with_sr': True,
                             'broker_conf': ['transaction.state.log.replication.factor=1',
                                             'transaction.state.log.min.isr=1']})
    try:
        yield cluster
    finally:
        cluster.stop()


@pytest.fixture()
def load_file():
    def get_handle(name):
        with open(os.path.join(work_dir, 'schema_registry', 'data', name)) as fd:
            return fd.read()
    return get_handle
