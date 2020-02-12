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

from cluster_fixture import TrivupFixture, ExternalClusterFixture


@pytest.fixture(scope="package")
def kafka_cluster():
    env_conf = {k[6:]: v for (k, v) in os.environ.items() if k.startswith('KAFKA')}

    if bool(env_conf):
        return ExternalClusterFixture(env_conf)

    return TrivupFixture({'broker_cnt': 1,
                          'broker_conf': ['transaction.state.log.replication.factor=1',
                                          'transaction.state.log.min.isr=1']})
