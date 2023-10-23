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
from tests.integration.cluster_fixture import ByoFixture

work_dir = os.path.dirname(os.path.realpath(__file__))


def create_trivup_cluster(conf={}):
    trivup_fixture_conf = {'with_sr': True,
                           'debug': True,
                           'cp_version': '7.4.0',
                           'version': '3.4.0',
                           'broker_conf': ['transaction.state.log.replication.factor=1',
                                           'transaction.state.log.min.isr=1']}
    trivup_fixture_conf.update(conf)
    return TrivupFixture(trivup_fixture_conf)


def create_sasl_cluster(conf={}):
    trivup_fixture_conf = {'with_sr': False,
                           'version': '3.4.0',
                           'sasl_mechanism': "PLAIN",
                           'sasl_users': 'sasl_user=sasl_user',
                           'debug': True,
                           'cp_version': 'latest',
                           'broker_conf': ['transaction.state.log.replication.factor=1',
                                           'transaction.state.log.min.isr=1']}
    trivup_fixture_conf.update(conf)
    return TrivupFixture(trivup_fixture_conf)


def create_byo_cluster(conf):
    """
    The cluster's bootstrap.servers must be set in dict.
    """
    return ByoFixture(conf)


def kafka_cluster_fixture(
    brokers_env="BROKERS",
    sr_url_env="SR_URL",
    trivup_cluster_conf={}
):
    """
    If BROKERS environment variable is set to a CSV list of bootstrap servers
    an existing cluster is used.
    Additionally, if SR_URL environment variable is set the Schema-Registry
    client will use the given URL.

    If BROKERS is not set a TrivUp cluster is created and used.
    """

    bootstraps = os.environ.get(brokers_env, "")
    if bootstraps != "":
        conf = {"bootstrap.servers": bootstraps}
        sr_url = os.environ.get(sr_url_env, "")
        if sr_url != "":
            conf["schema.registry.url"] = sr_url
        print("Using ByoFixture with config from env variables: ", conf)
        cluster = create_byo_cluster(conf)
    else:
        cluster = create_trivup_cluster(trivup_cluster_conf)
    try:
        yield cluster
    finally:
        cluster.stop()


def sasl_cluster_fixture(
    trivup_cluster_conf={}
):
    """
    If BROKERS environment variable is set to a CSV list of bootstrap servers
    an existing cluster is used.
    Additionally, if SR_URL environment variable is set the Schema-Registry
    client will use the given URL.

    If BROKERS is not set a TrivUp cluster is created and used.
    """

    cluster = create_sasl_cluster(trivup_cluster_conf)
    try:
        yield cluster
    finally:
        cluster.stop()


@pytest.fixture(scope="package")
def kafka_cluster():
    for fixture in kafka_cluster_fixture():
        yield fixture


@pytest.fixture(scope="package")
def sasl_cluster(request):
    for fixture in sasl_cluster_fixture(request.param):
        yield fixture


@pytest.fixture()
def load_file():
    def get_handle(name):
        with open(os.path.join(work_dir, 'schema_registry', 'data', name)) as fd:
            return fd.read()
    return get_handle
