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

from tests.common import TestUtils
from tests.integration.cluster_fixture import ByoFixture, TrivupFixture

work_dir = os.path.dirname(os.path.realpath(__file__))


def create_trivup_cluster(conf={}):
    trivup_fixture_conf = {
        'with_sr': True,
        'debug': True,
        'cp_version': '7.6.0',
        'kraft': TestUtils.use_kraft(),
        'version': TestUtils.broker_version(),
        'broker_conf': TestUtils.broker_conf(),
    }
    trivup_fixture_conf.update(conf)
    return TrivupFixture(trivup_fixture_conf)


def create_sasl_cluster(conf={}):
    trivup_fixture_conf = {
        'with_sr': False,
        'version': TestUtils.broker_version(),
        'sasl_mechanism': "PLAIN",
        'kraft': TestUtils.use_kraft(),
        'sasl_users': 'sasl_user=sasl_user',
        'debug': True,
        'cp_version': 'latest',
        'broker_conf': TestUtils.broker_conf(),
    }
    trivup_fixture_conf.update(conf)
    return TrivupFixture(trivup_fixture_conf)


def create_byo_cluster(conf):
    """
    The cluster's bootstrap.servers must be set in dict.
    """
    return ByoFixture(conf)


def _aws_iam_oauth_cb():
    """OAUTHBEARER DEFAULT-mode callback: mint a token via AWS STS
    GetWebIdentityToken (KIP-932 / Confluent Cloud AWS IAM).

    boto3 picks up the ambient (EC2/role) credentials; the returned tuple is
    what confluent-kafka expects: (token, expiry_epoch, principal, {extensions}).
    The logicalCluster + identityPoolId extensions route the token to the right
    CC cluster + identity pool.
    """
    import time

    import boto3

    audience = os.environ.get("OIDC_AUDIENCE", "https://confluent.cloud/oidc")
    region = os.environ.get("AWS_STS_REGION", "eu-north-1")
    logical_cluster = os.environ["KAFKA_LOGICAL_CLUSTER"]
    identity_pool_id = os.environ["IDENTITY_POOL_ID"]
    # Reported token lifetime: librdkafka refreshes/reauths at ~80% of this, so
    # 60s -> a refresh roughly every ~48s, exercising reauth during the run.
    # (The STS token itself is valid 300s; we just report shorter to force it.)
    lifetime = int(os.environ.get("OAUTH_TOKEN_LIFETIME", "60"))

    def oauth_cb(_config_str):
        token = boto3.client("sts", region_name=region).get_web_identity_token(
            Audience=[audience], SigningAlgorithm="ES384",
            DurationSeconds=300)["WebIdentityToken"]
        if os.environ.get("OAUTH_CB_DEBUG"):
            print(f"[oauth_cb] minted AWS STS token (len={len(token)}, "
                  f"pool={identity_pool_id}, lifetime={lifetime}s)", flush=True)
        return (token, time.time() + lifetime, "",
                {"logicalCluster": logical_cluster,
                 "identityPoolId": identity_pool_id})

    return oauth_cb


def oauthbearer_aws_conf(brokers_env="BROKERS"):
    """Full OAUTHBEARER (AWS IAM) client config for Confluent Cloud, or None.

    Auto-enabled when KAFKA_LOGICAL_CLUSTER and IDENTITY_POOL_ID are set (i.e.
    we're targeting a CC identity pool). The bootstrap comes from
    BOOTSTRAP_SERVERS (falling back to the brokers_env / BROKERS var). The
    oauth_cb callback can't live in a properties file, so it's attached here
    and flows to every client via ByoFixture.client_conf().
    """
    if not (os.environ.get("KAFKA_LOGICAL_CLUSTER")
            and os.environ.get("IDENTITY_POOL_ID")):
        return None
    bootstrap = os.environ.get("BOOTSTRAP_SERVERS") or os.environ.get(brokers_env, "")
    if not bootstrap:
        raise ValueError(
            "BOOTSTRAP_SERVERS (or BROKERS) must be set for OAUTHBEARER tests")
    return {
        "bootstrap.servers": bootstrap,
        "security.protocol": "SASL_SSL",
        "sasl.mechanisms": "OAUTHBEARER",
        "oauth_cb": _aws_iam_oauth_cb(),
        # Quiet the KIP-714 client-telemetry INFO logs ("GETSUBSCRIPTIONS ...
        # Telemetry client instance id changed"); not needed for tests.
        "enable.metrics.push": False,
    }


def parse_test_conf(path):
    """
    Parse a librdkafka-style properties file (key=value, '#' comments)
    into a config dict. Used for the TEST_CONF env var so an existing
    cluster's full client config (incl. SASL credentials) can be supplied
    without baking it into env variables.
    """
    conf = {}
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" not in line:
                raise ValueError(f"Malformed line in {path!r}: {line!r}")
            key, value = line.split("=", 1)
            conf[key.strip()] = value.strip()
    return conf


def kafka_cluster_fixture(brokers_env="BROKERS", sr_url_env="SR_URL", trivup_cluster_conf={}):
    """
    If the TEST_CONF environment variable points at a librdkafka-style
    properties file, its full client config (bootstrap.servers, SASL
    credentials, etc.) is used against an existing cluster.

    Otherwise, if BROKERS is set to a CSV list of bootstrap servers an
    existing cluster is used; additionally, if SR_URL is set the
    Schema-Registry client will use the given URL.

    If neither is set a TrivUp cluster is created and used.
    """

    test_conf_path = os.environ.get("TEST_CONF", "")
    bootstraps = os.environ.get(brokers_env, "")
    oauthbearer_conf = oauthbearer_aws_conf(brokers_env)
    if oauthbearer_conf is not None:
        sr_url = os.environ.get(sr_url_env, "")
        if sr_url != "":
            oauthbearer_conf["schema.registry.url"] = sr_url
        print("Using ByoFixture with OAUTHBEARER (AWS IAM) config for Confluent Cloud")
        cluster = create_byo_cluster(oauthbearer_conf)
    elif test_conf_path != "":
        conf = parse_test_conf(test_conf_path)
        if conf.get("bootstrap.servers", "") == "":
            raise ValueError(f"'bootstrap.servers' must be set in {test_conf_path!r}")
        print(f"Using ByoFixture with config from TEST_CONF={test_conf_path}")
        cluster = create_byo_cluster(conf)
    elif bootstraps != "":
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


def sasl_cluster_fixture(trivup_cluster_conf={}):
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


@pytest.fixture(scope="session")
def kafka_cluster():
    for fixture in kafka_cluster_fixture():
        yield fixture


@pytest.fixture(scope="session")
def sasl_cluster(request):
    for fixture in sasl_cluster_fixture(request.param):
        yield fixture


@pytest.fixture()
def load_file():
    def get_handle(name):
        with open(os.path.join(work_dir, 'schema_registry', 'data', name)) as fd:
            return fd.read()

    return get_handle
