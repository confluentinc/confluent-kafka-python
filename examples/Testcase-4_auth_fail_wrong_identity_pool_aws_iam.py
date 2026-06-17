#!/usr/bin/env python3
#
# Copyright 2026 Confluent Inc.
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
"""Testcase-4: OAUTHBEARER auth fails with the wrong identity pool (AWS IAM).

Negative test. The oauth_cb returns a VALID AWS STS token but routes it to a
bogus identityPoolId, so Confluent Cloud rejects it. PASS means the client fails
to authenticate (AdminClient.list_topics raises) within the timeout.

Env:
    BOOTSTRAP_SERVERS      Confluent Cloud bootstrap     [required; or argv[1]]
    KAFKA_LOGICAL_CLUSTER  CC logical cluster id          [required]
    OIDC_AUDIENCE          STS token audience             [default https://confluent.cloud/oidc]
    AWS_STS_REGION         STS region                     [default eu-north-1]
    PROBE_TIMEOUT_SECONDS  list_topics timeout            [default 15]

Usage:
    python Testcase-4_auth_fail_wrong_identity_pool_aws_iam.py [bootstrap.servers]
"""

import os
import sys
import time

import boto3
from confluent_kafka import KafkaException
from confluent_kafka.admin import AdminClient

BOGUS_POOL = "pool-INVALID"


def get_aws_iam_token(audience, aws_region):
    sts = boto3.client('sts', region_name=aws_region)
    return sts.get_web_identity_token(
        Audience=[audience], SigningAlgorithm='ES384',
        DurationSeconds=300)['WebIdentityToken']


def main():
    bootstrap = sys.argv[1] if len(sys.argv) > 1 else os.environ.get('BOOTSTRAP_SERVERS')
    logical_cluster = os.environ.get('KAFKA_LOGICAL_CLUSTER')
    audience = os.environ.get('OIDC_AUDIENCE', 'https://confluent.cloud/oidc')
    aws_region = os.environ.get('AWS_STS_REGION', 'eu-north-1')
    timeout = int(os.environ.get('PROBE_TIMEOUT_SECONDS', '15'))

    missing = [k for k, v in (('BOOTSTRAP_SERVERS', bootstrap),
                              ('KAFKA_LOGICAL_CLUSTER', logical_cluster)) if not v]
    if missing:
        sys.stderr.write(f"Missing required config: {', '.join(missing)}\n")
        return 2

    cache = {}

    def oauth_cb(_config_str):
        if 'token' not in cache:
            cache['token'] = get_aws_iam_token(audience, aws_region)
        # Valid token, but routed to a non-existent identity pool -> CC rejects.
        return (cache['token'], time.time() + 300, "",
                {"logicalCluster": logical_cluster, "identityPoolId": BOGUS_POOL})

    print("=" * 70)
    print("Testcase-4: auth fails with the wrong identityPoolId")
    print(f"  bootstrap={bootstrap}  cluster={logical_cluster}  bad_pool={BOGUS_POOL}")
    print(f"  region={aws_region}  probe_timeout={timeout}s")
    print("=" * 70)

    errors = []
    conf = {
        'bootstrap.servers': bootstrap,
        'security.protocol': 'SASL_SSL',
        'sasl.mechanisms': 'OAUTHBEARER',
        'oauth_cb': oauth_cb,
        'error_cb': lambda err: errors.append(str(err)),
    }

    try:
        admin = AdminClient(conf)
        md = admin.list_topics(timeout=timeout)
        ok, detail = False, f"auth unexpectedly SUCCEEDED (saw {len(md.topics)} topics)"
    except KafkaException as exc:
        tail = f"  [error_cb: {errors[-1]}]" if errors else ""
        ok, detail = True, f"auth failed as expected: {exc}{tail}"

    print("=" * 70)
    print(f"{'PASS' if ok else 'FAIL'} — {detail}")
    print("=" * 70)
    return 0 if ok else 1


if __name__ == '__main__':
    sys.exit(main())
