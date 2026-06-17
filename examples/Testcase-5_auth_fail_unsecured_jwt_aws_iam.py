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
"""Testcase-5: OAUTHBEARER auth fails with an unsecured (alg=none) JWT (AWS IAM).

Negative test. The oauth_cb returns a hand-built unsecured JWT (header alg=none,
empty signature) instead of a real STS token, so Confluent Cloud rejects it.
PASS means the client fails to authenticate (AdminClient.list_topics raises)
within the timeout — i.e. CC enforces secured tokens.

Env:
    BOOTSTRAP_SERVERS      Confluent Cloud bootstrap   [required; or argv[1]]
    KAFKA_LOGICAL_CLUSTER  CC logical cluster id        [required]
    IDENTITY_POOL_ID       CC identity pool id          [required]
    OIDC_AUDIENCE          JWT aud claim                [default https://confluent.cloud/oidc]
    PROBE_TIMEOUT_SECONDS  list_topics timeout          [default 15]

Usage:
    python Testcase-5_auth_fail_unsecured_jwt_aws_iam.py [bootstrap.servers]
"""

import base64
import json
import os
import sys
import time

from confluent_kafka import KafkaException
from confluent_kafka.admin import AdminClient


def _b64url(raw):
    return base64.urlsafe_b64encode(raw).rstrip(b'=').decode()


def make_unsecured_jwt(audience):
    """alg=none JWT with an empty signature — Confluent Cloud must reject this."""
    header = _b64url(json.dumps({"alg": "none", "typ": "JWT"}).encode())
    now = int(time.time())
    payload = _b64url(json.dumps({
        "sub": "aws-iam-negative-test", "aud": audience,
        "iss": "negative-test", "iat": now, "exp": now + 300}).encode())
    return f"{header}.{payload}."


def main():
    bootstrap = sys.argv[1] if len(sys.argv) > 1 else os.environ.get('BOOTSTRAP_SERVERS')
    logical_cluster = os.environ.get('KAFKA_LOGICAL_CLUSTER')
    identity_pool_id = os.environ.get('IDENTITY_POOL_ID')
    audience = os.environ.get('OIDC_AUDIENCE', 'https://confluent.cloud/oidc')
    timeout = int(os.environ.get('PROBE_TIMEOUT_SECONDS', '15'))

    missing = [k for k, v in (('BOOTSTRAP_SERVERS', bootstrap),
                              ('KAFKA_LOGICAL_CLUSTER', logical_cluster),
                              ('IDENTITY_POOL_ID', identity_pool_id)) if not v]
    if missing:
        sys.stderr.write(f"Missing required config: {', '.join(missing)}\n")
        return 2

    def oauth_cb(_config_str):
        return (make_unsecured_jwt(audience), time.time() + 300, "",
                {"logicalCluster": logical_cluster, "identityPoolId": identity_pool_id})

    print("=" * 70)
    print("Testcase-5: auth fails with an unsecured (alg=none) JWT")
    print(f"  bootstrap={bootstrap}  cluster={logical_cluster}  pool={identity_pool_id}")
    print(f"  probe_timeout={timeout}s")
    print("=" * 70)

    errors = []
    admin = AdminClient({
        'bootstrap.servers': bootstrap,
        'security.protocol': 'SASL_SSL',
        'sasl.mechanisms': 'OAUTHBEARER',
        'oauth_cb': oauth_cb,
        'error_cb': lambda err: errors.append(str(err)),
    })

    try:
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
