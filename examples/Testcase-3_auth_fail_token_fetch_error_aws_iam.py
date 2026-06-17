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
"""Testcase-3: OAUTHBEARER auth fails when the token can't be fetched (AWS IAM).

Negative test. The oauth_cb raises (simulating an STS/credential failure), so
librdkafka never obtains a token and authentication must fail. PASS means the
client fails to authenticate (AdminClient.list_topics raises) within the
timeout, rather than hanging or succeeding.

Env:
    BOOTSTRAP_SERVERS      Confluent Cloud bootstrap  [required; or argv[1]]
    PROBE_TIMEOUT_SECONDS  list_topics timeout        [default 15]

Usage:
    python Testcase-3_auth_fail_token_fetch_error_aws_iam.py [bootstrap.servers]
"""

import os
import sys

from confluent_kafka import KafkaException
from confluent_kafka.admin import AdminClient


def oauth_cb(_config_str):
    # Simulate being unable to fetch the credential (e.g. STS down / denied).
    raise RuntimeError("simulated credential fetch failure (STS unavailable)")


def main():
    bootstrap = sys.argv[1] if len(sys.argv) > 1 else os.environ.get('BOOTSTRAP_SERVERS')
    timeout = int(os.environ.get('PROBE_TIMEOUT_SECONDS', '15'))
    if not bootstrap:
        sys.stderr.write("Missing required config: BOOTSTRAP_SERVERS\n")
        return 2

    print("=" * 70)
    print("Testcase-3: auth fails when the token can't be fetched (oauth_cb raises)")
    print(f"  bootstrap={bootstrap}  probe_timeout={timeout}s")
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
