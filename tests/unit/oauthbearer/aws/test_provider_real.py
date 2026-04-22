#!/usr/bin/env python
# -*- coding: utf-8 -*-
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

"""M7 — real-AWS integration test.

Opt-in via RUN_AWS_STS_REAL=1. Requires AWS credentials resolvable via the
default boto3 chain (IMDSv2, EKS IRSA, ECS, env, profile, SSO) plus a role
that can call sts:GetWebIdentityToken against an account on which
``aws iam enable-outbound-web-identity-federation`` has been run.

Shared EC2 test box (cross-language):
    role     = ktrue-iam-sts-test-role
    region   = eu-north-1
    account  = 708975691912

See tests/integration/oauthbearer/aws/TESTING.md for run instructions.
"""

import os
import re
import time

import pytest

from confluent_kafka.oauthbearer.aws import AwsOAuthConfig, AwsStsTokenProvider


pytestmark = pytest.mark.skipif(
    os.environ.get("RUN_AWS_STS_REAL") != "1",
    reason="Set RUN_AWS_STS_REAL=1 and provide AWS credentials to run.",
)


def _cfg() -> AwsOAuthConfig:
    return AwsOAuthConfig(
        region=os.environ.get("AWS_REGION", "eu-north-1"),
        audience=os.environ.get("AUDIENCE", "https://api.example.com"),
        duration_seconds=int(os.environ.get("DURATION_SECONDS", "300")),
    )


def test_get_web_identity_token_real_mints_valid_jwt():
    provider = AwsStsTokenProvider(_cfg())
    token, expiry, principal, extensions = provider.token("")

    # JWT shape: three non-empty base64url segments separated by '.'
    assert re.match(
        r"^[A-Za-z0-9\-_]+\.[A-Za-z0-9\-_]+\.[A-Za-z0-9\-_]+$", token
    ), f"Not a well-formed 3-segment JWT: {token[:40]}..."

    # Expiry is epoch seconds in the near future
    now = time.time()
    assert expiry > now, "expiry must be in the future"
    assert expiry < now + 10 * 60, "expiry must be within 10 minutes for DurationSeconds ≤ 300"

    # Principal is a bare role ARN (AWS STS JWT convention)
    assert re.match(
        r"^arn:aws:iam::\d+:role/.+$", principal
    ), f"Unexpected principal format: {principal!r}"

    # Default config does not supply extensions
    assert extensions == {}


def test_token_length_matches_cross_language_observation():
    """Live AWS STS responses on the shared test role are 1256 chars.
    Go, .NET, JS, and librdkafka all observed this exact value.
    If this test fails with a different length, something shifted on the AWS
    side or the test environment changed roles/audiences.
    """
    provider = AwsStsTokenProvider(_cfg())
    token, *_ = provider.token("")
    expected_len = int(os.environ.get("EXPECTED_TOKEN_LEN", "1256"))
    assert len(token) == expected_len, (
        f"expected {expected_len} chars (matches Go/.NET/JS/librdkafka), got {len(token)}. "
        f"Set EXPECTED_TOKEN_LEN env var if running on a different role/audience."
    )


def test_repeat_invocation_hits_sts_each_time():
    """Provider does not cache; librdkafka drives refresh cadence via token
    expiration. Verify by calling token() twice and confirming that the two
    responses have distinct Expiration timestamps (STS mints fresh JWTs, each
    with a new expiry relative to 'now').
    """
    provider = AwsStsTokenProvider(_cfg())
    _, exp1, _, _ = provider.token("")
    time.sleep(1.0)
    _, exp2, _, _ = provider.token("")
    assert exp2 > exp1, "Second token's expiry must be later than the first"
