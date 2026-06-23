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

"""Drift-guard tests for the AWS IAM marker constants."""

from confluent_kafka.oauthbearer.aws._aws_iam_marker import (
    AWS_IAM_MARKER_KEY,
    AWS_IAM_MARKER_VALUE,
)


def test_marker_key_is_locked_value():
    assert AWS_IAM_MARKER_KEY == "sasl.oauthbearer.metadata.authentication.type"


def test_marker_value_is_locked_value():
    assert AWS_IAM_MARKER_VALUE == "aws_iam"


def test_c_dispatcher_recognises_python_authoritative_marker():
    import pytest

    from confluent_kafka import Producer

    with pytest.raises(ValueError, match="method=oidc"):
        Producer(
            {
                "bootstrap.servers": "broker.invalid:9092",
                "sasl.mechanisms": "OAUTHBEARER",
                AWS_IAM_MARKER_KEY: AWS_IAM_MARKER_VALUE,
                "sasl.oauthbearer.config": "region=us-east-1,audience=https://a",
                # Deliberately omitting sasl.oauthbearer.method to trigger the
                # dispatcher's precondition check. If the dispatcher's C-side
                # marker literals differ from AWS_IAM_MARKER_KEY/VALUE, the
                # dispatcher won't fire and this ValueError won't raise.
            }
        )
