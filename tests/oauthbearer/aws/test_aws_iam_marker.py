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

"""Drift-guard tests for the AWS IAM marker constants.

Phase 2 covers the Python-side half of the drift guard: the literal values
must not move. The end-to-end half (asserting the C dispatcher's literal
strings match these) lands in Phase 5 once the C dispatcher exists.
"""

from confluent_kafka.oauthbearer.aws._aws_iam_marker import (
    AWS_IAM_MARKER_KEY,
    AWS_IAM_MARKER_VALUE,
)


def test_marker_key_is_locked_value():
    """The marker key is part of the cross-language wire contract.

    Bumping it would silently break .NET / Go / JS / Python parity. Any change
    here MUST be coordinated as a major version bump across all four clients.
    """
    assert AWS_IAM_MARKER_KEY == "sasl.oauthbearer.metadata.authentication.type"


def test_marker_value_is_locked_value():
    """The marker value is part of the cross-language wire contract.

    Same constraint as :func:`test_marker_key_is_locked_value`.
    """
    assert AWS_IAM_MARKER_VALUE == "aws_iam"
