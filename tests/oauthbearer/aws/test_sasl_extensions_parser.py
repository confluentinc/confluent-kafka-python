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

"""Tests for confluent_kafka.oauthbearer.aws._sasl_extensions_parser."""

import pytest

from confluent_kafka.oauthbearer.aws._sasl_extensions_parser import parse

# ---- Null / empty input → None ----


def test_null_raw_returns_none():
    assert parse(None) is None


def test_empty_string_returns_none():
    assert parse("") is None


# ---- Happy paths ----


def test_single_entry_returns_one_item():
    result = parse("logicalCluster=lkc-abc")
    assert result == {"logicalCluster": "lkc-abc"}


def test_multiple_entries_returns_all():
    result = parse("logicalCluster=lkc-abc,identityPoolId=pool-x")
    assert result == {
        "logicalCluster": "lkc-abc",
        "identityPoolId": "pool-x",
    }


def test_whitespace_around_commas_trimmed_and_parsed():
    # Each comma-delimited entry is trimmed before parsing.
    result = parse(" logicalCluster=lkc-abc ,  identityPoolId=pool-x ")
    assert result == {
        "logicalCluster": "lkc-abc",
        "identityPoolId": "pool-x",
    }


def test_empty_entries_tolerated():
    result = parse("logicalCluster=lkc-abc,,identityPoolId=pool-x,")
    assert result == {
        "logicalCluster": "lkc-abc",
        "identityPoolId": "pool-x",
    }


def test_empty_value_accepted():
    # RFC 7628 SASL extensions allow empty values; mirror that.
    result = parse("logicalCluster=")
    assert result == {"logicalCluster": ""}


def test_duplicate_key_last_wins():
    result = parse("k=a,k=b")
    assert result == {"k": "b"}


# ---- Malformed input → throws ----


def test_missing_equals_raises():
    with pytest.raises(ValueError, match="sasl.oauthbearer.extensions"):
        parse("noEqualsHere")


def test_empty_key_raises():
    with pytest.raises(ValueError, match="sasl.oauthbearer.extensions"):
        parse("=value")
