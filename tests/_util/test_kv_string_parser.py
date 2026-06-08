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

"""Tests for confluent_kafka._util.kv_string_parser."""

import pytest

from confluent_kafka._util.kv_string_parser import parse_kv

# ---- Null guards ----


def test_null_raw_raises():
    with pytest.raises(TypeError):
        list(parse_kv(None, [","]))


def test_null_separators_raises():
    with pytest.raises(TypeError):
        list(parse_kv("a=1", None))


# ---- Empty / whitespace-only input ----


def test_empty_raw_yields_nothing():
    assert list(parse_kv("", [","])) == []


def test_whitespace_only_raw_yields_nothing():
    # After split + trim, no real tokens remain.
    assert list(parse_kv("   ", [","])) == []


# ---- Single separator ----


def test_single_separator_comma_basic_case():
    pairs = list(parse_kv("a=1,b=2", [","]))
    assert pairs == [("a", "1"), ("b", "2")]


# ---- Multiple separators (whitespace-tolerant case) ----


def test_multiple_separators_all_recognized():
    pairs = list(parse_kv("a=1\tb=2\nc=3 d=4", [" ", "\t", "\r", "\n"]))
    assert [k for k, _ in pairs] == ["a", "b", "c", "d"]


# ---- Empty tokens skipped ----


def test_consecutive_separators_skip_empty_tokens():
    pairs = list(parse_kv("a=1,,b=2,", [","]))
    assert len(pairs) == 2
    assert pairs == [("a", "1"), ("b", "2")]


# ---- Trim toggle ----


def test_trim_tokens_default_true_strips_leading_and_trailing():
    pairs = list(parse_kv(" a=1 , b=2 ", [","]))
    assert pairs == [("a", "1"), ("b", "2")]


def test_trim_tokens_false_preserves_whitespace_inside_tokens():
    pairs = list(parse_kv(" a=1 ,b=2 ", [","], trim_tokens=False))
    assert pairs == [(" a", "1 "), ("b", "2 ")]


# ---- Malformed tokens ----


def test_token_without_equals_raises():
    with pytest.raises(ValueError, match="Malformed.*bad"):
        list(parse_kv("a=1,bad,c=3", [","]))


def test_token_starts_with_equals_raises():
    with pytest.raises(ValueError, match="Malformed"):
        list(parse_kv("=value", [","]))


# ---- Value-content semantics ----


def test_value_contains_equals_kept_verbatim():
    # Split is on FIRST '=' only — the rest stays in the value.
    pairs = list(parse_kv("key=val=ue", [","]))
    assert pairs == [("key", "val=ue")]


def test_empty_value_allowed():
    # RFC 7628 SASL extensions allow empty values; AWS allows empty tag values.
    pairs = list(parse_kv("key=", [","]))
    assert pairs == [("key", "")]


# ---- Duplicate keys preserved (caller's responsibility to dedupe) ----


def test_duplicate_keys_both_yielded():
    pairs = list(parse_kv("a=1,a=2", [","]))
    assert pairs == [("a", "1"), ("a", "2")]


# ---- context_label weaves into error messages ----


def test_context_label_present_appears_in_error_message():
    with pytest.raises(ValueError, match="my.config.key"):
        list(parse_kv("bad", [","], context_label="my.config.key"))


def test_context_label_none_uses_generic_error_phrase():
    with pytest.raises(ValueError, match="key=value"):
        list(parse_kv("bad", [","]))
