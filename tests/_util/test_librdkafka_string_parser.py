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

"""Tests for confluent_kafka._util.librdkafka_string_parser.

The Split_* cases are ported VERBATIM from librdkafka's own ut_string_split
(src/rdstring.c) to lock byte-for-byte parity with rd_string_split. The
remaining cases cover the documented edge behaviour and the
rd_kafka_conf_kv_split key/value semantics.
"""

import pytest

from confluent_kafka._util.librdkafka_string_parser import parse_key_values, split

# ---------------------------------------------------------------------------
# split() — ported verbatim from librdkafka ut_string_split (src/rdstring.c).
# Python string literals are written with explicit "\\" per literal backslash;
# the decoded form is shown in a comment where non-obvious.
# ---------------------------------------------------------------------------


def test_split_single_field():
    assert split("just one field", ",", skip_empty=True) == ["just one field"]


def test_split_empty_skip_empty_no_fields():
    assert split("", ",", skip_empty=True) == []


def test_split_empty_no_skip_empty_one_empty_field():
    assert split("", ",", skip_empty=False) == [""]


def test_split_whitespace_and_empties_skip_empty():
    assert split(", a,b ,,c,   d,    e,f,ghijk,  lmn,opq  ,  r  s t u, v", ",", skip_empty=True) == [
        "a",
        "b",
        "c",
        "d",
        "e",
        "f",
        "ghijk",
        "lmn",
        "opq",
        "r  s t u",
        "v",
    ]


def test_split_whitespace_and_empties_no_skip_empty():
    assert split(", a,b ,,c,   d,    e,f,ghijk,  lmn,opq  ,  r  s t u, v", ",", skip_empty=False) == [
        "",
        "a",
        "b",
        "",
        "c",
        "d",
        "e",
        "f",
        "ghijk",
        "lmn",
        "opq",
        "r  s t u",
        "v",
    ]


def test_split_escapes_quoted_separators_and_backslashes():
    # Decoded input:  «  this is an \,escaped comma,\,,\\, and this is an
    #                   unbalanced escape: \\\\\\\»  (7 trailing backslashes)
    raw = "  this is an \\,escaped comma,\\,,\\\\, and this is an unbalanced escape: \\\\\\\\\\\\\\"
    assert split(raw, ",", skip_empty=True) == [
        "this is an ,escaped comma",
        ",",
        "\\",  # one literal backslash
        "and this is an unbalanced escape: \\\\\\",  # three literal backslashes
    ]


def test_split_alternate_separator_pipe_no_skip_empty():
    # Decoded input: «using|another ||\|d|elimiter»
    assert split("using|another ||\\|d|elimiter", "|", skip_empty=False) == [
        "using",
        "another",
        "",
        "|d",
        "elimiter",
    ]


# ---------------------------------------------------------------------------
# split() — documented edge behaviour (cross-checked against .NET's port).
# ---------------------------------------------------------------------------


def test_split_escape_substitutions_tab_newline_cr_nul():
    # \t \n \r \0 substitute to TAB/LF/CR/NUL; kept internal so the
    # trailing-trim never touches them.
    assert split("a\\tb\\nc\\rd\\0e", ",", skip_empty=True) == ["a\tb\nc\rd\0e"]


def test_split_unicode_whitespace_not_trimmed():
    # U+00A0 (NBSP) is Unicode whitespace but NOT ASCII isspace, so neither the
    # leading nor trailing NBSP is trimmed — proving we match C isspace, not
    # str.isspace (which WOULD trim them, diverging from librdkafka).
    assert split(" a ", ",", skip_empty=True) == [" a "]


def test_split_escaped_whitespace_leading_kept_trailing_trimmed():
    # Asymmetry copied from librdkafka: leading whitespace is stripped only when
    # UNescaped, so an escaped \t at the start survives; trailing whitespace is
    # stripped UNCONDITIONALLY, so an escaped \t at the end is removed.
    assert split("\\tx", ",", skip_empty=True) == ["\tx"]
    assert split("x\\t", ",", skip_empty=True) == ["x"]


def test_split_whitespace_only_trims_to_empty():
    # An all-whitespace field trims to empty: skipped when skip_empty, else
    # returned as a single empty field.
    assert split("   ", ",", skip_empty=True) == []
    assert split("   ", ",", skip_empty=False) == [""]


def test_split_internal_whitespace_preserved():
    assert split("  r  s t u  ", ",", skip_empty=True) == ["r  s t u"]


def test_split_dangling_trailing_backslash_dropped():
    # A trailing unbalanced escape has nothing to escape and is dropped.
    assert split("abc\\", ",", skip_empty=True) == ["abc"]


def test_split_none_raises_type_error():
    with pytest.raises(TypeError):
        split(None, ",", skip_empty=True)


# ---------------------------------------------------------------------------
# parse_key_values() — rd_kafka_conf_kv_split semantics.
# ---------------------------------------------------------------------------


def test_parse_kv_basic_pairs():
    assert parse_key_values("region=us-east-1,audience=https://x", ",", "cfg") == [
        ("region", "us-east-1"),
        ("audience", "https://x"),
    ]


def test_parse_kv_splits_on_first_equals():
    assert parse_key_values("a=b=c", ",", "cfg") == [("a", "b=c")]


def test_parse_kv_empty_value_allowed():
    assert parse_key_values("a=", ",", "cfg") == [("a", "")]


def test_parse_kv_escaped_comma_in_value_stays_one_entry():
    # Headline case: identityPoolId as a comma-separated list, commas quoted
    # with a backslash so the value survives as a single entry.
    assert parse_key_values("identityPoolId=pool-1\\,pool-2", ",", "cfg") == [
        ("identityPoolId", "pool-1,pool-2"),
    ]


def test_parse_kv_no_equals_raises_with_context_label():
    with pytest.raises(ValueError, match="cfg"):
        parse_key_values("abc", ",", "cfg")


def test_parse_kv_empty_key_raises():
    with pytest.raises(ValueError, match="Malformed"):
        parse_key_values("=value", ",", "cfg")


def test_parse_kv_none_context_label_uses_generic_phrase():
    with pytest.raises(ValueError, match="key=value"):
        parse_key_values("abc", ",")


def test_parse_kv_duplicate_keys_both_returned():
    # Last-wins dedup is the caller's responsibility; the parser returns both.
    assert parse_key_values("a=1,a=2", ",", "cfg") == [("a", "1"), ("a", "2")]


def test_parse_kv_consecutive_separators_skipped():
    assert parse_key_values("a=1,,b=2,", ",", "cfg") == [("a", "1"), ("b", "2")]


def test_parse_kv_full_config_all_pairs_in_order():
    pairs = parse_key_values(
        "region=us-east-1,audience=https://a,"
        "duration_seconds=900,signing_algorithm=RS256,"
        "sts_endpoint=https://sts.us-east-1.amazonaws.com,"
        "aws_debug=none,"
        "tag_team=platform,tag_environment=prod",
        ",",
        "SaslOauthbearerConfig",
    )
    assert pairs == [
        ("region", "us-east-1"),
        ("audience", "https://a"),
        ("duration_seconds", "900"),
        ("signing_algorithm", "RS256"),
        ("sts_endpoint", "https://sts.us-east-1.amazonaws.com"),
        ("aws_debug", "none"),
        ("tag_team", "platform"),
        ("tag_environment", "prod"),
    ]


def test_parse_kv_none_raises_type_error():
    with pytest.raises(TypeError):
        parse_key_values(None, ",")
