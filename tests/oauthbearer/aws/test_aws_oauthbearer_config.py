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

"""Tests for confluent_kafka.oauthbearer.aws._aws_oauthbearer_config."""

import pytest

from confluent_kafka.oauthbearer.aws._aws_oauthbearer_config import (
    AWS_DEBUG_CONSOLE,
    AWS_DEBUG_NONE,
    AwsOAuthBearerConfig,
)

# ---- Required fields ----


def test_parse_minimal_required_populates_region_and_audience():
    cfg = AwsOAuthBearerConfig.parse("region=us-east-1 audience=https://a")
    assert cfg.region == "us-east-1"
    assert cfg.audience == "https://a"


def test_parse_missing_region_raises():
    with pytest.raises(ValueError, match="region.*required"):
        AwsOAuthBearerConfig.parse("audience=https://a")


def test_parse_missing_audience_raises():
    with pytest.raises(ValueError, match="audience.*required"):
        AwsOAuthBearerConfig.parse("region=us-east-1")


def test_parse_null_input_raises():
    with pytest.raises(TypeError):
        AwsOAuthBearerConfig.parse(None)


def test_parse_empty_input_raises_for_missing_region():
    with pytest.raises(ValueError, match="region.*required"):
        AwsOAuthBearerConfig.parse("")


def test_parse_empty_value_on_required_key_raises():
    with pytest.raises(ValueError, match="region.*must not be empty"):
        AwsOAuthBearerConfig.parse("region= audience=https://a")


@pytest.mark.parametrize("key", ["signing_algorithm", "sts_endpoint", "principal_name"])
def test_parse_empty_value_on_optional_key_raises(key):
    with pytest.raises(ValueError, match=key):
        AwsOAuthBearerConfig.parse(f"region=us-east-1 audience=https://a {key}=")


# ---- Defaults applied during parse ----


def test_parse_no_signing_algorithm_defaults_to_es384():
    cfg = AwsOAuthBearerConfig.parse("region=us-east-1 audience=https://a")
    assert cfg.signing_algorithm == "ES384"


def test_parse_no_duration_defaults_to_300_seconds():
    cfg = AwsOAuthBearerConfig.parse("region=us-east-1 audience=https://a")
    assert cfg.duration_seconds == 300


def test_parse_optional_fields_default_to_none():
    cfg = AwsOAuthBearerConfig.parse("region=us-east-1 audience=https://a")
    assert cfg.sts_endpoint is None
    assert cfg.principal_name is None
    assert cfg.sasl_extensions is None
    assert cfg.tags is None


# ---- duration_seconds ----


@pytest.mark.parametrize("seconds", [60, 300, 3600])
def test_parse_duration_in_range_accepted(seconds):
    cfg = AwsOAuthBearerConfig.parse(f"region=us-east-1 audience=https://a duration_seconds={seconds}")
    assert cfg.duration_seconds == seconds


@pytest.mark.parametrize("seconds", [0, 59, 3601, -10])
def test_parse_duration_out_of_range_raises(seconds):
    with pytest.raises(ValueError, match="duration_seconds.*must be between"):
        AwsOAuthBearerConfig.parse(f"region=us-east-1 audience=https://a duration_seconds={seconds}")


def test_parse_duration_not_integer_raises():
    with pytest.raises(ValueError, match="duration_seconds.*must be an integer"):
        AwsOAuthBearerConfig.parse("region=us-east-1 audience=https://a duration_seconds=abc")


# ---- signing_algorithm ----


@pytest.mark.parametrize("alg", ["ES384", "RS256"])
def test_parse_allowed_signing_algorithm_accepted(alg):
    cfg = AwsOAuthBearerConfig.parse(f"region=us-east-1 audience=https://a signing_algorithm={alg}")
    assert cfg.signing_algorithm == alg


@pytest.mark.parametrize("alg", ["HS256", "es384", "RS512"])
def test_parse_disallowed_signing_algorithm_raises(alg):
    with pytest.raises(ValueError, match="signing_algorithm.*ES384.*RS256"):
        AwsOAuthBearerConfig.parse(f"region=us-east-1 audience=https://a signing_algorithm={alg}")


# ---- aws_debug (none/console only) ----


def test_parse_no_aws_debug_defaults_to_none():
    cfg = AwsOAuthBearerConfig.parse("region=us-east-1 audience=https://a")
    assert cfg.aws_debug == AWS_DEBUG_NONE


@pytest.mark.parametrize(
    "value,expected",
    [
        ("none", AWS_DEBUG_NONE),
        ("console", AWS_DEBUG_CONSOLE),
    ],
)
def test_parse_aws_debug_values_accepted(value, expected):
    cfg = AwsOAuthBearerConfig.parse(f"region=us-east-1 audience=https://a aws_debug={value}")
    assert cfg.aws_debug == expected


@pytest.mark.parametrize(
    "value,expected",
    [
        ("Console", AWS_DEBUG_CONSOLE),
        ("CONSOLE", AWS_DEBUG_CONSOLE),
        ("NONE", AWS_DEBUG_NONE),
    ],
)
def test_parse_aws_debug_case_insensitive_accepted(value, expected):
    cfg = AwsOAuthBearerConfig.parse(f"region=us-east-1 audience=https://a aws_debug={value}")
    assert cfg.aws_debug == expected


# Every unsupported aws_debug value is rejected with the same uniform error.
@pytest.mark.parametrize(
    "value",
    ["verbose", "etw", "debug", "true", "foo", "log4net", "systemdiagnostics", "Log4Net", "SystemDiagnostics"],
)
def test_parse_aws_debug_invalid_value_raises(value):
    with pytest.raises(ValueError, match="aws_debug.*none, console"):
        AwsOAuthBearerConfig.parse(f"region=us-east-1 audience=https://a aws_debug={value}")


def test_parse_aws_debug_empty_value_raises():
    with pytest.raises(ValueError, match="aws_debug.*must not be empty"):
        AwsOAuthBearerConfig.parse("region=us-east-1 audience=https://a aws_debug=")


# ---- sts_endpoint, principal_name ----


def test_parse_sts_endpoint_stored_verbatim():
    cfg = AwsOAuthBearerConfig.parse(
        "region=us-east-1 audience=https://a " "sts_endpoint=https://sts-fips.us-east-1.amazonaws.com"
    )
    assert cfg.sts_endpoint == "https://sts-fips.us-east-1.amazonaws.com"


def test_parse_principal_name_stored_verbatim():
    cfg = AwsOAuthBearerConfig.parse("region=us-east-1 audience=https://a principal_name=my-principal")
    assert cfg.principal_name == "my-principal"


# ---- sasl_extensions argument (typed property pass-through) ----


def test_parse_extension_prefix_in_config_rejected_as_unknown_key():
    """sasl extensions are accepted via typed sasl.oauthbearer.extensions
    property only; embedded extension_* keys are rejected."""
    with pytest.raises(ValueError, match="extension_logicalCluster"):
        AwsOAuthBearerConfig.parse("region=us-east-1 audience=https://a extension_logicalCluster=lkc-abc")


def test_parse_sasl_extensions_arg_stored_on_config():
    ext = {"logicalCluster": "lkc-abc", "identityPoolId": "pool-xyz"}
    cfg = AwsOAuthBearerConfig.parse("region=us-east-1 audience=https://a", ext)
    assert cfg.sasl_extensions is ext


def test_parse_sasl_extensions_arg_null_keeps_sasl_extensions_null():
    cfg = AwsOAuthBearerConfig.parse("region=us-east-1 audience=https://a", None)
    assert cfg.sasl_extensions is None


# ---- tag_<name> ----


def test_parse_single_tag_collected_into_tags():
    cfg = AwsOAuthBearerConfig.parse("region=us-east-1 audience=https://a tag_team=platform")
    assert cfg.tags == {"team": "platform"}


def test_parse_multiple_tags_all_collected():
    cfg = AwsOAuthBearerConfig.parse("region=us-east-1 audience=https://a " "tag_team=platform " "tag_environment=prod")
    assert cfg.tags == {"team": "platform", "environment": "prod"}


def test_parse_empty_tag_name_raises():
    with pytest.raises(ValueError, match="empty name"):
        AwsOAuthBearerConfig.parse("region=us-east-1 audience=https://a tag_=value")


def test_parse_empty_tag_value_accepted():
    # AWS allows tag values of 0 chars; mirror that.
    cfg = AwsOAuthBearerConfig.parse("region=us-east-1 audience=https://a tag_team=")
    assert cfg.tags == {"team": ""}


def test_parse_duplicate_tag_name_last_wins():
    cfg = AwsOAuthBearerConfig.parse("region=us-east-1 audience=https://a tag_team=infra tag_team=platform")
    assert cfg.tags == {"team": "platform"}


def test_parse_exactly_max_tags_accepted():
    parts = ["region=us-east-1", "audience=https://a"]
    parts.extend(f"tag_k{i}=v{i}" for i in range(50))
    cfg = AwsOAuthBearerConfig.parse(" ".join(parts))
    assert len(cfg.tags) == 50


def test_parse_over_max_tags_raises():
    parts = ["region=us-east-1", "audience=https://a"]
    parts.extend(f"tag_k{i}=v{i}" for i in range(51))
    with pytest.raises(ValueError, match="50"):
        AwsOAuthBearerConfig.parse(" ".join(parts))


# ---- Unknown keys ----


def test_parse_unknown_key_raises():
    with pytest.raises(ValueError, match="Unknown key.*not_a_key"):
        AwsOAuthBearerConfig.parse("region=us-east-1 audience=https://a not_a_key=foo")


# ---- Whitespace / ordering ----


def test_parse_tabs_and_multiple_spaces_tolerated():
    cfg = AwsOAuthBearerConfig.parse("region=us-east-1\taudience=https://a   duration_seconds=600")
    assert cfg.region == "us-east-1"
    assert cfg.audience == "https://a"
    assert cfg.duration_seconds == 600


def test_parse_leading_and_trailing_whitespace_tolerated():
    cfg = AwsOAuthBearerConfig.parse("  region=us-east-1 audience=https://a   ")
    assert cfg.region == "us-east-1"
    assert cfg.audience == "https://a"


def test_parse_order_invariant():
    cfg = AwsOAuthBearerConfig.parse("duration_seconds=600 audience=https://a region=us-east-1 signing_algorithm=RS256")
    assert cfg.region == "us-east-1"
    assert cfg.audience == "https://a"
    assert cfg.duration_seconds == 600
    assert cfg.signing_algorithm == "RS256"


def test_parse_duplicate_key_last_wins():
    cfg = AwsOAuthBearerConfig.parse("region=us-east-1 audience=https://a region=us-west-2")
    assert cfg.region == "us-west-2"


# ---- Malformed entries ----


def test_parse_no_equals_raises():
    with pytest.raises(ValueError, match="Malformed"):
        AwsOAuthBearerConfig.parse("region us-east-1 audience=https://a")


def test_parse_leading_equals_raises():
    with pytest.raises(ValueError, match="Malformed"):
        AwsOAuthBearerConfig.parse("=value audience=https://a region=us-east-1")


# ---- Integration ----


def test_parse_all_fields_together_all_populated_correctly():
    sasl_extensions = {"logicalCluster": "lkc-abc"}
    cfg = AwsOAuthBearerConfig.parse(
        "region=us-east-1 "
        "audience=https://confluent.cloud/oidc "
        "duration_seconds=1800 "
        "signing_algorithm=RS256 "
        "sts_endpoint=https://sts.us-east-1.amazonaws.com "
        "principal_name=test-principal "
        "aws_debug=console "
        "tag_team=platform",
        sasl_extensions,
    )

    assert cfg.region == "us-east-1"
    assert cfg.audience == "https://confluent.cloud/oidc"
    assert cfg.duration_seconds == 1800
    assert cfg.signing_algorithm == "RS256"
    assert cfg.sts_endpoint == "https://sts.us-east-1.amazonaws.com"
    assert cfg.principal_name == "test-principal"
    assert cfg.aws_debug == AWS_DEBUG_CONSOLE
    assert cfg.sasl_extensions == {"logicalCluster": "lkc-abc"}
    assert cfg.tags == {"team": "platform"}


# ---- Direct construction (bypassing parse) still validated ----


def test_direct_construction_with_empty_region_raises():
    with pytest.raises(ValueError, match="region.*must not be empty"):
        AwsOAuthBearerConfig(region="", audience="https://a")


def test_direct_construction_with_out_of_range_duration_raises():
    with pytest.raises(ValueError, match="duration_seconds.*must be between"):
        AwsOAuthBearerConfig(region="us-east-1", audience="https://a", duration_seconds=10)


def test_direct_construction_with_bool_duration_rejected():
    """bool is-a int in Python; reject explicitly so True/False doesn't pass."""
    with pytest.raises(ValueError, match="duration_seconds.*must be an integer"):
        AwsOAuthBearerConfig(region="us-east-1", audience="https://a", duration_seconds=True)


# ---- Surface invariants ----


def test_aws_oauth_bearer_config_not_exposed_via_subpackage_init():
    """Public surface stays minimal — AwsOAuthBearerConfig is private to the
    autowire layer (only `create_handler` is publicly importable)."""
    import confluent_kafka.oauthbearer.aws as aws_pkg

    assert not hasattr(aws_pkg, "AwsOAuthBearerConfig")
