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

import pytest

from confluent_kafka.oauthbearer.aws import AwsOAuthConfig


def _minimal(**kwargs):
    return AwsOAuthConfig(region="us-east-1", audience="https://example.com", **kwargs)


def test_minimal_valid_config():
    cfg = _minimal()
    assert cfg.region == "us-east-1"
    assert cfg.audience == "https://example.com"


def test_defaults_signing_algorithm_and_duration():
    cfg = _minimal()
    assert cfg.signing_algorithm == "ES384"
    assert cfg.duration_seconds == 300
    assert cfg.sts_endpoint_url is None
    assert cfg.session is None
    assert cfg.sasl_extensions is None
    assert cfg.principal_name_override is None


def test_missing_region_is_type_error():
    # Required positional / keyword — dataclass raises TypeError
    with pytest.raises(TypeError):
        AwsOAuthConfig(audience="https://example.com")  # type: ignore[call-arg]


def test_empty_region_raises():
    with pytest.raises(ValueError, match="region"):
        AwsOAuthConfig(region="", audience="https://example.com")


def test_non_string_region_raises():
    with pytest.raises(ValueError, match="region"):
        AwsOAuthConfig(region=123, audience="https://example.com")  # type: ignore[arg-type]


def test_empty_audience_raises():
    with pytest.raises(ValueError, match="audience"):
        AwsOAuthConfig(region="us-east-1", audience="")


def test_bad_signing_algorithm_rejected():
    with pytest.raises(ValueError, match="signing_algorithm"):
        _minimal(signing_algorithm="HS256")


def test_rs256_accepted():
    cfg = _minimal(signing_algorithm="RS256")
    assert cfg.signing_algorithm == "RS256"


def test_duration_below_minimum_rejected():
    with pytest.raises(ValueError, match="duration_seconds"):
        _minimal(duration_seconds=30)


def test_duration_above_maximum_rejected():
    with pytest.raises(ValueError, match="duration_seconds"):
        _minimal(duration_seconds=7200)


def test_duration_at_boundaries_accepted():
    assert _minimal(duration_seconds=60).duration_seconds == 60
    assert _minimal(duration_seconds=3600).duration_seconds == 3600


def test_bool_duration_rejected():
    # `True` is-a int in Python; reject it explicitly to catch typos.
    with pytest.raises(ValueError, match="duration_seconds"):
        _minimal(duration_seconds=True)


def test_sts_endpoint_http_rejected():
    with pytest.raises(ValueError, match="sts_endpoint_url"):
        _minimal(sts_endpoint_url="http://insecure.example.com")


def test_sts_endpoint_empty_rejected():
    with pytest.raises(ValueError, match="sts_endpoint_url"):
        _minimal(sts_endpoint_url="")


def test_sts_endpoint_https_accepted():
    cfg = _minimal(sts_endpoint_url="https://sts-fips.us-east-1.amazonaws.com")
    assert cfg.sts_endpoint_url == "https://sts-fips.us-east-1.amazonaws.com"


def test_empty_principal_override_rejected():
    with pytest.raises(ValueError, match="principal_name_override"):
        _minimal(principal_name_override="")


def test_principal_override_accepted():
    cfg = _minimal(principal_name_override="explicit-principal")
    assert cfg.principal_name_override == "explicit-principal"


def test_sasl_extensions_must_be_dict():
    with pytest.raises(ValueError, match="sasl_extensions"):
        _minimal(sasl_extensions=["not", "a", "dict"])  # type: ignore[arg-type]


def test_sasl_extensions_accepted():
    cfg = _minimal(sasl_extensions={"logicalCluster": "lkc-abc"})
    assert cfg.sasl_extensions == {"logicalCluster": "lkc-abc"}


def test_validation_is_synchronous_no_network():
    # Construction must raise immediately — no implicit boto3 session creation,
    # no HTTP I/O. We prove the lack of network by running construction with
    # invalid input and confirming the stack trace stays inside our module.
    import traceback

    try:
        AwsOAuthConfig(region="", audience="")
    except ValueError:
        tb = traceback.format_exc()
        assert "botocore" not in tb
        assert "http" not in tb.lower() or "oauthbearer" in tb.lower()
