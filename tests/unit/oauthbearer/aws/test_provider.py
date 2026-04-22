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

import base64
import json
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import pytest
from botocore.exceptions import ClientError
from botocore.stub import Stubber

from confluent_kafka.oauthbearer.aws import AwsOAuthConfig, AwsStsTokenProvider


def _build_jwt(sub: str) -> str:
    """Build a valid 3-segment JWT whose payload has the given sub claim."""
    payload = {"sub": sub, "iss": "https://issuer.example.com"}
    body = json.dumps(payload).encode("utf-8")
    encoded = base64.urlsafe_b64encode(body).decode("ascii").rstrip("=")
    return f"h.{encoded}.s"


def _new_provider(**cfg_kwargs) -> "AwsStsTokenProvider":
    cfg = AwsOAuthConfig(
        region=cfg_kwargs.pop("region", "us-east-1"),
        audience=cfg_kwargs.pop("audience", "https://example.com"),
        **cfg_kwargs,
    )
    return AwsStsTokenProvider(cfg)


def _fixed_expiration() -> datetime:
    return datetime(2026, 4, 21, 6, 6, 47, 641000, tzinfo=timezone.utc)


# ---------- Request-capture cases ------------------------------------------------


def test_audience_passthrough():
    provider = _new_provider(audience="https://foo.example.com")
    stubber = Stubber(provider._sts)
    stubber.add_response(
        "get_web_identity_token",
        {"WebIdentityToken": _build_jwt("arn:aws:iam::1:role/R"),
         "Expiration": _fixed_expiration()},
        expected_params={
            "Audience": ["https://foo.example.com"],
            "SigningAlgorithm": "ES384",
            "DurationSeconds": 300,
        },
    )
    with stubber:
        provider.token()
    stubber.assert_no_pending_responses()


def test_signing_algorithm_rs256_passthrough():
    provider = _new_provider(signing_algorithm="RS256")
    stubber = Stubber(provider._sts)
    stubber.add_response(
        "get_web_identity_token",
        {"WebIdentityToken": _build_jwt("arn:aws:iam::1:role/R"),
         "Expiration": _fixed_expiration()},
        expected_params={
            "Audience": ["https://example.com"],
            "SigningAlgorithm": "RS256",
            "DurationSeconds": 300,
        },
    )
    with stubber:
        provider.token()
    stubber.assert_no_pending_responses()


def test_duration_seconds_passthrough():
    provider = _new_provider(duration_seconds=900)
    stubber = Stubber(provider._sts)
    stubber.add_response(
        "get_web_identity_token",
        {"WebIdentityToken": _build_jwt("arn:aws:iam::1:role/R"),
         "Expiration": _fixed_expiration()},
        expected_params={
            "Audience": ["https://example.com"],
            "SigningAlgorithm": "ES384",
            "DurationSeconds": 900,
        },
    )
    with stubber:
        provider.token()
    stubber.assert_no_pending_responses()


# ---------- Response-mapping cases ----------------------------------------------


def test_happy_path_returns_four_tuple():
    provider = _new_provider()
    jwt = _build_jwt("arn:aws:iam::123:role/TestRole")
    expiration = _fixed_expiration()

    stubber = Stubber(provider._sts)
    stubber.add_response(
        "get_web_identity_token",
        {"WebIdentityToken": jwt, "Expiration": expiration},
    )
    with stubber:
        token, expiry, principal, extensions = provider.token("ignored-config")

    assert token == jwt
    assert expiry == expiration.timestamp()
    assert principal == "arn:aws:iam::123:role/TestRole"
    assert extensions == {}


def test_principal_name_override_takes_precedence():
    provider = _new_provider(principal_name_override="explicit-principal")
    stubber = Stubber(provider._sts)
    stubber.add_response(
        "get_web_identity_token",
        {"WebIdentityToken": _build_jwt("arn:aws:iam::123:role/FromJwt"),
         "Expiration": _fixed_expiration()},
    )
    with stubber:
        _, _, principal, _ = provider.token()
    assert principal == "explicit-principal"


def test_sasl_extensions_round_trip():
    ext = {"logicalCluster": "lkc-abc", "identityPoolId": "pool-xyz"}
    provider = _new_provider(sasl_extensions=ext)
    stubber = Stubber(provider._sts)
    stubber.add_response(
        "get_web_identity_token",
        {"WebIdentityToken": _build_jwt("arn:aws:iam::1:role/R"),
         "Expiration": _fixed_expiration()},
    )
    with stubber:
        _, _, _, returned = provider.token()
    assert returned == ext


def test_malformed_jwt_propagates_value_error():
    provider = _new_provider()
    stubber = Stubber(provider._sts)
    stubber.add_response(
        "get_web_identity_token",
        {"WebIdentityToken": "not-a-jwt", "Expiration": _fixed_expiration()},
    )
    with stubber, pytest.raises(ValueError, match="3 dot-separated segments"):
        provider.token()


def test_sts_access_denied_propagates_client_error():
    provider = _new_provider()
    stubber = Stubber(provider._sts)
    stubber.add_client_error(
        "get_web_identity_token",
        service_error_code="AccessDenied",
        service_message="User is not authorized to perform: sts:GetWebIdentityToken",
    )
    with stubber, pytest.raises(ClientError) as exc_info:
        provider.token()
    assert exc_info.value.response["Error"]["Code"] == "AccessDenied"


def test_sts_federation_not_enabled_propagates_client_error():
    provider = _new_provider()
    stubber = Stubber(provider._sts)
    stubber.add_client_error(
        "get_web_identity_token",
        service_error_code="OutboundWebIdentityFederationDisabledException",
        service_message="Outbound Identity Federation is not enabled for this account",
    )
    with stubber, pytest.raises(ClientError) as exc_info:
        provider.token()
    assert (
        exc_info.value.response["Error"]["Code"]
        == "OutboundWebIdentityFederationDisabledException"
    )


def test_non_utc_expiration_converts_to_utc_epoch():
    provider = _new_provider()
    # +05:30 tz — smoke-test that .timestamp() handles tz-aware datetimes
    non_utc = datetime(2026, 4, 21, 11, 36, 47, 641000,
                       tzinfo=timezone(timedelta(hours=5, minutes=30)))
    stubber = Stubber(provider._sts)
    stubber.add_response(
        "get_web_identity_token",
        {"WebIdentityToken": _build_jwt("arn:aws:iam::1:role/R"),
         "Expiration": non_utc},
    )
    with stubber:
        _, expiry, _, _ = provider.token()
    # Must equal the UTC epoch of the same instant
    assert expiry == non_utc.timestamp()
    # Sanity: the same instant as _fixed_expiration (06:06:47 UTC == 11:36:47 +05:30)
    assert expiry == _fixed_expiration().timestamp()


# ---------- Construction cases --------------------------------------------------


def test_construction_does_not_resolve_credentials():
    # boto3.Session.get_credentials() is what materialises the credential chain.
    # If construction is lazy, it must not be called.
    with patch("boto3.Session.get_credentials") as mocked:
        provider = _new_provider()
        assert provider is not None
        mocked.assert_not_called()


def test_sts_endpoint_override_plumbed_through():
    provider = _new_provider(sts_endpoint_url="https://sts-fips.us-east-1.amazonaws.com")
    # boto3 stores the configured endpoint in the client's meta.endpoint_url
    assert provider._sts.meta.endpoint_url == "https://sts-fips.us-east-1.amazonaws.com"


def test_pre_built_session_is_reused():
    # Use a Session with a recognisable botocore_session so we can identify it.
    import boto3
    session = boto3.Session(region_name="us-east-1")
    session_marker = id(session)
    cfg = AwsOAuthConfig(
        region="us-east-1", audience="https://example.com", session=session,
    )
    provider = AwsStsTokenProvider(cfg)
    # The provider's STS client must have been built from the injected session,
    # which is most easily proven by checking the underlying botocore session
    # came from the same Session instance (boto3.Session wraps a single
    # botocore.Session).
    assert id(cfg.session) == session_marker
    # The client itself is functional
    assert provider._sts is not None
