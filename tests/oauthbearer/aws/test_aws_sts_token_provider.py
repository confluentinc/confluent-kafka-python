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

"""Tests for confluent_kafka.oauthbearer.aws._aws_sts_token_provider.

- Python uses a tuple return (not a typed record) so the "no extensions →
  null" assertion becomes "no extensions → empty dict" (the C oauth_cb
  contract requires a dict, not None, at that slot).
- Python returns ``expiry_epoch_seconds`` (float seconds) not
  ``LifetimeMs`` (long milliseconds) — the C wrapper multiplies by 1000.
"""

import base64
import datetime
import logging
from pathlib import Path
from typing import Any, Dict, Optional
from unittest.mock import patch

import pytest

# botocore is a transitive of boto3; available in opt-in venv.
pytest.importorskip("boto3")
pytest.importorskip("botocore")

from botocore.exceptions import ClientError  # noqa: E402

from confluent_kafka.oauthbearer.aws._aws_oauthbearer_config import AwsOAuthBearerConfig  # noqa: E402
from confluent_kafka.oauthbearer.aws._aws_sts_token_provider import (  # noqa: E402
    MINIMUM_BOTO3_VERSION,
    AwsStsTokenProvider,
    _require_boto3_version,
    _version_tuple,
)

# ---- Test helpers ----


_ROLE_ARN = "arn:aws:iam::123:role/R"


def _base64url(data: bytes) -> str:
    return base64.b64encode(data).decode("ascii").rstrip("=").replace("+", "-").replace("/", "_")


def _canned_jwt(sub: str = _ROLE_ARN) -> str:
    header = _base64url(b'{"alg":"ES384","typ":"JWT"}')
    payload = _base64url(f'{{"sub":"{sub}"}}'.encode("utf-8"))
    return f"{header}.{payload}.sig"


_CANNED_JWT = _canned_jwt()
_CANNED_EXPIRY = datetime.datetime(
    2099,
    4,
    21,
    6,
    6,
    47,
    641_000,
    tzinfo=datetime.timezone.utc,
)


class FakeStsClient:
    """Test double mirroring .NET's FakeStsClient.

    Records the last request kwargs and returns a canned response (or raises
    a canned exception). Avoids the ceremony of ``botocore.stub.Stubber`` for
    the simple cases here.
    """

    def __init__(self, responder=None, raises: Optional[BaseException] = None) -> None:
        self.last_request: Optional[Dict[str, Any]] = None
        self._responder = responder
        self._raises = raises

    def get_web_identity_token(self, **kwargs: Any) -> Dict[str, Any]:
        self.last_request = kwargs
        if self._raises is not None:
            raise self._raises
        if self._responder is not None:
            return self._responder(kwargs)
        return _ok_response()


def _ok_response(
    jwt: str = _CANNED_JWT,
    expiration: datetime.datetime = _CANNED_EXPIRY,
) -> Dict[str, Any]:
    return {"WebIdentityToken": jwt, "Expiration": expiration}


# ---- Constructor: checks ----


def test_ctor_null_config_raises():
    with pytest.raises(TypeError):
        AwsStsTokenProvider(None)


def test_ctor_valid_parsed_config_succeeds():
    """No AWS / no HTTP call at construction time (lazy credential chain)."""
    cfg = AwsOAuthBearerConfig.parse("region=us-east-1 audience=https://a")
    provider = AwsStsTokenProvider(cfg, sts_client=FakeStsClient())
    # Does not throw; does not call AWS (lazy credential chain).
    assert provider is not None


# ---- Constructor: aws_debug ----


def test_ctor_no_aws_debug_does_not_mutate_botocore_logger():
    cfg = AwsOAuthBearerConfig.parse("region=us-east-1 audience=https://a")
    # Capture botocore logger's level before & after construction; without
    # an explicit aws_debug=console, we must not touch boto3.set_stream_logger.
    with patch("boto3.set_stream_logger") as mock_setter:
        AwsStsTokenProvider(cfg, sts_client=FakeStsClient())
        mock_setter.assert_not_called()


def test_ctor_aws_debug_none_does_not_mutate_botocore_logger():
    cfg = AwsOAuthBearerConfig.parse("region=us-east-1 audience=https://a aws_debug=none")
    with patch("boto3.set_stream_logger") as mock_setter:
        AwsStsTokenProvider(cfg, sts_client=FakeStsClient())
        mock_setter.assert_not_called()


def test_ctor_aws_debug_console_routes_botocore_logger_to_stream():
    cfg = AwsOAuthBearerConfig.parse("region=us-east-1 audience=https://a aws_debug=console")
    with patch("boto3.set_stream_logger") as mock_setter:
        AwsStsTokenProvider(cfg, sts_client=FakeStsClient())
        mock_setter.assert_called_once_with("botocore", logging.DEBUG)


# ---- token(): request shape ----


def test_token_audience_passthrough():
    fake = FakeStsClient()
    cfg = AwsOAuthBearerConfig.parse("region=us-east-1 audience=https://my.audience")
    provider = AwsStsTokenProvider(cfg, sts_client=fake)
    provider.token()
    assert fake.last_request["Audience"] == ["https://my.audience"]


def test_token_signing_algorithm_passthrough():
    fake = FakeStsClient()
    cfg = AwsOAuthBearerConfig.parse("region=us-east-1 audience=https://a signing_algorithm=RS256")
    provider = AwsStsTokenProvider(cfg, sts_client=fake)
    provider.token()
    assert fake.last_request["SigningAlgorithm"] == "RS256"


def test_token_duration_seconds_passthrough():
    fake = FakeStsClient()
    cfg = AwsOAuthBearerConfig.parse("region=us-east-1 audience=https://a duration_seconds=900")
    provider = AwsStsTokenProvider(cfg, sts_client=fake)
    provider.token()
    assert fake.last_request["DurationSeconds"] == 900


def test_token_default_duration_sends_300_seconds():
    fake = FakeStsClient()
    cfg = AwsOAuthBearerConfig.parse("region=us-east-1 audience=https://a")
    provider = AwsStsTokenProvider(cfg, sts_client=fake)
    provider.token()
    assert fake.last_request["DurationSeconds"] == 300


def test_token_default_signing_algorithm_sends_es384():
    fake = FakeStsClient()
    cfg = AwsOAuthBearerConfig.parse("region=us-east-1 audience=https://a")
    provider = AwsStsTokenProvider(cfg, sts_client=fake)
    provider.token()
    assert fake.last_request["SigningAlgorithm"] == "ES384"


def test_token_tags_passthrough():
    fake = FakeStsClient()
    cfg = AwsOAuthBearerConfig.parse("region=us-east-1 audience=https://a tag_team=platform tag_environment=prod")
    provider = AwsStsTokenProvider(cfg, sts_client=fake)
    provider.token()

    tags = fake.last_request["Tags"]
    assert len(tags) == 2
    assert {"Key": "team", "Value": "platform"} in tags
    assert {"Key": "environment", "Value": "prod"} in tags


def test_token_no_tags_omits_tags_field_from_request():
    """When no tags configured, omit the Tags field rather than send empty list."""
    fake = FakeStsClient()
    cfg = AwsOAuthBearerConfig.parse("region=us-east-1 audience=https://a")
    provider = AwsStsTokenProvider(cfg, sts_client=fake)
    provider.token()
    assert "Tags" not in fake.last_request


# ---- token(): response mapping ----


def test_token_returns_mapped_fields():
    fake = FakeStsClient()
    cfg = AwsOAuthBearerConfig.parse("region=us-east-1 audience=https://a")
    provider = AwsStsTokenProvider(cfg, sts_client=fake)
    jwt, expiry, principal, extensions = provider.token()

    assert jwt == _CANNED_JWT
    assert principal == _ROLE_ARN
    assert expiry == _CANNED_EXPIRY.timestamp()
    assert extensions == {}


def test_token_principal_name_override_wins_over_jwt_sub():
    fake = FakeStsClient()
    cfg = AwsOAuthBearerConfig.parse("region=us-east-1 audience=https://a principal_name=explicit-principal")
    provider = AwsStsTokenProvider(cfg, sts_client=fake)
    _, _, principal, _ = provider.token()
    assert principal == "explicit-principal"


def test_token_sasl_extensions_passthrough():
    fake = FakeStsClient()
    sasl_extensions = {"logicalCluster": "lkc-123", "identityPoolId": "pool-x"}
    cfg = AwsOAuthBearerConfig.parse(
        "region=us-east-1 audience=https://a",
        sasl_extensions,
    )
    provider = AwsStsTokenProvider(cfg, sts_client=fake)
    _, _, _, extensions = provider.token()
    assert extensions == {"logicalCluster": "lkc-123", "identityPoolId": "pool-x"}


def test_token_no_extensions_configured_returns_empty_dict():
    """Python deviation from .NET: empty dict, not None, because the C
    oauth_cb contract uses PyArg_ParseTuple "O!" with PyDict_Type at that
    slot (rejects None). Empty dict is the Pythonic equivalent of the
    .NET null-extensions case."""
    fake = FakeStsClient()
    cfg = AwsOAuthBearerConfig.parse("region=us-east-1 audience=https://a")
    provider = AwsStsTokenProvider(cfg, sts_client=fake)
    _, _, _, extensions = provider.token()
    assert extensions == {}
    assert isinstance(extensions, dict)


def test_token_expiry_is_epoch_seconds_float():
    """The C wrapper expects expiry as epoch seconds (float). It multiplies
    by 1000 internally to get milliseconds for librdkafka."""
    fake = FakeStsClient()
    cfg = AwsOAuthBearerConfig.parse("region=us-east-1 audience=https://a")
    provider = AwsStsTokenProvider(cfg, sts_client=fake)
    _, expiry, _, _ = provider.token()
    assert isinstance(expiry, float)
    assert expiry == _CANNED_EXPIRY.timestamp()


# ---- token(): error propagation ----


def test_token_missing_expiration_raises():
    """STS response without Expiration → ValueError surfaced as
    rd_kafka_oauthbearer_set_token_failure by the C wrapper."""
    fake = FakeStsClient(responder=lambda kwargs: {"WebIdentityToken": _CANNED_JWT})
    cfg = AwsOAuthBearerConfig.parse("region=us-east-1 audience=https://a")
    provider = AwsStsTokenProvider(cfg, sts_client=fake)
    with pytest.raises(ValueError, match="Expiration"):
        provider.token()


def test_token_missing_token_value_raises():
    fake = FakeStsClient(responder=lambda kwargs: {"Expiration": _CANNED_EXPIRY})
    cfg = AwsOAuthBearerConfig.parse("region=us-east-1 audience=https://a")
    provider = AwsStsTokenProvider(cfg, sts_client=fake)
    with pytest.raises(ValueError, match="WebIdentityToken"):
        provider.token()


def test_token_malformed_jwt_raises_value_error():
    fake = FakeStsClient(
        responder=lambda kwargs: {
            "WebIdentityToken": "not-a-jwt",
            "Expiration": _CANNED_EXPIRY,
        }
    )
    cfg = AwsOAuthBearerConfig.parse("region=us-east-1 audience=https://a")
    provider = AwsStsTokenProvider(cfg, sts_client=fake)
    with pytest.raises(ValueError):
        provider.token()


def test_token_sts_access_denied_propagates():
    fake = FakeStsClient(
        raises=ClientError(
            error_response={
                "Error": {
                    "Code": "AccessDenied",
                    "Message": "User is not authorized to perform: sts:GetWebIdentityToken",
                }
            },
            operation_name="GetWebIdentityToken",
        ),
    )
    cfg = AwsOAuthBearerConfig.parse("region=us-east-1 audience=https://a")
    provider = AwsStsTokenProvider(cfg, sts_client=fake)
    with pytest.raises(ClientError) as exc_info:
        provider.token()
    assert exc_info.value.response["Error"]["Code"] == "AccessDenied"


def test_token_outbound_federation_disabled_propagates():
    fake = FakeStsClient(
        raises=ClientError(
            error_response={
                "Error": {
                    "Code": "OutboundWebIdentityFederationDisabledException",
                    "Message": "Outbound web identity federation is not enabled on this account.",
                }
            },
            operation_name="GetWebIdentityToken",
        ),
    )
    cfg = AwsOAuthBearerConfig.parse("region=us-east-1 audience=https://a")
    provider = AwsStsTokenProvider(cfg, sts_client=fake)
    with pytest.raises(ClientError) as exc_info:
        provider.token()
    assert exc_info.value.response["Error"]["Code"] == "OutboundWebIdentityFederationDisabledException"


# ---- Surface invariants ----


def test_aws_sts_token_provider_not_exposed_via_subpackage_init():
    """Public surface stays minimal — AwsStsTokenProvider is private."""
    import confluent_kafka.oauthbearer.aws as aws_pkg

    assert not hasattr(aws_pkg, "AwsStsTokenProvider")


# ---- Lazy credential resolution ----


def test_ctor_does_not_invoke_sts():
    """Construction must not make any STS call (lazy credential chain).
    Verify by checking the fake client received no requests after __init__
    but before any .token() invocation."""
    fake = FakeStsClient()
    cfg = AwsOAuthBearerConfig.parse("region=us-east-1 audience=https://a")
    AwsStsTokenProvider(cfg, sts_client=fake)
    assert fake.last_request is None


# ---- sts_endpoint plumbed to boto3.client ----


def test_ctor_sts_endpoint_plumbed_to_boto3_client():
    """When sts_endpoint is set on config, boto3.Session().client('sts', ...)
    receives an endpoint_url kwarg."""
    cfg = AwsOAuthBearerConfig.parse(
        "region=us-east-1 audience=https://a " "sts_endpoint=https://sts-fips.us-east-1.amazonaws.com"
    )
    with patch("boto3.Session") as mock_session_cls:
        mock_session = mock_session_cls.return_value
        AwsStsTokenProvider(cfg)
        mock_session.client.assert_called_once_with(
            "sts",
            region_name="us-east-1",
            endpoint_url="https://sts-fips.us-east-1.amazonaws.com",
        )


def test_ctor_no_sts_endpoint_omits_endpoint_url_kwarg():
    cfg = AwsOAuthBearerConfig.parse("region=us-east-1 audience=https://a")
    with patch("boto3.Session") as mock_session_cls:
        mock_session = mock_session_cls.return_value
        AwsStsTokenProvider(cfg)
        mock_session.client.assert_called_once_with(
            "sts",
            region_name="us-east-1",
        )


# ---- boto3 version floor check ----


def test_version_tuple_parses_numeric_components():
    assert _version_tuple("1.42.25") == (1, 42, 25)
    assert _version_tuple("1.42.97") == (1, 42, 97)
    assert _version_tuple("2.0.0") == (2, 0, 0)


def test_version_tuple_tolerates_prerelease_suffix():
    assert _version_tuple("1.42.0rc1") == (1, 42, 0)
    assert _version_tuple("1.42.25.dev0") == (1, 42, 25, 0)


def test_require_boto3_version_passes_on_installed_version():
    # The opt-in test venv installs boto3 >= the floor, so this must not raise.
    _require_boto3_version()


def test_require_boto3_version_below_floor_raises(monkeypatch):
    import boto3

    monkeypatch.setattr(boto3, "__version__", "1.40.0")
    with pytest.raises(ImportError, match="requires boto3>=") as exc_info:
        _require_boto3_version()
    # message names both the required floor and the offending installed version
    assert MINIMUM_BOTO3_VERSION in str(exc_info.value)
    assert "1.40.0" in str(exc_info.value)


def test_require_boto3_version_at_floor_ok(monkeypatch):
    import boto3

    monkeypatch.setattr(boto3, "__version__", MINIMUM_BOTO3_VERSION)
    _require_boto3_version()  # exactly the floor satisfies ">="


def test_ctor_old_boto3_raises_before_building_client(monkeypatch):
    """The floor check is wired into __init__ on the real-client path and fails
    fast — before any boto3.Session/client is constructed."""
    import boto3

    monkeypatch.setattr(boto3, "__version__", "1.40.0")
    cfg = AwsOAuthBearerConfig.parse("region=us-east-1 audience=https://a")
    with patch("boto3.Session") as mock_session_cls:
        with pytest.raises(ImportError, match="GetWebIdentityToken"):
            AwsStsTokenProvider(cfg)  # no sts_client → real path → check runs
        mock_session_cls.assert_not_called()  # raised before building a client


def test_ctor_injected_client_skips_version_check(monkeypatch):
    """Injecting an sts_client (test seam) bypasses the boto3 floor check — the
    real boto3 isn't used for the STS call in that case."""
    import boto3

    monkeypatch.setattr(boto3, "__version__", "1.0.0")  # ancient, but skipped
    cfg = AwsOAuthBearerConfig.parse("region=us-east-1 audience=https://a")
    provider = AwsStsTokenProvider(cfg, sts_client=FakeStsClient())
    assert provider is not None


def test_minimum_boto3_version_matches_requirements_file():
    """Guard against MINIMUM_BOTO3_VERSION drifting from the pinned floor in
    requirements/requirements-oauthbearer-aws.txt."""
    req = Path(__file__).resolve().parents[3] / "requirements" / "requirements-oauthbearer-aws.txt"
    floor = None
    for line in req.read_text().splitlines():
        stripped = line.strip()
        if stripped.startswith("boto3") and ">=" in stripped:
            floor = stripped.split(">=", 1)[1].strip()
            break
    assert floor == MINIMUM_BOTO3_VERSION, (
        f"requirements-oauthbearer-aws.txt pins boto3>={floor} but "
        f"MINIMUM_BOTO3_VERSION is {MINIMUM_BOTO3_VERSION}; keep them in sync."
    )
