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

"""Tests for confluent_kafka.oauthbearer.aws.aws_autowire.create_handler.

End-to-end orchestration tests. The frozen-signature contract guard lives
in test_contract.py.
"""

import pytest

pytest.importorskip("boto3")

from confluent_kafka.oauthbearer.aws.aws_autowire import create_handler  # noqa: E402

# ---- Input validation (defensive checks for direct callers) ----


def test_create_handler_null_config_raises():
    with pytest.raises(ValueError, match="missing or empty"):
        create_handler(None, None)


def test_create_handler_empty_config_raises():
    with pytest.raises(ValueError, match="missing or empty"):
        create_handler("", None)


def test_create_handler_error_message_names_marker_and_config_key():
    with pytest.raises(ValueError) as exc_info:
        create_handler(None, None)
    # User should see both: the marker that's set AND the config key that's missing.
    assert "sasl.oauthbearer.metadata.authentication.type" in str(exc_info.value)
    assert "aws_iam" in str(exc_info.value)
    assert "sasl.oauthbearer.config" in str(exc_info.value)


# ---- Parse delegation: errors from AwsOAuthBearerConfig.parse propagate ----


def test_create_handler_missing_region_raises():
    with pytest.raises(ValueError, match="region.*required"):
        create_handler("audience=https://a", None)


def test_create_handler_missing_audience_raises():
    with pytest.raises(ValueError, match="audience.*required"):
        create_handler("region=us-east-1", None)


def test_create_handler_invalid_signing_algorithm_raises():
    with pytest.raises(ValueError, match="signing_algorithm"):
        create_handler(
            "region=us-east-1 audience=https://a signing_algorithm=HS256",
            None,
        )


def test_create_handler_invalid_duration_raises():
    with pytest.raises(ValueError, match="duration_seconds"):
        create_handler(
            "region=us-east-1 audience=https://a duration_seconds=10",
            None,
        )


def test_create_handler_unknown_key_raises():
    with pytest.raises(ValueError, match="not_a_key"):
        create_handler(
            "region=us-east-1 audience=https://a not_a_key=foo",
            None,
        )


def test_create_handler_invalid_extensions_grammar_raises():
    with pytest.raises(ValueError, match="sasl.oauthbearer.extensions"):
        create_handler(
            "region=us-east-1 audience=https://a",
            "noEqualsHere",
        )


def test_create_handler_aws_debug_invalid_value_raises():
    """An unsupported aws_debug value is rejected — the client accepts none/console only."""
    with pytest.raises(ValueError, match="aws_debug.*none, console"):
        create_handler(
            "region=us-east-1 audience=https://a aws_debug=log4net",
            None,
        )


# ---- Success cases — handler returned, no throw, no STS call yet ----


def test_create_handler_marker_only_minimum_config_returns_handler():
    handler = create_handler(
        "region=us-east-1 audience=https://a",
        None,
    )
    assert handler is not None
    assert callable(handler)


def test_create_handler_all_optional_fields_returns_handler():
    handler = create_handler(
        "region=us-east-1 audience=https://a "
        "duration_seconds=900 signing_algorithm=RS256 "
        "sts_endpoint=https://sts.us-east-1.amazonaws.com "
        "principal_name=my-principal "
        "aws_debug=none "
        "tag_team=platform tag_environment=prod",
        None,
    )
    assert handler is not None
    assert callable(handler)


def test_create_handler_tag_config_handler_ready():
    handler = create_handler(
        "region=us-east-1 audience=https://a tag_team=platform tag_environment=prod",
        None,
    )
    assert callable(handler)


def test_create_handler_principal_name_override_handler_ready():
    handler = create_handler(
        "region=us-east-1 audience=https://a principal_name=explicit-principal",
        None,
    )
    assert callable(handler)


# ---- Extensions argument handling ----


def test_create_handler_null_extensions_treats_as_absent():
    handler = create_handler(
        "region=us-east-1 audience=https://a",
        None,
    )
    assert callable(handler)


def test_create_handler_empty_extensions_treats_as_absent():
    handler = create_handler(
        "region=us-east-1 audience=https://a",
        "",
    )
    assert callable(handler)


def test_create_handler_single_extension_handler_ready():
    handler = create_handler(
        "region=us-east-1 audience=https://a",
        "logicalCluster=lkc-abc",
    )
    assert callable(handler)


def test_create_handler_multiple_extensions_handler_ready():
    handler = create_handler(
        "region=us-east-1 audience=https://a",
        "logicalCluster=lkc-abc,identityPoolId=pool-x",
    )
    assert callable(handler)


# ---- Returned callable invokes correctly with a real STS round-trip stubbed at the boto3 layer ----
# We can't easily stub the boto3 client AwsStsTokenProvider creates internally without
# patching boto3.Session. End-to-end with-injected-client tests live in
# test_aws_sts_token_provider.py; here we cover the *closure* level (no STS call yet).


def test_create_handler_does_not_call_sts_at_construction():
    """create_handler must NOT make an STS call — credential resolution and
    network I/O are deferred to the first invocation of the returned callable.
    Verify by patching boto3.Session to detect any client.get_web_identity_token call.
    """
    from unittest.mock import MagicMock, patch

    with patch("boto3.Session") as mock_session_cls:
        mock_session = mock_session_cls.return_value
        mock_client = MagicMock()
        mock_session.client.return_value = mock_client
        create_handler("region=us-east-1 audience=https://a", None)
        # Session/client construction OK; get_web_identity_token must not have been called.
        mock_client.get_web_identity_token.assert_not_called()


def test_create_handler_returned_callable_when_invoked_calls_sts():
    """When the returned callable is invoked, it triggers exactly one STS call."""
    import datetime
    from unittest.mock import MagicMock, patch

    canned_response = {
        "WebIdentityToken": _canned_jwt(),
        "Expiration": datetime.datetime(2099, 4, 21, 6, 6, 47, tzinfo=datetime.timezone.utc),
    }
    with patch("boto3.Session") as mock_session_cls:
        mock_session = mock_session_cls.return_value
        mock_client = MagicMock()
        mock_client.get_web_identity_token.return_value = canned_response
        mock_session.client.return_value = mock_client

        handler = create_handler("region=us-east-1 audience=https://a", None)
        result = handler("ignored-passthrough-string")

        mock_client.get_web_identity_token.assert_called_once()
        assert isinstance(result, tuple)
        assert len(result) == 4
        token, expiry, principal, extensions = result
        assert token == canned_response["WebIdentityToken"]
        assert isinstance(expiry, float)
        assert principal.startswith("arn:")
        assert extensions == {}


def test_create_handler_returned_callable_round_trips_extensions():
    """Extensions configured via the typed property flow through to the
    4-tuple's extensions slot."""
    import datetime
    from unittest.mock import MagicMock, patch

    canned_response = {
        "WebIdentityToken": _canned_jwt(),
        "Expiration": datetime.datetime(2099, 4, 21, 6, 6, 47, tzinfo=datetime.timezone.utc),
    }
    with patch("boto3.Session") as mock_session_cls:
        mock_session = mock_session_cls.return_value
        mock_client = MagicMock()
        mock_client.get_web_identity_token.return_value = canned_response
        mock_session.client.return_value = mock_client

        handler = create_handler(
            "region=us-east-1 audience=https://a",
            "logicalCluster=lkc-abc,identityPoolId=pool-x",
        )
        _, _, _, extensions = handler("")

        assert extensions == {
            "logicalCluster": "lkc-abc",
            "identityPoolId": "pool-x",
        }


# ---- Test helpers ----


def _base64url(data: bytes) -> str:
    import base64

    return base64.b64encode(data).decode("ascii").rstrip("=").replace("+", "-").replace("/", "_")


def _canned_jwt(sub: str = "arn:aws:iam::123:role/R") -> str:
    header = _base64url(b'{"alg":"ES384","typ":"JWT"}')
    payload = _base64url(f'{{"sub":"{sub}"}}'.encode("utf-8"))
    return f"{header}.{payload}.sig"
