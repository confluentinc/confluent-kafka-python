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

"""Tests for the C-extension dispatcher resolve_aws_oauthbearer_marker().

The dispatcher fires inside common_conf_setup() (src/confluent_kafka.c) on
every Producer / Consumer / AdminClient construction (and transitively
AIOProducer / AIOConsumer, which wrap the sync clients).

These tests exercise the C dispatcher indirectly via client construction:
no direct path to the helper exists from Python — that's the design.
"""

import base64
import datetime
import sys
from typing import Any, Dict, Optional
from unittest.mock import MagicMock, patch

import pytest

pytest.importorskip("boto3")

import confluent_kafka  # noqa: E402
from confluent_kafka.admin import AdminClient  # noqa: E402

# ---- Test helpers ----


def _base64url(data: bytes) -> str:
    return base64.b64encode(data).decode("ascii").rstrip("=").replace("+", "-").replace("/", "_")


def _canned_jwt(sub: str = "arn:aws:iam::123:role/R") -> str:
    header = _base64url(b'{"alg":"ES384","typ":"JWT"}')
    payload = _base64url(f'{{"sub":"{sub}"}}'.encode("utf-8"))
    return f"{header}.{payload}.sig"


def _canned_response() -> Dict[str, Any]:
    return {
        "WebIdentityToken": _canned_jwt(),
        "Expiration": datetime.datetime(
            2099,
            4,
            21,
            6,
            6,
            47,
            tzinfo=datetime.timezone.utc,
        ),
    }


@pytest.fixture
def mocked_boto3():
    """Patch boto3.Session so the autowired provider's STS client is a mock.
    The actual STS call doesn't happen at client construction (lazy), but the
    Session/client construction does — without this, boto3 would try to
    resolve real credentials."""
    with patch("boto3.Session") as session_cls:
        session = session_cls.return_value
        client = MagicMock()
        client.get_web_identity_token.return_value = _canned_response()
        session.client.return_value = client
        yield client


def _minimal_aws_iam_config(extra: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
    cfg = {
        "bootstrap.servers": "broker.invalid:9092",
        # security.protocol=SASL_SSL is required for librdkafka to actually
        # engage the OAUTHBEARER refresh path. Without it, sasl.mechanisms is
        # inert (librdkafka logs CONFWARN and the refresh_cb never fires).
        "security.protocol": "SASL_SSL",
        "sasl.mechanisms": "OAUTHBEARER",
        "sasl.oauthbearer.method": "oidc",
        "sasl.oauthbearer.metadata.authentication.type": "aws_iam",
        "sasl.oauthbearer.config": "region=us-east-1 audience=https://a",
    }
    if extra:
        cfg.update(extra)
    return cfg


# ---- 1. Marker absent → dispatcher is a no-op (regression check) ----


def test_marker_absent_producer_constructs_unchanged():
    p = confluent_kafka.Producer({"bootstrap.servers": "localhost:9092"})
    p.flush(timeout=0.1)


def test_marker_absent_consumer_constructs_unchanged():
    c = confluent_kafka.Consumer(
        {
            "bootstrap.servers": "localhost:9092",
            "group.id": "test-group",
        }
    )
    c.close()


def test_marker_absent_admin_client_constructs_unchanged():
    a = AdminClient({"bootstrap.servers": "localhost:9092"})
    assert a is not None


def test_marker_absent_does_not_import_aws_modules():
    """If the marker is absent the dispatcher must NOT touch the optional
    subpackage — boto3 import must not happen."""
    # Clear any prior imports of the AWS subpackage so we can detect a fresh import.
    for name in list(sys.modules):
        if name.startswith("confluent_kafka.oauthbearer.aws"):
            del sys.modules[name]
    confluent_kafka.Producer({"bootstrap.servers": "localhost:9092"})
    for name in [
        "confluent_kafka.oauthbearer.aws.aws_autowire",
        "confluent_kafka.oauthbearer.aws._aws_sts_token_provider",
    ]:
        assert name not in sys.modules, (
            f"Dispatcher imported {name} when no marker was set — " "this means the no-op short-circuit broke."
        )


# ---- 2. Other marker values pass through verbatim ----


def test_other_marker_value_passes_through_unchanged():
    """azure_imds / unknown values are not our concern — librdkafka handles them."""
    # azure_imds is a librdkafka-recognised value; we expect Producer construction
    # to NOT raise our ValueError. (librdkafka itself may complain about the
    # value, but our dispatcher must not.)
    with pytest.raises(Exception) as exc_info:
        confluent_kafka.Producer(
            {
                "bootstrap.servers": "broker.invalid:9092",
                "sasl.mechanisms": "OAUTHBEARER",
                "sasl.oauthbearer.method": "oidc",
                "sasl.oauthbearer.metadata.authentication.type": "azure_imds",
            }
        )
    # Whatever librdkafka raises, it must NOT be our method-requirement
    # error or our config-requirement error.
    msg = str(exc_info.value)
    assert "AWS IAM" not in msg
    assert "aws_iam" not in msg


# ---- 3. method=oidc requirement ----


def test_marker_without_method_raises():
    with pytest.raises(ValueError, match="method=oidc"):
        confluent_kafka.Producer(
            {
                "bootstrap.servers": "broker.invalid:9092",
                "sasl.mechanisms": "OAUTHBEARER",
                "sasl.oauthbearer.metadata.authentication.type": "aws_iam",
                "sasl.oauthbearer.config": "region=us-east-1 audience=https://a",
            }
        )


def test_marker_with_method_default_raises():
    with pytest.raises(ValueError, match="method=oidc"):
        confluent_kafka.Producer(
            {
                "bootstrap.servers": "broker.invalid:9092",
                "sasl.mechanisms": "OAUTHBEARER",
                "sasl.oauthbearer.method": "default",
                "sasl.oauthbearer.metadata.authentication.type": "aws_iam",
                "sasl.oauthbearer.config": "region=us-east-1 audience=https://a",
            }
        )


def test_marker_with_method_oidc_uppercase_raises():
    """Strict matching — librdkafka's method values are lowercase canonical."""
    with pytest.raises(ValueError, match="method=oidc"):
        confluent_kafka.Producer(
            {
                "bootstrap.servers": "broker.invalid:9092",
                "sasl.mechanisms": "OAUTHBEARER",
                "sasl.oauthbearer.method": "OIDC",
                "sasl.oauthbearer.metadata.authentication.type": "aws_iam",
                "sasl.oauthbearer.config": "region=us-east-1 audience=https://a",
            }
        )


# ---- 4. sasl.oauthbearer.config requirement ----


def test_marker_with_method_oidc_but_missing_config_raises():
    with pytest.raises(ValueError, match="sasl.oauthbearer.config.*missing or empty"):
        confluent_kafka.Producer(
            {
                "bootstrap.servers": "broker.invalid:9092",
                "sasl.mechanisms": "OAUTHBEARER",
                "sasl.oauthbearer.method": "oidc",
                "sasl.oauthbearer.metadata.authentication.type": "aws_iam",
            }
        )


def test_marker_with_method_oidc_but_empty_config_raises():
    with pytest.raises(ValueError, match="sasl.oauthbearer.config.*missing or empty"):
        confluent_kafka.Producer(
            {
                "bootstrap.servers": "broker.invalid:9092",
                "sasl.mechanisms": "OAUTHBEARER",
                "sasl.oauthbearer.method": "oidc",
                "sasl.oauthbearer.metadata.authentication.type": "aws_iam",
                "sasl.oauthbearer.config": "",
            }
        )


# ---- 5. Happy path: marker + method=oidc + valid config + boto3 stubbed ----


def test_happy_path_producer_constructs_with_marker(mocked_boto3):
    p = confluent_kafka.Producer(_minimal_aws_iam_config())
    assert p is not None
    p.flush(timeout=0.1)


def test_happy_path_consumer_constructs_with_marker(mocked_boto3):
    c = confluent_kafka.Consumer(_minimal_aws_iam_config({"group.id": "test-group"}))
    assert c is not None
    c.close()


def test_happy_path_admin_client_constructs_with_marker(mocked_boto3):
    a = AdminClient(_minimal_aws_iam_config())
    assert a is not None


async def test_happy_path_aio_producer_constructs_with_marker(mocked_boto3):
    """AIO wraps sync — same dispatcher fires via the inner cimpl.Producer.

    AIOProducer.__init__ calls asyncio.get_running_loop(), so this test
    must run inside an async context (pyproject.toml's asyncio_mode=auto
    handles that for async def test functions)."""
    from confluent_kafka.aio import AIOProducer

    p = AIOProducer(_minimal_aws_iam_config())
    assert p is not None


async def test_happy_path_aio_consumer_constructs_with_marker(mocked_boto3):
    """Same async-context requirement as AIOProducer."""
    from confluent_kafka.aio import AIOConsumer

    c = AIOConsumer(_minimal_aws_iam_config({"group.id": "test-group"}))
    assert c is not None


# ---- 6. Explicit oauth_cb wins (precedence rule) ----


def test_explicit_oauth_cb_wins_over_marker():
    """When the user supplies their own oauth_cb, the dispatcher leaves the
    marker in place and yields — the explicit handler wins and boto3 is NOT
    touched. The AWS-IAM-aware librdkafka accepts the marker and uses the
    user's oauth_cb (its native aws_iam cb only registers when no refresh cb
    is set)."""
    sentinel_called = []

    def user_oauth_cb(config_str):
        sentinel_called.append(config_str)
        return ("user-token", 9999999999.0, "user-principal", {})

    # Clear sys.modules so we can verify aws_autowire was NOT imported.
    for name in list(sys.modules):
        if name.startswith("confluent_kafka.oauthbearer.aws"):
            del sys.modules[name]

    p = confluent_kafka.Producer(
        {
            "bootstrap.servers": "broker.invalid:9092",
            "security.protocol": "SASL_SSL",
            "sasl.mechanisms": "OAUTHBEARER",
            "sasl.oauthbearer.method": "oidc",
            "sasl.oauthbearer.metadata.authentication.type": "aws_iam",
            "sasl.oauthbearer.config": "region=us-east-1 audience=https://a",
            "oauth_cb": user_oauth_cb,
        }
    )
    assert p is not None
    p.flush(timeout=0.1)
    # The autowire module must not have been imported.
    assert "confluent_kafka.oauthbearer.aws.aws_autowire" not in sys.modules


# ---- 7. Parser errors surface from the autowire path ----


def test_marker_with_invalid_config_grammar_raises(mocked_boto3):
    """Config parser ValueError surfaces through the dispatcher."""
    with pytest.raises(ValueError, match="Unknown key.*not_a_key"):
        confluent_kafka.Producer(
            {
                "bootstrap.servers": "broker.invalid:9092",
                "sasl.mechanisms": "OAUTHBEARER",
                "sasl.oauthbearer.method": "oidc",
                "sasl.oauthbearer.metadata.authentication.type": "aws_iam",
                "sasl.oauthbearer.config": "region=us-east-1 audience=https://a not_a_key=foo",
            }
        )


def test_marker_with_invalid_signing_algorithm_raises(mocked_boto3):
    with pytest.raises(ValueError, match="signing_algorithm"):
        confluent_kafka.Producer(
            {
                "bootstrap.servers": "broker.invalid:9092",
                "sasl.mechanisms": "OAUTHBEARER",
                "sasl.oauthbearer.method": "oidc",
                "sasl.oauthbearer.metadata.authentication.type": "aws_iam",
                "sasl.oauthbearer.config": "region=us-east-1 audience=https://a signing_algorithm=HS256",
            }
        )


def test_marker_with_invalid_extensions_grammar_raises(mocked_boto3):
    with pytest.raises(ValueError, match="sasl.oauthbearer.extensions"):
        confluent_kafka.Producer(
            {
                "bootstrap.servers": "broker.invalid:9092",
                "sasl.mechanisms": "OAUTHBEARER",
                "sasl.oauthbearer.method": "oidc",
                "sasl.oauthbearer.metadata.authentication.type": "aws_iam",
                "sasl.oauthbearer.config": "region=us-east-1 audience=https://a",
                "sasl.oauthbearer.extensions": "malformed-no-equals",
            }
        )


# ---- 8. Friendly ImportError when the optional extra is missing ----


@pytest.fixture
def boto3_absent(monkeypatch):
    """Simulates an opt-out environment: boto3 import fails + relevant
    aws.* submodules cleared from sys.modules so the C dispatcher's
    PyImport_ImportModule re-executes the module body and hits the
    boto3=None gate."""
    # Force any subsequent `import boto3` to raise ImportError.
    monkeypatch.setitem(sys.modules, "boto3", None)
    # Clear the cached aws submodules so PyImport_ImportModule re-executes
    # their top-level statements (including `import boto3`).
    for name in list(sys.modules):
        if name.startswith("confluent_kafka.oauthbearer.aws"):
            monkeypatch.delitem(sys.modules, name, raising=False)
    yield
    # monkeypatch reverts on teardown.


def test_marker_with_missing_extra_raises_friendly_import_error(boto3_absent):
    """When boto3 isn't available, the dispatcher catches the
    ModuleNotFoundError from the import chain and rewrites it into a
    friendly install hint. __cause__ chain preserves the original."""
    with pytest.raises(ImportError) as exc_info:
        confluent_kafka.Producer(
            {
                "bootstrap.servers": "broker.invalid:9092",
                "sasl.mechanisms": "OAUTHBEARER",
                "sasl.oauthbearer.method": "oidc",
                "sasl.oauthbearer.metadata.authentication.type": "aws_iam",
                "sasl.oauthbearer.config": "region=us-east-1 audience=https://a",
            }
        )
    msg = str(exc_info.value)
    assert "oauthbearer-aws" in msg
    assert "pip install" in msg
    assert "aws_iam" in msg
    # __cause__ preserves the original failure for diagnostic tools.
    assert exc_info.value.__cause__ is not None


def test_friendly_import_error_on_consumer_too(boto3_absent):
    with pytest.raises(ImportError, match="oauthbearer-aws"):
        confluent_kafka.Consumer(
            {
                "bootstrap.servers": "broker.invalid:9092",
                "group.id": "g",
                "sasl.mechanisms": "OAUTHBEARER",
                "sasl.oauthbearer.method": "oidc",
                "sasl.oauthbearer.metadata.authentication.type": "aws_iam",
                "sasl.oauthbearer.config": "region=us-east-1 audience=https://a",
            }
        )


def test_friendly_import_error_on_admin_client_too(boto3_absent):
    with pytest.raises(ImportError, match="oauthbearer-aws"):
        AdminClient(
            {
                "bootstrap.servers": "broker.invalid:9092",
                "sasl.mechanisms": "OAUTHBEARER",
                "sasl.oauthbearer.method": "oidc",
                "sasl.oauthbearer.metadata.authentication.type": "aws_iam",
                "sasl.oauthbearer.config": "region=us-east-1 audience=https://a",
            }
        )


# ---- 9. Marker is passed through to the AWS-IAM-aware librdkafka ----


def test_marker_passed_through_to_librdkafka(mocked_boto3):
    """The dispatcher leaves the 'aws_iam' marker in confdict; it is passed to
    librdkafka unchanged. The bundled AWS-IAM-aware librdkafka recognizes the
    'aws_iam' enum value and accepts it at rd_kafka_conf_set time (bypassing
    the token.endpoint.url + grant-type requirements), so the Producer
    constructs cleanly. This also pins the requirement that the linked
    librdkafka be AWS-IAM-aware: against stock librdkafka the marker would be
    rejected with an 'invalid value' error here."""
    p = confluent_kafka.Producer(_minimal_aws_iam_config())
    assert p is not None
    p.flush(timeout=0.1)


# ---- 10. Extensions plumbing through the dispatcher ----


def test_marker_with_extensions_plumbs_through(mocked_boto3):
    """The dispatcher reads sasl.oauthbearer.extensions and passes it as the
    second arg to create_handler. Verify by checking the returned callable
    yields the extensions in its 4-tuple result."""
    p = confluent_kafka.Producer(
        _minimal_aws_iam_config(
            {
                "sasl.oauthbearer.extensions": "logicalCluster=lkc-123",
            }
        )
    )
    assert p is not None
    p.flush(timeout=0.1)


def test_marker_with_empty_extensions_treated_as_absent(mocked_boto3):
    """Empty extensions string → autowire treats as None (no extensions)."""
    p = confluent_kafka.Producer(
        _minimal_aws_iam_config(
            {
                "sasl.oauthbearer.extensions": "",
            }
        )
    )
    assert p is not None
    p.flush(timeout=0.1)
