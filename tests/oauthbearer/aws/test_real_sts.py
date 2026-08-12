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

"""Phase 6 — real-AWS integration tests for the autowire path.

Gated on ``RUN_AWS_STS_REAL=1``. Default ``pytest tests/oauthbearer/aws``
runs every other Phase 1-5 case but skips this file's tests so CI doesn't
hit the AWS API.

Why this file lives under ``tests/oauthbearer/aws/`` rather than
``tests/integration/``: ``tests/integration/conftest.py`` eagerly imports
``trivup.clusters.KafkaCluster``, which is a heavyweight broker-only
dependency. The STS provider has no broker dependency — the env-var gate
is sufficient isolation. Same gate-placement call as the 29-April POC.

Cross-language invariant: on the shared EC2 test box
(``ktrue-iam-sts-test-role`` in ``eu-north-1``, account ``708975691912``,
audience ``https://api.example.com``) Go / .NET / JS / librdkafka all
mint a **1256-byte JWT** with principal
``arn:aws:iam::708975691912:role/ktrue-iam-sts-test-role``. The Python
port preserves that invariant.

Run instructions::

    # On EC2 with role attached, audience-trust-policy enabled:
    export RUN_AWS_STS_REAL=1
    export AWS_STS_TEST_REGION=eu-north-1     # AWS_REGION also accepted (fallback)
    export AWS_STS_TEST_AUDIENCE=https://api.example.com
    /tmp/ckp-optin/bin/pytest -v -s tests/oauthbearer/aws/test_real_sts.py

The ``-s`` flag preserves the diagnostic ``print(...)`` lines that capture
the JWT length, principal, and timing — important for cross-language
parity evidence in PR descriptions.
"""

import base64
import json
import os
import re
import time

import pytest

# boto3 is loaded transitively via the autowire path. Skip the whole file
# in opt-out venvs so collection doesn't blow up.
pytest.importorskip("boto3")

# Gate the entire module behind RUN_AWS_STS_REAL=1 so CI doesn't accidentally
# hit STS. Individual @pytest.mark.skipif decorators would work too but the
# module-level pytestmark is cleaner and less error-prone.
pytestmark = pytest.mark.skipif(
    os.environ.get("RUN_AWS_STS_REAL") != "1",
    reason="Set RUN_AWS_STS_REAL=1 and provide AWS credentials to run.",
)

from confluent_kafka._oauthbearer.aws.aws_autowire import create_handler  # noqa: E402

# =============================================================================
# Configuration sourced from env vars so the test can be re-pointed at a
# different role / audience without code changes.
# =============================================================================

_REGION = os.environ.get("AWS_STS_TEST_REGION") or os.environ.get("AWS_REGION", "eu-north-1")
_AUDIENCE = os.environ.get("AWS_STS_TEST_AUDIENCE", "https://api.example.com")
_DURATION = os.environ.get("DURATION_SECONDS", "300")

# Cross-language invariant: 1256 bytes on the shared test role + audience.
# Allow a tolerance band for variation across audiences (URL length affects
# payload size). Catches order-of-magnitude bugs without flaking on tiny
# audience-string changes.
_JWT_LENGTH_MIN = int(os.environ.get("JWT_LENGTH_MIN", "1100"))
_JWT_LENGTH_MAX = int(os.environ.get("JWT_LENGTH_MAX", "1500"))

_JWT_PATTERN = re.compile(r"^[A-Za-z0-9\-_]+\.[A-Za-z0-9\-_]+\.[A-Za-z0-9\-_]+$")
_ARN_PATTERN = re.compile(r"^arn:aws:(?:iam|sts)::\d+:[^/]+/.+$")


def _config_string(**parts) -> str:
    """Render a sasl.oauthbearer.config wire string from key=value parts."""
    return ",".join(f"{k}={v}" for k, v in parts.items())


def _default_config(**extra) -> str:
    """Build a sasl.oauthbearer.config string with the defaults plus extras."""
    parts = {"region": _REGION, "audience": _AUDIENCE, "duration_seconds": _DURATION}
    parts.update(extra)
    return _config_string(**parts)


def _decode_jwt_payload(jwt: str) -> dict:
    """Decode the JWT payload (middle segment) into a Python dict.

    Helper for tests that need to inspect specific claims (tags, audience,
    algorithm) end-to-end.
    """
    payload_b64url = jwt.split(".")[1]
    # base64url → base64 with padding
    padding = "=" * (-len(payload_b64url) % 4)
    standard_b64 = (payload_b64url + padding).replace("-", "+").replace("_", "/")
    return json.loads(base64.b64decode(standard_b64).decode("utf-8"))


# =============================================================================
# create_handler() — the cross-module entry-point contract
# =============================================================================


def test_create_handler_mints_valid_jwt():
    handler = create_handler(_default_config(), None)
    token, expiry, principal, extensions = handler("")

    # 3-segment base64url JWT.
    assert _JWT_PATTERN.match(token), f"not a 3-segment JWT: {token[:60]}..."

    # Length within cross-language tolerance.
    assert _JWT_LENGTH_MIN <= len(token) <= _JWT_LENGTH_MAX, (
        f"JWT length {len(token)} outside [{_JWT_LENGTH_MIN}, "
        f"{_JWT_LENGTH_MAX}] (Go/.NET/JS/librdkafka observe 1256 on the shared "
        f"test box). Set JWT_LENGTH_MIN/MAX env vars if running on a different "
        f"role/audience."
    )

    # Expiry within the requested window.
    now = time.time()
    assert expiry > now, "expiry must be in the future"
    assert expiry < now + 10 * 60, "expiry must be within 10 min for DurationSeconds <= 300"

    # Principal is a bare role ARN (AWS STS GetWebIdentityToken convention).
    assert _ARN_PATTERN.match(principal), f"unexpected principal: {principal!r}"

    # Default config has no extensions.
    assert extensions == {}

    # Diagnostic for the run log so EC2 evidence is captured in CI / PR.
    print(f"\n[autowire-real] jwt_length={len(token)} " f"principal={principal} expires_in={int(expiry - now)}s")


def test_create_handler_jwt_length_matches_cross_language():
    """Cross-language byte-exact JWT length parity check.

    Different roles produce different JWT lengths because AWS STS bakes
    role-attached ``principal_tags`` into the payload (e.g. provisioning-
    system tags like ``divvy_owner`` add ~70 bytes). The 1256-byte length
    seen on Go/.NET/JS/librdkafka in earlier validation was specific to
    one tag-less role.

    This test only runs when ``EXPECTED_TOKEN_LEN`` is explicitly set —
    its purpose is to confirm byte-for-byte parity against a baseline you've
    measured by running any sibling client (Go / .NET / JS) against the
    same role + audience.
    """
    expected_str = os.environ.get("EXPECTED_TOKEN_LEN")
    if not expected_str:
        pytest.skip(
            "EXPECTED_TOKEN_LEN not set — cross-language byte-exact parity "
            "check only runs when you supply a known-good baseline. Run a "
            "sibling client (Go/.NET/JS) against the same role+audience, "
            "note the JWT length, and set EXPECTED_TOKEN_LEN to that value."
        )

    expected = int(expected_str)
    handler = create_handler(_default_config(), None)
    token, *_ = handler("")
    assert len(token) == expected, f"expected {expected} bytes (cross-language match), got {len(token)}"


def test_create_handler_principal_matches_role_arn():
    """JWT 'sub' claim is the bare role ARN. AWS STS GetWebIdentityToken
    convention (no session prefix on bare role tokens)."""
    handler = create_handler(_default_config(), None)
    _, _, principal, _ = handler("")
    assert principal.startswith("arn:aws:iam::"), f"not an IAM role ARN: {principal!r}"
    assert ":role/" in principal, f"not a role ARN: {principal!r}"


def test_create_handler_repeats_mint_distinct_tokens():
    """Provider does not cache; STS mints a fresh JWT each invocation."""
    handler = create_handler(_default_config(), None)
    _, exp1, _, _ = handler("")
    time.sleep(1.0)
    _, exp2, _, _ = handler("")
    assert exp2 > exp1, "second token's expiry must be later than the first"


def test_create_handler_honours_duration_seconds():
    """Custom duration_seconds=60 produces an expiry ~1 min out, not the 300s
    default.

    The shared test role's IAM policy caps DurationSeconds at 300 (per .NET
    PR validation §2g and the JS hybrid plan). So we go *below* the default
    rather than above to prove the parameter is honored end-to-end. Same
    value JS uses for the same reason.
    """
    handler = create_handler(_default_config(duration_seconds="60"), None)
    _, expiry, _, _ = handler("")
    seconds_remaining = expiry - time.time()
    assert 50 <= seconds_remaining <= 80, f"duration_seconds=60 → expected ~60s window, got {seconds_remaining:.0f}s"


def test_create_handler_honours_signing_algorithm_rs256():
    """signing_algorithm=RS256 produces a JWT with alg=RS256 in the header.

    Cross-language parity check: the .NET test asserts the same header.alg
    round-trip. Catches any SDK-side algorithm-shaping surprises.
    """
    handler = create_handler(_default_config(signing_algorithm="RS256"), None)
    token, *_ = handler("")
    # Decode the JWT header (first segment).
    header_b64url = token.split(".")[0]
    padding = "=" * (-len(header_b64url) % 4)
    header_json = json.loads(
        base64.b64decode((header_b64url + padding).replace("-", "+").replace("_", "/")).decode("utf-8")
    )
    assert header_json.get("alg") == "RS256", f"header.alg expected RS256, got {header_json.get('alg')!r}"


def test_create_handler_round_trips_sasl_extensions():
    """The typed sasl.oauthbearer.extensions property is parsed separately
    (comma-separated) and surfaces in the returned extensions dict.

    Note: extensions are forwarded to the broker — not to STS — so they
    don't appear in the JWT. This test asserts the dispatcher-to-tuple
    round-trip, not the JWT payload."""
    handler = create_handler(
        _default_config(),
        "logicalCluster=lkc-abc,identityPoolId=pool-xyz",
    )
    _, _, _, extensions = handler("")
    assert extensions == {
        "logicalCluster": "lkc-abc",
        "identityPoolId": "pool-xyz",
    }


def test_create_handler_tag_claims_flow_to_sts():
    """tag_<NAME>=<VALUE> in the wire grammar flows into the STS Tags
    parameter without breaking the request.

    The primary evidence is **token minting succeeding** — if our Tags
    parameter caused STS to reject the request (e.g. malformed shape or
    over the 50-tag cap), we'd see a ClientError exception. Token returned
    cleanly means dispatcher → provider → boto3 → AWS STS handoff worked.

    Whether AWS STS surfaces our custom tags in the JWT payload's
    ``principal_tags`` claim is a SEPARATE question depending on the role's
    IAM trust policy — specifically whether ``sts:TagSession`` is allowed.
    Without that permission, STS silently drops user tags from the JWT.
    The diagnostic prints below show what STS actually embedded so users
    can spot the case where tags are silently dropped.

    Set ``ASSERT_TAGS_IN_JWT=1`` to require that our tags appear in the
    JWT — only meaningful on roles with ``sts:TagSession`` allowed.
    """
    handler = create_handler(
        _default_config(tag_team="platform", tag_environment="prod"),
        None,
    )
    # If our Tags param breaks the STS call, this raises.
    token, *_ = handler("")
    payload = _decode_jwt_payload(token)

    # AWS STS surfaces tags either under "principal_tags" at the JWT root
    # OR nested under "https://sts.amazonaws.com/" → "principal_tags".
    found_tags = {}
    if "principal_tags" in payload:
        found_tags.update(payload["principal_tags"])
    for nested_value in payload.values():
        if isinstance(nested_value, dict) and "principal_tags" in nested_value:
            found_tags.update(nested_value["principal_tags"])

    our_tags_present = "team" in found_tags and "environment" in found_tags
    print(f"\n[autowire-real] JWT payload keys: {list(payload.keys())}")
    print(f"[autowire-real] tags surfaced in JWT: {found_tags}")
    print(
        f"[autowire-real] our injected tags appear in JWT: {our_tags_present}"
        + ("" if our_tags_present else " (role likely missing sts:TagSession permission)")
    )

    # Token minted successfully — Tags param didn't cause STS rejection.
    assert len(token) > 100, "Token should be a valid JWT"

    # Strict assertion only when the user explicitly opts in via env var.
    if os.environ.get("ASSERT_TAGS_IN_JWT") == "1":
        assert our_tags_present, (
            f"ASSERT_TAGS_IN_JWT=1 set but our tags aren't in the JWT. "
            f"Role likely missing sts:TagSession permission. "
            f"Found tags: {found_tags}"
        )


def test_create_handler_jwt_audience_matches_request():
    """The JWT payload's 'aud' claim matches the audience we asked AWS for.

    Defensive — catches any audience-shaping surprises (AWS should not munge
    the audience string). Mirrors the .NET integration test's assertion.
    """
    handler = create_handler(_default_config(), None)
    token, *_ = handler("")
    payload = _decode_jwt_payload(token)
    assert payload.get("aud") == _AUDIENCE, f"JWT aud {payload.get('aud')!r} doesn't match requested {_AUDIENCE!r}"


# =============================================================================
# Full dispatcher flow — Producer with marker
# =============================================================================


def test_producer_with_marker_succeeds_against_real_sts():
    """End-to-end: construct a Producer with the marker set; the C dispatcher
    in common_conf_setup() invokes create_handler() which builds a real
    provider; librdkafka's background thread invokes the autowired callback;
    STS mints a real token; rd_kafka_oauthbearer_set_token marks the token
    as set; wait_for_oauth_token_set returns successfully and the Producer
    constructor returns.

    The bootstrap.servers points at a non-resolvable host so we don't waste
    a real broker connection — the OAUTHBEARER refresh path doesn't need
    one. It fires immediately on background-thread startup.
    """
    from confluent_kafka import Producer

    t0 = time.time()
    Producer(
        {
            "bootstrap.servers": "broker.invalid:9092",
            "security.protocol": "SASL_SSL",
            "sasl.mechanisms": "OAUTHBEARER",
            "sasl.oauthbearer.method": "oidc",
            "sasl.oauthbearer.metadata.authentication.type": "aws_iam",
            "sasl.oauthbearer.config": _default_config(),
        }
    )
    elapsed = time.time() - t0

    # Producer construction returns once the autowire callback has set a
    # real token (typically <2s on EC2). wait_for_oauth_token_set timeout
    # is 10s — anything over that means the autowire chain didn't fire.
    assert elapsed < 5.0, (
        f"Producer construction took {elapsed:.1f}s — autowire path slow "
        f"or broken (10s timeout = OAuth refresh never set the token)"
    )
    print(f"\n[autowire-real] Producer constructed via dispatcher in {elapsed:.2f}s")


def test_producer_with_marker_and_extensions_succeeds():
    """Same end-to-end path with sasl.oauthbearer.extensions set on the
    typed property. Proves the C dispatcher reads the extensions string
    and passes it as the second arg to create_handler."""
    from confluent_kafka import Producer

    t0 = time.time()
    Producer(
        {
            "bootstrap.servers": "broker.invalid:9092",
            "security.protocol": "SASL_SSL",
            "sasl.mechanisms": "OAUTHBEARER",
            "sasl.oauthbearer.method": "oidc",
            "sasl.oauthbearer.metadata.authentication.type": "aws_iam",
            "sasl.oauthbearer.config": _default_config(),
            "sasl.oauthbearer.extensions": "logicalCluster=lkc-abc",
        }
    )
    elapsed = time.time() - t0
    assert elapsed < 5.0, f"Producer with extensions took {elapsed:.1f}s — autowire path " f"slow or broken"
    print(f"\n[autowire-real] Producer (with extensions) constructed via " f"dispatcher in {elapsed:.2f}s")
