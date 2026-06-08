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

"""Tests for confluent_kafka.oauthbearer.aws._aws_jwt_subject_extractor."""

import base64

import pytest

from confluent_kafka.oauthbearer.aws._aws_jwt_subject_extractor import extract_sub

# ---- Test helpers ----


def _base64url_encode(data: bytes) -> str:
    """Base64url-encodes a byte array: standard base64, trim '=' padding,
    swap '+' → '-' and '/' → '_'.
    """
    return base64.b64encode(data).decode("ascii").rstrip("=").replace("+", "-").replace("/", "_")


def _make_jwt(payload_json: str, alg: str = "ES384") -> str:
    """Build a 3-segment JWT with the given payload JSON.

    Header and signature segments are placeholders — ``extract_sub`` only
    decodes the payload (``parts[1]``).
    """
    header = _base64url_encode(f'{{"alg":"{alg}","typ":"JWT"}}'.encode("utf-8"))
    payload = _base64url_encode(payload_json.encode("utf-8"))
    return f"{header}.{payload}.sig"


# ---- Real STS payloads (from .NET test suite — signature stripped, only payload matters) ----


_EXPECTED_ROLE_ARN = "arn:aws:iam::708975691912:role/prashah-iam-sts-test-role"

_REAL_ES384_JWT = (
    "eyJraWQiOiJFQzM4NF8wIiwidHlwIjoiSldUIiwiYWxnIjoiRVMzODQifQ"
    ".eyJhdWQiOiJodHRwczovL2FwaS5leGFtcGxlLmNvbSIsInN1YiI6ImFybjphd3M6aWFtOjo3MDg5N"
    "zU2OTE5MTI6cm9sZS9wcmFzaGFoLWlhbS1zdHMtdGVzdC1yb2xlIiwiaHR0cHM6Ly9zdHMuYW1hem9u"
    "YXdzLmNvbS8iOnsiZWMyX2luc3RhbmNlX3NvdXJjZV92cGMiOiJ2cGMtYWQ4NzMzYzQiLCJlYzJfcm9"
    "sZV9kZWxpdmVyeSI6IjIuMCIsIm9yZ19pZCI6Im8tMHgzdDh1bW9seiIsImF3c19hY2NvdW50IjoiNz"
    "A4OTc1NjkxOTEyIiwib3VfcGF0aCI6WyJvLTB4M3Q4dW1vbHovci16YzVqL291LXpjNWotNXJwd3pqb"
    "HIvIl0sIm9yaWdpbmFsX3Nlc3Npb25fZXhwIjoiMjAyNi0wNS0xNFQyMzoxNjozMloiLCJzb3VyY2Vf"
    "cmVnaW9uIjoiZXUtbm9ydGgtMSIsImVjMl9zb3VyY2VfaW5zdGFuY2VfYXJuIjoiYXJuOmF3czplYzI"
    "6ZXUtbm9ydGgtMTo3MDg5NzU2OTE5MTI6aW5zdGFuY2UvaS0wOTc5MGY5OTY4YzExYTFjNyIsInByaW"
    "5jaXBhbF9pZCI6ImFybjphd3M6aWFtOjo3MDg5NzU2OTE5MTI6cm9sZS9wcmFzaGFoLWlhbS1zdHMtd"
    "GVzdC1yb2xlIiwicHJpbmNpcGFsX3RhZ3MiOnsiZGl2dnlfb3duZXIiOiJwcmFzaGFoQGNvbmZsdWVu"
    "dC5pbyIsImRpdnZ5X2xhc3RfbW9kaWZpZWRfYnkiOiJwcmFzaGFoQGNvbmZsdWVudC5pbyJ9LCJlYzJ"
    "faW5zdGFuY2Vfc291cmNlX3ByaXZhdGVfaXB2NCI6IjE3Mi4zMS4zLjEwOSJ9LCJpc3MiOiJodHRwcz"
    "ovL2ExZWJjNzA1LWNkNGQtNDJiNC05M2I1LTk2ZTkzYWNmYjQzMS50b2tlbnMuc3RzLmdsb2JhbC5hc"
    "GkuYXdzIiwiZXhwIjoxNzc4Nzc4NzY2LCJpYXQiOjE3Nzg3Nzg0NjYsImp0aSI6IjFkM2QzZmMyLTBl"
    "NzktNDI2OS05NjcxLTJmODQ4NDYxOWZiNyJ9"
    ".sig"
)

_REAL_RS256_JWT = (
    "eyJraWQiOiJSU0FfMCIsInR5cCI6IkpXVCIsImFsZyI6IlJTMjU2In0"
    ".eyJhdWQiOiJodHRwczovL2FwaS5leGFtcGxlLmNvbSIsInN1YiI6ImFybjphd3M6aWFtOjo3MDg5N"
    "zU2OTE5MTI6cm9sZS9wcmFzaGFoLWlhbS1zdHMtdGVzdC1yb2xlIiwiaHR0cHM6Ly9zdHMuYW1hem9u"
    "YXdzLmNvbS8iOnsiZWMyX2luc3RhbmNlX3NvdXJjZV92cGMiOiJ2cGMtYWQ4NzMzYzQiLCJlYzJfcm9"
    "sZV9kZWxpdmVyeSI6IjIuMCIsIm9yZ19pZCI6Im8tMHgzdDh1bW9seiIsImF3c19hY2NvdW50IjoiNz"
    "A4OTc1NjkxOTEyIiwib3VfcGF0aCI6WyJvLTB4M3Q4dW1vbHovci16YzVqL291LXpjNWotNXJwd3pqb"
    "HIvIl0sIm9yaWdpbmFsX3Nlc3Npb25fZXhwIjoiMjAyNi0wNS0xNFQyMzoxNjozMloiLCJzb3VyY2Vf"
    "cmVnaW9uIjoiZXUtbm9ydGgtMSIsImVjMl9zb3VyY2VfaW5zdGFuY2VfYXJuIjoiYXJuOmF3czplYzI"
    "6ZXUtbm9ydGgtMTo3MDg5NzU2OTE5MTI6aW5zdGFuY2UvaS0wOTc5MGY5OTY4YzExYTFjNyIsInByaW"
    "5jaXBhbF9pZCI6ImFybjphd3M6aWFtOjo3MDg5NzU2OTE5MTI6cm9sZS9wcmFzaGFoLWlhbS1zdHMtd"
    "GVzdC1yb2xlIiwicHJpbmNpcGFsX3RhZ3MiOnsiZGl2dnlfb3duZXIiOiJwcmFzaGFoQGNvbmZsdWVu"
    "dC5pbyIsImRpdnZ5X2xhc3RfbW9kaWZpZWRfYnkiOiJwcmFzaGFoQGNvbmZsdWVudC5pbyJ9LCJlYzJ"
    "faW5zdGFuY2Vfc291cmNlX3ByaXZhdGVfaXB2NCI6IjE3Mi4zMS4zLjEwOSJ9LCJpc3MiOiJodHRwcz"
    "ovL2ExZWJjNzA1LWNkNGQtNDJiNC05M2I1LTk2ZTkzYWNmYjQzMS50b2tlbnMuc3RzLmdsb2JhbC5hc"
    "GkuYXdzIiwiZXhwIjoxNzc4Nzc4NzY2LCJpYXQiOjE3Nzg3Nzg0NjYsImp0aSI6IjU5OTliZDc1LTBm"
    "NTctNDMzMS1iOGExLWJkYzYwMjgxN2UyZiJ9"
    ".sig"
)


# ---- Happy paths ----


def test_extract_sub_role_arn_returned():
    jwt = _make_jwt('{"sub":"arn:aws:iam::123456789012:role/MyRole","iat":1}')
    assert extract_sub(jwt) == "arn:aws:iam::123456789012:role/MyRole"


def test_extract_sub_assumed_role_arn_returned():
    jwt = _make_jwt('{"sub":"arn:aws:sts::123456789012:assumed-role/MyRole/session-name"}')
    assert extract_sub(jwt) == "arn:aws:sts::123456789012:assumed-role/MyRole/session-name"


def test_extract_sub_other_claims_ignored():
    jwt = _make_jwt('{"iss":"https://x","sub":"arn:aws:iam::1:role/R",' '"aud":"a","exp":1,"iat":0,"jti":"j"}')
    assert extract_sub(jwt) == "arn:aws:iam::1:role/R"


@pytest.mark.parametrize("jwt", [_REAL_ES384_JWT, _REAL_RS256_JWT])
def test_extract_sub_real_sts_jwt_returns_expected_arn(jwt):
    assert extract_sub(jwt) == _EXPECTED_ROLE_ARN


# ---- Base64url padding branches (% 4 ∈ {0, 2, 3}) ----


def test_extract_sub_unpadded_base64url_works():
    unpadded = _make_jwt('{"sub":"a"}')
    assert extract_sub(unpadded) == "a"


def test_extract_sub_padded_base64url_also_works():
    """If user pre-pads with '=' (non-canonical but tolerated), still decodes."""
    header = _base64url_encode(b'{"alg":"none"}')
    # Encode WITHOUT stripping padding.
    payload = base64.b64encode(b'{"sub":"abc"}').decode("ascii").replace("+", "-").replace("/", "_")
    jwt_3seg = f"{header}.{payload}.sig"
    assert extract_sub(jwt_3seg) == "abc"


def test_extract_sub_url_safe_chars_handled():
    bytes_ = b'{"sub":"x"}'
    normal = base64.b64encode(bytes_).decode("ascii")
    url_safe = normal.replace("+", "-").replace("/", "_").rstrip("=")
    jwt = "aGVhZGVy." + url_safe + ".c2ln"
    assert extract_sub(jwt) == "x"


@pytest.mark.parametrize(
    "payload_json,expected_sub",
    [
        ('{"sub":"a"}', "a"),  # → encoded length % 4 = 3 (one '=' padded)
        ('{"sub":"ab"}', "ab"),  # → encoded length % 4 = 0 (no padding)
        ('{"sub":"abc"}', "abc"),  # → encoded length % 4 = 2 (two '==' padded)
    ],
)
def test_extract_sub_padding_branches_all_hit_decodes_correctly(payload_json, expected_sub):
    """Explicit per-branch coverage of the % 4 padding switch."""
    assert extract_sub(_make_jwt(payload_json)) == expected_sub


# ---- Top-level shape errors ----


def test_extract_sub_null_raises():
    with pytest.raises(ValueError, match="null"):
        extract_sub(None)


def test_extract_sub_empty_raises():
    with pytest.raises(ValueError, match="empty"):
        extract_sub("")


def test_extract_sub_one_segment_raises():
    with pytest.raises(ValueError, match="3"):
        extract_sub("onlyonepart")


def test_extract_sub_two_segments_raises():
    with pytest.raises(ValueError, match="3"):
        extract_sub("a.b")


def test_extract_sub_four_segments_raises():
    with pytest.raises(ValueError, match="3"):
        extract_sub("a.b.c.d")


def test_extract_sub_oversized_input_raises():
    oversized = "a" * 8193
    with pytest.raises(ValueError, match="exceeds maximum"):
        extract_sub(oversized)


def test_extract_sub_at_ceiling_reaches_parser():
    """8192-char input passes the size gate and fails downstream on segment count."""
    at_ceiling = "a" * 8192
    with pytest.raises(ValueError, match="3"):
        extract_sub(at_ceiling)


def test_extract_sub_empty_payload_segment_raises():
    with pytest.raises(ValueError, match="empty"):
        extract_sub("header..sig")


# ---- Payload-content errors ----


def test_extract_sub_malformed_base64_in_payload_raises():
    with pytest.raises(ValueError, match="base64url"):
        extract_sub("aGVhZGVy.not!base64.c2ln")


def test_extract_sub_malformed_json_in_payload_raises():
    bad_payload = _base64url_encode(b"not json")
    with pytest.raises(ValueError, match="not valid JSON"):
        extract_sub(f"aGVhZGVy.{bad_payload}.c2ln")


def test_extract_sub_payload_is_json_array_raises():
    array_payload = _base64url_encode(b'["not","an","object"]')
    with pytest.raises(ValueError, match="not a JSON object"):
        extract_sub(f"aGVhZGVy.{array_payload}.c2ln")


def test_extract_sub_missing_sub_claim_raises():
    jwt = _make_jwt('{"iss":"https://x","aud":"a"}')
    with pytest.raises(ValueError, match="'sub'"):
        extract_sub(jwt)


def test_extract_sub_sub_claim_is_number_raises():
    jwt = _make_jwt('{"sub":12345}')
    with pytest.raises(ValueError, match="'sub'"):
        extract_sub(jwt)


def test_extract_sub_sub_claim_is_null_raises():
    jwt = _make_jwt('{"sub":null}')
    with pytest.raises(ValueError, match="'sub'"):
        extract_sub(jwt)


def test_extract_sub_sub_claim_is_empty_string_raises():
    jwt = _make_jwt('{"sub":""}')
    with pytest.raises(ValueError, match="empty"):
        extract_sub(jwt)
