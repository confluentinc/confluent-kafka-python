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

import pytest

from confluent_kafka.oauthbearer.aws._jwt import extract_sub


def _make_jwt(payload: dict, unpadded: bool = True) -> str:
    """Build a 3-segment JWT with header='h', signature='s' and an arbitrary payload."""
    body = json.dumps(payload).encode("utf-8")
    encoded = base64.urlsafe_b64encode(body).decode("ascii")
    if unpadded:
        encoded = encoded.rstrip("=")
    return f"h.{encoded}.s"


def test_role_arn():
    jwt = _make_jwt({"sub": "arn:aws:iam::123456789012:role/MyRole"})
    assert extract_sub(jwt) == "arn:aws:iam::123456789012:role/MyRole"


def test_assumed_role_arn():
    sub = "arn:aws:sts::123456789012:assumed-role/MyRole/session-name"
    assert extract_sub(_make_jwt({"sub": sub})) == sub


def test_missing_sub_claim():
    jwt = _make_jwt({"iss": "https://issuer.example.com"})
    with pytest.raises(ValueError, match="sub"):
        extract_sub(jwt)


def test_too_few_segments():
    with pytest.raises(ValueError, match="3 dot-separated segments"):
        extract_sub("only.two")


def test_too_many_segments():
    with pytest.raises(ValueError, match="3 dot-separated segments"):
        extract_sub("a.b.c.d")


def test_malformed_base64url():
    # '!!!' is not valid base64url
    with pytest.raises(ValueError, match="base64url"):
        extract_sub("h.!!!.s")


def test_malformed_json():
    payload_b64 = base64.urlsafe_b64encode(b"not-json").decode("ascii").rstrip("=")
    with pytest.raises(ValueError, match="JSON"):
        extract_sub(f"h.{payload_b64}.s")


def test_empty_string():
    with pytest.raises(ValueError, match="non-empty"):
        extract_sub("")


def test_none_input():
    with pytest.raises(ValueError, match="non-empty"):
        extract_sub(None)  # type: ignore[arg-type]


def test_oversized_input():
    # 9 KB is above the 8 KB ceiling
    oversized = "a" * (9 * 1024)
    with pytest.raises(ValueError, match="size ceiling"):
        extract_sub(oversized)


def test_unpadded_base64url():
    jwt = _make_jwt({"sub": "user1"}, unpadded=True)
    assert extract_sub(jwt) == "user1"


def test_padded_base64url():
    jwt = _make_jwt({"sub": "user1"}, unpadded=False)
    assert extract_sub(jwt) == "user1"


def test_non_string_sub():
    jwt = _make_jwt({"sub": 12345})
    with pytest.raises(ValueError, match="sub"):
        extract_sub(jwt)


def test_empty_string_sub():
    jwt = _make_jwt({"sub": ""})
    with pytest.raises(ValueError, match="sub"):
        extract_sub(jwt)


def test_payload_not_object():
    # JSON payload is an array, not an object
    payload_b64 = base64.urlsafe_b64encode(b'["a","b"]').decode("ascii").rstrip("=")
    with pytest.raises(ValueError, match="JSON object"):
        extract_sub(f"h.{payload_b64}.s")
