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

# Live AWS STS tokens observed at ~1256 bytes across Go / .NET / JS / librdkafka.
# 8 KB is a generous ceiling that bounds attacker-controlled allocation while
# leaving plenty of headroom for future AWS payload growth.
_MAX_JWT_BYTES = 8 * 1024


def extract_sub(jwt: str) -> str:
    """Extract the 'sub' claim from an unverified JWT payload.

    AWS has already signed this token; signature verification is not our job.
    We only need the claim for the principal name forwarded to
    rd_kafka_oauthbearer_set_token (see confluent_kafka.c:2317).
    """
    if not isinstance(jwt, str) or not jwt:
        raise ValueError("JWT must be a non-empty string")
    if len(jwt.encode("utf-8")) > _MAX_JWT_BYTES:
        raise ValueError(f"JWT exceeds {_MAX_JWT_BYTES}-byte size ceiling")

    parts = jwt.split(".")
    if len(parts) != 3:
        raise ValueError(
            f"JWT must have 3 dot-separated segments, got {len(parts)}"
        )

    payload_b64 = parts[1]
    # base64url → base64, pad to multiple of 4. AWS tokens are unpadded; the
    # spec (RFC 7515 §2) permits either. Use strict validate=True so stray
    # non-alphabet characters raise instead of being silently dropped.
    padded = payload_b64 + "=" * (-len(payload_b64) % 4)
    standard = padded.replace("-", "+").replace("_", "/")
    try:
        decoded = base64.b64decode(standard.encode("ascii"), validate=True)
    except (ValueError, UnicodeEncodeError, base64.binascii.Error) as exc:
        raise ValueError(f"JWT payload is not valid base64url: {exc}") from exc

    try:
        claims = json.loads(decoded)
    except json.JSONDecodeError as exc:
        raise ValueError(f"JWT payload is not valid JSON: {exc}") from exc

    if not isinstance(claims, dict):
        raise ValueError("JWT payload must decode to a JSON object")

    sub = claims.get("sub")
    if not isinstance(sub, str) or not sub:
        raise ValueError("JWT payload missing or empty 'sub' claim")
    return sub
