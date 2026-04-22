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

from dataclasses import dataclass, field
from typing import Any, Dict, Optional, Tuple

import boto3

from . import _jwt


_ALLOWED_SIGNING_ALGORITHMS = ("ES384", "RS256")
_MIN_DURATION_SECONDS = 60
_MAX_DURATION_SECONDS = 3600


@dataclass
class AwsOAuthConfig:
    """Configuration for AwsStsTokenProvider.

    Parameters map 1:1 to the sts:GetWebIdentityToken request, plus a handful
    of fields shaping the behaviour of the boto3 Session / STS client.

    All validation runs in __post_init__, so misconfigured instances fail
    synchronously at construction — no late errors on first broker contact.
    """

    # Required — no silent default, no IMDS sniffing. AWS region the STS
    # client is pinned to, e.g. "us-east-1".
    region: str

    # Required — OIDC audience claim the relying party expects.
    audience: str

    # "ES384" (default) or "RS256".
    signing_algorithm: str = "ES384"

    # Token lifetime requested from STS. 60–3600 seconds. Default 300.
    duration_seconds: int = 300

    # Optional STS endpoint override (FIPS, VPC endpoint). Must start with
    # "https://".
    sts_endpoint_url: Optional[str] = None

    # Optional pre-built boto3 Session. If None, a default Session is used,
    # which resolves credentials through the standard chain
    # (env → EKS IRSA → ECS → IMDSv2 → profile).
    session: Optional[boto3.Session] = field(default=None, repr=False)

    # Optional SASL extensions communicated to the broker (RFC 7628 §3.1).
    sasl_extensions: Optional[Dict[str, str]] = None

    # Optional override for the principal name forwarded to
    # rd_kafka_oauthbearer_set_token. When None, the JWT "sub" claim is used
    # (bare role ARN for AWS STS tokens).
    principal_name_override: Optional[str] = None

    def __post_init__(self) -> None:
        self._validate()

    def _validate(self) -> None:
        if not isinstance(self.region, str) or not self.region:
            raise ValueError("region is required (non-empty string)")
        if not isinstance(self.audience, str) or not self.audience:
            raise ValueError("audience is required (non-empty string)")
        if self.signing_algorithm not in _ALLOWED_SIGNING_ALGORITHMS:
            raise ValueError(
                f"signing_algorithm must be one of {_ALLOWED_SIGNING_ALGORITHMS}, "
                f"got {self.signing_algorithm!r}"
            )
        if not isinstance(self.duration_seconds, int) or isinstance(
            self.duration_seconds, bool
        ):
            raise ValueError("duration_seconds must be an int")
        if not _MIN_DURATION_SECONDS <= self.duration_seconds <= _MAX_DURATION_SECONDS:
            raise ValueError(
                "duration_seconds must be in "
                f"[{_MIN_DURATION_SECONDS}, {_MAX_DURATION_SECONDS}], "
                f"got {self.duration_seconds}"
            )
        if self.sts_endpoint_url is not None:
            if not isinstance(self.sts_endpoint_url, str) or not self.sts_endpoint_url:
                raise ValueError(
                    "sts_endpoint_url, if set, must be a non-empty string"
                )
            if not self.sts_endpoint_url.startswith("https://"):
                raise ValueError("sts_endpoint_url must start with 'https://'")
        if self.principal_name_override is not None:
            if (
                not isinstance(self.principal_name_override, str)
                or not self.principal_name_override
            ):
                raise ValueError(
                    "principal_name_override, if set, must be a non-empty string"
                )
        if self.sasl_extensions is not None and not isinstance(
            self.sasl_extensions, dict
        ):
            raise ValueError("sasl_extensions, if set, must be a dict")


class AwsStsTokenProvider:
    """Fetches OAUTHBEARER tokens via sts:GetWebIdentityToken.

    Thread-safe; share one instance across clients in a process.

    The instance method ``token`` is directly assignable to the ``oauth_cb``
    config parameter of ``confluent_kafka.Producer`` / ``Consumer`` /
    ``AdminClient``. It returns the 4-tuple
    ``(token_str, expiry_epoch_seconds, principal, extensions)`` that the
    C-extension oauth_cb contract expects — see confluent_kafka.c:2291.

    Credential resolution is lazy: constructing the provider only builds the
    boto3 Session / STS client, which does not invoke the credential chain.
    The first ``token`` call is what triggers credential resolution and the
    actual STS request.
    """

    def __init__(self, config: AwsOAuthConfig) -> None:
        self._cfg = config
        session = config.session or boto3.Session(region_name=config.region)
        client_kwargs: Dict[str, Any] = {"region_name": config.region}
        if config.sts_endpoint_url:
            client_kwargs["endpoint_url"] = config.sts_endpoint_url
        self._sts = session.client("sts", **client_kwargs)

    def token(
        self, oauthbearer_config: str = ""
    ) -> Tuple[str, float, str, Dict[str, str]]:
        """Mint a fresh JWT via sts:GetWebIdentityToken.

        Returns the 4-tuple the ``oauth_cb`` contract expects:
        ``(token_str, expiry_epoch_seconds_float, principal, extensions)``.

        Raises ``botocore.exceptions.ClientError`` on STS errors (AccessDenied,
        OutboundWebIdentityFederationDisabledException, …) and ``ValueError``
        on malformed JWT responses. The C-extension oauth_cb handler converts
        raised exceptions into ``rd_kafka_oauthbearer_set_token_failure``
        (see confluent_kafka.c:2338).

        The ``oauthbearer_config`` argument is the pass-through of
        ``sasl.oauthbearer.config`` from librdkafka; unused here.
        """
        resp = self._sts.get_web_identity_token(
            Audience=[self._cfg.audience],
            SigningAlgorithm=self._cfg.signing_algorithm,
            DurationSeconds=self._cfg.duration_seconds,
        )
        jwt_str: str = resp["WebIdentityToken"]
        # boto3 returns a tz-aware datetime in UTC.
        expiration = resp["Expiration"]
        expiry_epoch = expiration.timestamp()

        principal = (
            self._cfg.principal_name_override
            if self._cfg.principal_name_override is not None
            else _jwt.extract_sub(jwt_str)
        )
        extensions = self._cfg.sasl_extensions or {}
        return jwt_str, expiry_epoch, principal, extensions
