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

"""Internal: Fetches OAUTHBEARER tokens via AWS STS <c>GetWebIdentityToken."""

import logging
from typing import Any, Dict, Optional, Tuple

import boto3

from . import _aws_jwt_subject_extractor
from ._aws_oauthbearer_config import (
    AWS_DEBUG_CONSOLE,
    AwsOAuthBearerConfig,
)

__all__ = ["AwsStsTokenProvider"]


# Logger name targeted by ``aws_debug=console``. Routes botocore's HTTP /
# credential-chain / signing diagnostic logs to stderr at DEBUG level.
_BOTOCORE_LOGGER_NAME = "botocore"


class AwsStsTokenProvider:
    """Mints OAUTHBEARER tokens via AWS STS ``GetWebIdentityToken``."""

    def __init__(
        self,
        config: AwsOAuthBearerConfig,
        sts_client: Optional[Any] = None,
    ) -> None:
        """Construct a provider bound to ``config``.

        :param config: Validated :class:`AwsOAuthBearerConfig` instance.
        :param sts_client: Test seam — when supplied, the provider uses this
            client directly instead of constructing a real boto3 STS client.
            Production callers pass ``None``.
        :raises TypeError: ``config`` is ``None``.
        """
        if config is None:
            raise TypeError("config must not be None")
        self._cfg = config

        self._apply_aws_debug(config.aws_debug)

        if sts_client is not None:
            self._sts = sts_client
        else:
            # boto3.Session() with explicit region_name short-circuits the
            # AWS_DEFAULT_REGION env lookup; client() still re-asserts the
            # region for STS endpoint resolution.
            session = boto3.Session(region_name=config.region)
            client_kwargs: Dict[str, Any] = {"region_name": config.region}
            if config.sts_endpoint:
                client_kwargs["endpoint_url"] = config.sts_endpoint
            self._sts = session.client("sts", **client_kwargs)

    @staticmethod
    def _apply_aws_debug(aws_debug: str) -> None:
        """Apply the ``aws_debug`` side-effect to botocore's logger.

        Process-wide effect, intentionally — mirrors .NET's
        ``AWSConfigs.LoggingConfig.LogTo`` behaviour. When the user opts in
        with ``aws_debug=console``, every boto3 client in the process gets
        DEBUG-level stderr logs. ``aws_debug=none`` is a no-op so any
        logging the user has configured elsewhere is preserved.
        """
        if aws_debug == AWS_DEBUG_CONSOLE:
            boto3.set_stream_logger(_BOTOCORE_LOGGER_NAME, logging.DEBUG)
        # AWS_DEBUG_NONE → no-op. Other values are rejected by config validation.

    def token(
        self,
        oauthbearer_config: str = "",
    ) -> Tuple[str, float, str, Dict[str, str]]:
        """Mint a fresh JWT and return the ``oauth_cb`` 4-tuple.

        :param oauthbearer_config: The verbatim ``sasl.oauthbearer.config``
            string librdkafka passes back on every refresh. Accepted for
            interface completeness but unused — the AWS path's fields are
            sourced from the bound :class:`AwsOAuthBearerConfig` at
            construction time, not re-parsed per refresh.

        :returns: 4-tuple ``(token, expiry_epoch_seconds, principal,
            extensions)`` matching the C ``oauth_cb`` contract.

        :raises botocore.exceptions.ClientError: STS-side error
            (``AccessDenied``, ``OutboundWebIdentityFederationDisabled``,
            ...). The C ``oauth_cb`` wrapper converts raised exceptions
            into ``rd_kafka_oauthbearer_set_token_failure``.
        :raises ValueError: STS returned a malformed JWT or missing
            ``Expiration``.
        """
        request_kwargs: Dict[str, Any] = {
            "Audience": [self._cfg.audience],
            "SigningAlgorithm": self._cfg.signing_algorithm,
            "DurationSeconds": self._cfg.duration_seconds,
        }
        if self._cfg.tags:
            request_kwargs["Tags"] = [{"Key": k, "Value": v} for k, v in self._cfg.tags.items()]

        response = self._sts.get_web_identity_token(**request_kwargs)

        jwt = response.get("WebIdentityToken")
        if not isinstance(jwt, str) or not jwt:
            raise ValueError("STS response missing WebIdentityToken; cannot mint OAUTHBEARER token.")

        expiration = response.get("Expiration")
        if expiration is None:
            raise ValueError("STS response missing Expiration; cannot compute token lifetime.")
        # boto3 normalises the timestamp to a tz-aware UTC datetime;
        # .timestamp() returns epoch seconds as a float.
        expiry_epoch_seconds = expiration.timestamp()

        principal = (
            self._cfg.principal_name
            if self._cfg.principal_name is not None
            else _aws_jwt_subject_extractor.extract_sub(jwt)
        )

        # Always return a dict for the extensions slot — the C oauth_cb
        # wrapper's PyArg_ParseTuple uses "O!" with PyDict_Type for that slot,
        # which would reject None. Empty dict is the Pythonic equivalent of
        # .NET's null-Extensions case.
        extensions = dict(self._cfg.sasl_extensions) if self._cfg.sasl_extensions else {}

        return jwt, expiry_epoch_seconds, principal, extensions
