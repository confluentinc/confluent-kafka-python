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

"""Public entry-point for AWS IAM OAUTHBEARER autowire.

Mirrors .NET's ``Confluent.Kafka.OAuthBearer.Aws.AwsAutoWire``. This is the
**only publicly importable name** in the optional subpackage. The C
dispatcher in ``src/confluent_kafka/src/confluent_kafka.c`` (Phase 5) reaches
this module via::

    PyImport_ImportModule("confluent_kafka.oauthbearer.aws.aws_autowire")

and resolves :func:`create_handler` by name. The function signature is a
**frozen cross-module contract**:

* arity:   2 positional parameters
* names:   ``sasl_oauthbearer_config``, ``sasl_oauthbearer_extensions``
* types:   ``str``, ``Optional[str]``
* return:  :data:`OAuthBearerCallback`

Bumping any of these is a breaking change requiring a major version
increment on the ``confluent-kafka`` distribution. The frozen contract is
test-guarded by ``tests/oauthbearer/aws/test_contract.py``.

The marker key/value check is performed in core (the C dispatcher);
:func:`create_handler` is invoked only when the caller has already decided
to autowire the AWS path. The function therefore unconditionally attempts
to build a handler and raises on any input it cannot parse.
"""

from typing import Callable, Dict, Optional, Tuple

from . import _aws_sasl_extensions_parser
from ._aws_iam_marker import AWS_IAM_MARKER_KEY, AWS_IAM_MARKER_VALUE
from ._aws_oauthbearer_config import CONFIG_KEY, AwsOAuthBearerConfig
from ._aws_sts_token_provider import AwsStsTokenProvider

__all__ = ["create_handler", "OAuthBearerCallback"]


#: Type alias for the callable returned by :func:`create_handler`.
#:
#: Tuple shape matches the existing ``oauth_cb`` contract enforced in C at
#: ``confluent_kafka.c`` around L2291 via
#: ``PyArg_ParseTuple(result, "sd|sO!", ...)`` — single ``str`` argument
#: (the ``sasl.oauthbearer.config`` value librdkafka passes back on every
#: refresh), returning ``(token_str, expiry_epoch_seconds, principal_str,
#: extensions_dict)``.
OAuthBearerCallback = Callable[[str], Tuple[str, float, str, Dict[str, str]]]


def create_handler(
    sasl_oauthbearer_config: str,
    sasl_oauthbearer_extensions: Optional[str],
) -> OAuthBearerCallback:
    """Build an OAUTHBEARER refresh callback from the two OAUTHBEARER config strings.

    Construction-time work:

    1. Validates ``sasl_oauthbearer_config`` is a non-empty string.
    2. Parses ``sasl_oauthbearer_extensions`` (comma-separated ``key=value``)
       via :mod:`._aws_sasl_extensions_parser` into an optional dict.
    3. Parses ``sasl_oauthbearer_config`` (whitespace-separated ``key=value``)
       into a validated :class:`._aws_oauthbearer_config.AwsOAuthBearerConfig`.
    4. Constructs an :class:`._aws_sts_token_provider.AwsStsTokenProvider`
       (no HTTP yet — credential resolution is lazy until the first
       ``token()`` invocation).
    5. Returns the provider's bound :meth:`token` method as the callable.

    The returned callable is invoked by the C ``oauth_cb`` wrapper on every
    OAUTHBEARER refresh; each call performs one STS ``GetWebIdentityToken``
    round-trip and returns a fresh 4-tuple suitable for
    ``rd_kafka_oauthbearer_set_token``.

    :param sasl_oauthbearer_config: The verbatim ``sasl.oauthbearer.config``
        value (whitespace-separated ``key=value`` pairs). Must be non-empty.
    :param sasl_oauthbearer_extensions: The verbatim
        ``sasl.oauthbearer.extensions`` value (comma-separated ``key=value``
        pairs, RFC 7628 §3.1). May be ``None`` or empty when the user has
        no extensions configured.

    :returns: A callable matching :data:`OAuthBearerCallback`.

    :raises ValueError: ``sasl_oauthbearer_config`` is ``None`` or empty;
        the wire-grammar parse fails (unknown key, malformed token, missing
        required field, range/enum violation, etc.).
    :raises RuntimeError: AWS SDK reachability or initialisation failure
        (e.g. unknown region, malformed ``sts_endpoint``).
    """
    if not sasl_oauthbearer_config:
        raise ValueError(
            f"'{AWS_IAM_MARKER_KEY}={AWS_IAM_MARKER_VALUE}' is set but "
            f"'{CONFIG_KEY}' is missing or empty. The AWS IAM autowire path "
            f"requires region and audience to be supplied via "
            f"{CONFIG_KEY} (e.g. \"region=us-east-1 audience=https://...\")."
        )

    sasl_extensions = _aws_sasl_extensions_parser.parse(sasl_oauthbearer_extensions)
    config = AwsOAuthBearerConfig.parse(sasl_oauthbearer_config, sasl_extensions)
    provider = AwsStsTokenProvider(config)
    return provider.token
