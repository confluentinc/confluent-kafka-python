"""
Token helper for OAUTHBEARER integration tests.

Produces unsigned JWTs acceptable to Apache Kafka's
OAuthBearerUnsecuredValidatorCallbackHandler (KIP-255). The broker fixture
in this directory uses that validator, so tokens generated here are valid
credentials for it.

NEVER use this helper or the broker fixture in production. The unsecured
validator does not verify any signature.
"""

import base64
import json
import time
from typing import Tuple


def _b64url(data: bytes) -> str:
    """Base64URL-encode without padding (the encoding required for JWT
    compact serialization). Java's Base64.getUrlDecoder() accepts both
    padded and unpadded forms, so stripping '=' is safe."""
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def make_unsecured_jwt(
    subject: str = "alice",
    lifetime_sec: float = 300.0,
) -> Tuple[str, float]:
    """Build an unsecured JWT and return (token, exp_epoch_seconds).

    The token has the JWT compact form ``header.payload.`` (empty
    signature). The validator checks the ``sub`` claim exists and is a
    string, that ``exp`` is in the future (modulo clock skew), and that
    ``iat`` (if present) precedes ``exp``.

    :param subject: Value for the ``sub`` claim. Becomes the SASL
        principal name on the broker.
    :param lifetime_sec: Seconds from now until the token expires. The
        returned ``exp_epoch_seconds`` is what oauth_cb should hand back
        to librdkafka so it can schedule the next refresh.
    """
    now = time.time()
    exp = now + lifetime_sec

    header = {"alg": "none", "typ": "JWT"}
    payload = {
        "sub": subject,
        "iat": int(now),
        "exp": int(exp),
    }

    token = (
        _b64url(json.dumps(header, separators=(",", ":")).encode("utf-8"))
        + "."
        + _b64url(json.dumps(payload, separators=(",", ":")).encode("utf-8"))
        + "."
    )
    return token, exp
