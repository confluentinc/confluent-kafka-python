"""
Token helper for OAUTHBEARER integration tests.

Produces unsigned JWTs that Apache Kafka's
OAuthBearerUnsecuredValidatorCallbackHandler (KIP-255) accepts. Used by
the share_consumer_oauth/ tests against the trivup OAUTHBEARER fixture.

Defaults use sub="admin" and scope="requiredScope" because trivup's
unsecured listener JAAS fixes those values:
    unsecuredLoginStringClaim_sub="admin"
    unsecuredValidatorRequiredScope="requiredScope"
Override either if a test wants a token the broker will reject.

Do not use this helper or the broker fixture in production; the unsecured
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
    subject: str = "admin",
    lifetime_sec: float = 300.0,
    scope: str = "requiredScope",
) -> Tuple[str, float]:
    """Build an unsecured JWT and return (token, exp_epoch_seconds).

    The token has the JWT compact form "header.payload." (empty
    signature). The validator checks that the sub claim exists and is a
    string, that exp is in the future (allowing for clock skew), and
    that iat (if present) is before exp. If
    unsecuredValidatorRequiredScope is configured on the listener, the
    validator also checks that the JWT's scope claim contains the
    required value.

    :param subject: Value for the sub claim. Becomes the SASL principal
        name on the broker. Default matches trivup's
        unsecuredLoginStringClaim_sub="admin".
    :param lifetime_sec: Seconds from now until the token expires. The
        returned exp_epoch_seconds is what oauth_cb returns to librdkafka
        so it can schedule the next refresh.
    :param scope: Value for the scope claim. Default matches trivup's
        unsecuredValidatorRequiredScope="requiredScope".
    """
    now = time.time()
    exp = now + lifetime_sec

    header = {"alg": "none", "typ": "JWT"}
    payload = {
        "sub": subject,
        "scope": scope,
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
