# Design: AWS STS `GetWebIdentityToken` OAUTHBEARER Provider — Python

**Status:** Draft — pending naming decision and first implementation
**Owner:** prashah@confluent.io
**Last updated:** 2026-04-21

## 1. Context

AWS shipped **IAM Outbound Identity Federation** (GA 2025-11-19), exposing a new STS API, `GetWebIdentityToken`. AWS principals mint short-lived OIDC JWTs that external OIDC-compatible services (e.g. Confluent Cloud) can verify against an AWS-hosted JWKS. AWS acts as the OIDC IdP; the external service is the relying party.

Confluent customers running Kafka clients on AWS (EC2, EKS, ECS/Fargate, Lambda) want to authenticate to OIDC-gated Kafka endpoints using their AWS identity, without long-lived secrets.

A parallel effort in librdkafka implements this in C (reachable from every Confluent Kafka client). This document describes a **complementary, Python-managed integration** that lets users ship today without waiting for or upgrading the bundled librdkafka, and that leverages boto3's mature credential chain (env / IMDSv2 / ECS / EKS IRSA / EKS Pod Identity / SSO / profile).

The Go equivalent is documented in `DESIGN_AWS_OAUTHBEARER.md` at the `confluent-kafka-go` repo root; the .NET equivalent is at `confluent-kafka-dotnet`. This doc mirrors them; API shapes are aligned where idiomatic for Python.

## 2. Decision

Add an **optional integration gated by a new PyPI extra** — not a separate distribution:

```
confluent-kafka[oauthbearer-aws]
  │
  ├── new code at:  src/confluent_kafka/oauthbearer/aws/
  └── new deps in:  requirements/requirements-oauthbearer-aws.txt  (boto3>=1.42.25)
```

Users opt in with `pip install 'confluent-kafka[oauthbearer-aws]'`. The code lives inside the main `confluent-kafka` wheel but is never imported by `confluent_kafka/__init__.py`, so:

- Users who don't install the extra get **zero additional transitive deps** — `boto3`/`botocore` stay out of their lockfile and SBOM.
- Users who install the extra but never `import confluent_kafka.oauthbearer.aws` also pay no runtime cost.
- Users who do both get a one-import, one-callback integration.

This mirrors the pattern already established in this repo by every existing optional integration — `schemaregistry`, `avro`, `json`, `protobuf`, `rules`. See §3 for why Python uses extras where Go/.NET use separate publishable units.

## 3. Rejected alternatives

### 3a. Separate PyPI distribution (`confluent-kafka-oauthbearer-aws`)

Mirrors the Go/.NET submodule/NuGet pattern literally. Would require a second release pipeline and a second GitHub repo (or at least a second packaging root), with no added dep-graph isolation benefit over PEP 621 extras: pip resolves `[oauthbearer-aws]` identically to a separate `require` in a `*-aws` distribution. Python's extras mechanism is the idiomatic analogue to Go submodules here — the repo has been using it for five optional integrations already. Adopting separate distributions for this one would be inconsistent and double the release surface.

### 3b. Fold AWS support into the existing `rules` extra

The `rules` extra already pulls `boto3>=1.35`, so adding AWS STS code under `src/confluent_kafka/schema_registry/rules/encryption/awskms/` (next to the existing KMS driver) would need no new extra.

**Why rejected:** `rules` is a monolith that bundles every cloud KMS SDK (AWS + Azure + GCP + Vault + tink + cel-python + ~10 other packages). A user who wants only AWS OAuth authentication would drag in all four cloud SDKs. Adding a per-cloud `oauthbearer-aws` extra keeps the dep graph minimal. The `rules` shape is legacy, not a model to emulate.

### 3c. librdkafka-native path only (`sasl.oauthbearer.method=aws_msk_iam` equivalent)

Relies on the parallel librdkafka C-layer work landing and users upgrading the bundled librdkafka. Users stuck on pinned versions, or who want boto3's role-chaining / SSO / profile support, get nothing. Both paths should coexist — this doc covers the managed one.

### 3d. Guard imports with `try: import boto3 except ImportError`

Would let the module live under the main package even when the extra isn't installed, deferring failure to first use. **Why rejected:** violates the established repo convention. Every existing optional integration in this repo — [awskms/aws_driver.py:17-21](src/confluent_kafka/schema_registry/rules/encryption/awskms/aws_driver.py#L17-L21), [common/avro.py:10-11](src/confluent_kafka/schema_registry/common/avro.py#L10-L11), [common/protobuf.py:8](src/confluent_kafka/schema_registry/common/protobuf.py#L8) — uses unconditional top-level imports. The failure mode "ImportError when you try to `from confluent_kafka.oauthbearer.aws import …` without the extra" is loud, early, and consistent with the rest of the codebase. Silent-fallback imports would be the anomaly.

## 4. Verification performed (2026-04-21)

### 4a. `GetWebIdentityToken` is available in boto3 / botocore

- **botocore** first shipped `get_web_identity_token` in **1.42.25** (released 2026-01-09).
- **boto3** picked up the matching botocore in the same-day release of **boto3 1.42.25** (released 2026-01-12).
- Current latest as of today: **boto3 1.42.92 / botocore 1.42.91**.

boto3 service method shape confirmed at [boto3 docs](https://docs.aws.amazon.com/boto3/latest/reference/services/sts/client/get_web_identity_token.html):

```python
response = sts_client.get_web_identity_token(
    Audience=['https://confluent.cloud/oidc'],
    SigningAlgorithm='ES384',        # or 'RS256'
    DurationSeconds=3600,             # 60–3600, optional, default 300
    Tags=[{'Key': '...', 'Value': '...'}],  # optional
)
# response == {
#     'WebIdentityToken': str,       # the JWT
#     'Expiration': datetime.datetime  # timezone-aware, UTC
# }
```

**Target for this extra:** `boto3>=1.42.25` (pin to the first release that shipped the method; botocore floor is inherited transitively). This is looser than the existing `rules` extra pin (`boto3>=1.35`) because that one predates outbound federation. Users who already installed `rules` and want `oauthbearer-aws` on top will upgrade boto3 to the higher floor — fine, boto3 is backward-compatible within major versions.

### 4b. OAUTHBEARER hook in the Python client

The `oauth_cb` config parameter on `Producer` / `Consumer` / `AdminClient` is the refresh hook. Implementation is in the C extension at [src/confluent_kafka/src/confluent_kafka.c:2268-2352](src/confluent_kafka/src/confluent_kafka.c#L2268-L2352):

- **Input to callback:** the `sasl.oauthbearer.config` string (line 2284 — `PyObject_CallFunctionObjArgs(h->oauth_cb, eo, NULL)`).
- **Return contract:** `(token_str, expiry_time_float)` required, `(token_str, expiry_time_float, principal_str, extensions_dict)` optional. Parsed at line 2291-2292 via `PyArg_ParseTuple(result, "sd|sO!", …)`.
- **Expiry semantics:** `expiry_time_float` is absolute epoch seconds; the C layer converts via `(int64_t)(expiry * 1000)` to the milliseconds librdkafka expects (line 2318).
- **Success path:** calls `rd_kafka_oauthbearer_set_token(...)` at line 2317.
- **Failure path:** on raised exception or bad return shape, calls `rd_kafka_oauthbearer_set_token_failure(...)` at line 2338.
- **Threading:** the callback runs on librdkafka's background thread (comment at line 2264-2266). No `asyncio` event loop is involved — boto3's sync API is a natural fit.

Existing example at [examples/oauth_producer.py:34-55](examples/oauth_producer.py#L34-L55) demonstrates the pattern with `functools.partial(_get_token, args)` returning `(token, time.time() + expires_in)`. Our provider will slot into the same spot.

## 5. Architecture

### 5a. Directory layout

```
confluent-kafka-python/
├── pyproject.toml                              ← +1 extra entry
├── requirements/
│   └── requirements-oauthbearer-aws.txt         ← NEW (boto3>=1.42.25)
├── src/
│   └── confluent_kafka/
│       ├── __init__.py                         ← unchanged (does not import oauthbearer)
│       └── oauthbearer/                        ← NEW
│           ├── __init__.py                     ← empty; namespace only
│           └── aws/                            ← NEW
│               ├── __init__.py                 ← re-exports public names
│               ├── provider.py                 ← AwsOAuthConfig, AwsStsTokenProvider
│               ├── _jwt.py                     ← ~20 LoC base64url sub extractor
│               └── py.typed                    ← PEP 561 marker (follow package convention)
├── tests/
│   └── unit/
│       └── oauthbearer/
│           └── aws/                            ← NEW
│               ├── __init__.py
│               ├── test_provider.py             ← mock boto3 STS client
│               └── test_jwt.py
└── examples/
    └── oauth_aws_producer.py                   ← NEW, optional
```

Test discovery: [tox.ini](tox.ini) / `pytest` already auto-discover anything under `tests/unit/`. No new CI wiring needed beyond adding the directory.

### 5b. pyproject.toml changes

Two additions to [pyproject.toml](pyproject.toml), both inside `[tool.setuptools.dynamic]`:

```toml
optional-dependencies.oauthbearer-aws = { file = ["requirements/requirements-oauthbearer-aws.txt"] }

# and folded into the aggregate extras:
optional-dependencies.all = { file = [
    "requirements/requirements-soaktest.txt",
    "requirements/requirements-docs.txt",
    "requirements/requirements-examples.txt",
    "requirements/requirements-tests.txt",
    "requirements/requirements-schemaregistry.txt",
    "requirements/requirements-rules.txt",
    "requirements/requirements-avro.txt",
    "requirements/requirements-json.txt",
    "requirements/requirements-protobuf.txt",
    "requirements/requirements-oauthbearer-aws.txt",   # NEW
] }
```

Also add to `dev` and `tests` aggregates so CI covers it.

[requirements/requirements-oauthbearer-aws.txt](requirements/requirements-oauthbearer-aws.txt):

```
boto3>=1.42.25
```

That's it — one line. `botocore` is a transitive dep of `boto3` with tight version coupling, no need to pin separately.

### 5c. Public API surface

```python
# src/confluent_kafka/oauthbearer/aws/provider.py
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

import boto3

__all__ = ['AwsOAuthConfig', 'AwsStsTokenProvider']


@dataclass
class AwsOAuthConfig:
    """Parameters for sts:GetWebIdentityToken."""

    # Required. No silent default.
    region: str
    audience: str

    # "ES384" (default) or "RS256".
    signing_algorithm: str = "ES384"

    # Token lifetime requested from STS. 60–3600 seconds. Default 300.
    duration_seconds: int = 300

    # Optional STS endpoint override (FIPS, VPC endpoint).
    sts_endpoint_url: Optional[str] = None

    # Optional pre-built boto3 session. If None, boto3.Session() uses the
    # default credential chain (env → EKS IRSA → ECS → IMDSv2 → profile).
    session: Optional["boto3.Session"] = None

    # Optional SASL extensions communicated to the broker (RFC 7628 §3.1).
    # e.g. {"logicalCluster": "lkc-abc", "identityPoolId": "pool-xyz"}.
    sasl_extensions: Optional[Dict[str, str]] = None

    # Optional override for the principal name. When None, the provider
    # extracts the JWT "sub" claim (bare role ARN).
    principal_name_override: Optional[str] = None


class AwsStsTokenProvider:
    """Fetches OAUTHBEARER tokens via sts:GetWebIdentityToken.

    Thread-safe; share one instance across clients in a process.
    """

    def __init__(self, config: AwsOAuthConfig) -> None: ...

    def token(self, oauthbearer_config: str = "") -> Tuple[str, float, str, Dict[str, str]]:
        """Mint a fresh JWT. Return shape matches confluent_kafka oauth_cb contract:
        (token_str, expiry_time_epoch_seconds, principal, extensions).

        Raises on STS/config/parsing failure — the C layer converts raised
        exceptions into rd_kafka_oauthbearer_set_token_failure.
        """
        ...
```

Users wire it into a client with one line:

```python
provider = AwsStsTokenProvider(AwsOAuthConfig(region="us-east-1", audience="..."))
producer = Producer({
    "bootstrap.servers": "...",
    "sasl.mechanisms": "OAUTHBEARER",
    "oauth_cb": provider.token,   # bound method; signature matches the contract
})
```

The public surface is intentionally small — a dataclass, a provider class with one callable method. No builder, no registry, no `init()` side effects. Follows the shape of [examples/oauth_producer.py](examples/oauth_producer.py) rather than inventing a new pattern.

### 5d. `token()` internal flow

```python
def token(self, oauthbearer_config: str = "") -> Tuple[str, float, str, Dict[str, str]]:
    resp = self._sts.get_web_identity_token(
        Audience=[self._cfg.audience],
        SigningAlgorithm=self._cfg.signing_algorithm,
        DurationSeconds=self._cfg.duration_seconds,
    )

    jwt_str: str = resp["WebIdentityToken"]
    expiration: "datetime.datetime" = resp["Expiration"]  # tz-aware UTC
    expiry_epoch_seconds = expiration.timestamp()

    principal = self._cfg.principal_name_override or _jwt.extract_sub(jwt_str)
    extensions = self._cfg.sasl_extensions or {}

    return jwt_str, expiry_epoch_seconds, principal, extensions
```

All SigV4 signing, credential resolution (IMDSv2 token hop, EKS IRSA file rotation, ECS agent handshake, SSO refresh), retries, clock-skew correction, and JSON/XML parsing are delegated to botocore. This is the main argument for going managed instead of native: thousands of engineering hours of AWS-SDK work reused for free.

### 5e. JWT `sub` extraction

Live STS response (librdkafka memory, §"Probe B"): `sub` is the bare role ARN like `arn:aws:iam::708975691912:role/ktrue-iam-sts-test-role`. Extraction is ~20 LoC in `_jwt.py`: split on `.`, pad the base64url segment to a multiple of 4, decode, `json.loads`, read `sub`. No JWT library dep — we are not validating the signature (AWS minted it; trust is upstream).

### 5f. Threading and the sync/async question

The `oauth_cb` delegate is synchronous and invoked on librdkafka's background thread (verified at [confluent_kafka.c:2264-2266](src/confluent_kafka/src/confluent_kafka.c#L2264-L2266)). boto3's `get_web_identity_token` is synchronous. No bridging needed — the call just blocks the background thread for one HTTP round-trip, then returns.

**Do not** provide an async variant using `aioboto3` or `asyncio`. The callback is not awaited — any coroutine returned would be silently dropped. If demand surfaces for an async path (e.g. for the `confluent_kafka.aio.*` clients), evaluate separately; aio clients still ultimately call into the same C callback infrastructure.

## 6. User-side integration pattern

```python
# Install:    pip install 'confluent-kafka[oauthbearer-aws]'

from confluent_kafka import Consumer
from confluent_kafka.oauthbearer.aws import AwsOAuthConfig, AwsStsTokenProvider

provider = AwsStsTokenProvider(AwsOAuthConfig(
    region="us-east-1",
    audience="https://confluent.cloud/oidc",
    duration_seconds=3600,
    sasl_extensions={
        "logicalCluster": "lkc-abc123",
        "identityPoolId": "pool-xyz",
    },
))

consumer = Consumer({
    "bootstrap.servers": "pkc-xxxx.us-east-1.aws.confluent.cloud:9092",
    "security.protocol": "SASL_SSL",
    "sasl.mechanisms": "OAUTHBEARER",
    "group.id": "my-group",
    "auto.offset.reset": "earliest",
    "oauth_cb": provider.token,
    # NOTE: do NOT set sasl.oauthbearer.method — that selects the librdkafka-native path.
})

consumer.subscribe(["my-topic"])
while True:
    msg = consumer.poll(1.0)
    # ...
```

No timers, no background threads, no manual polling for tokens. librdkafka emits the refresh event; our callback services it; token lifetime drives the next refresh.

## 7. Local development

No `go.work` analogue needed. Development is the standard editable install:

```bash
pip install -e '.[oauthbearer-aws,tests]'
pytest tests/unit/oauthbearer/aws/
```

The existing [tox.ini](tox.ini) config picks up the new test directory automatically. `make test` and the Semaphore CI pipeline require no changes beyond ensuring one of the matrix envs installs the `oauthbearer-aws` extra (simplest: add it to the `tests` aggregate extra so every CI job already has boto3).

## 8. Release and versioning policy

**Policy:** lockstep with the main package — inherent, because we ship one wheel.

- Single `version = "2.14.0"` in [pyproject.toml](pyproject.toml#L7). Bumping it bumps everything.
- Users matching `confluent-kafka 2.15.0` automatically get the matching `oauthbearer.aws` subpackage.
- No compatibility matrix. Same-minor always works.
- Cross-version concern between boto3 and `confluent_kafka.oauthbearer.aws`: only use the stable public methods of `boto3.Session` / `boto3.client('sts').get_web_identity_token(...)`. Both are covered by boto3's backward-compat guarantees within major versions. Pin floor; let upper bound float.

## 9. Testing strategy

Three layers, all local:

1. **Unit — token provider** (`test_provider.py`)
   Use `unittest.mock.patch` on `boto3.client` (or inject a pre-built mock STS client via `AwsOAuthConfig.session`). Return canned `get_web_identity_token` response dicts. Assert we:
   - pass `Audience`, `SigningAlgorithm`, `DurationSeconds` through correctly (region/endpoint flow into the session/client config);
   - surface `botocore.exceptions.ClientError` as-is (let the C layer turn it into `set_token_failure`);
   - map `Expiration` (tz-aware UTC `datetime`) → `float` epoch seconds via `.timestamp()`;
   - use `principal_name_override` when set; fall back to JWT `sub` otherwise;
   - round-trip `sasl_extensions` unchanged.

2. **Unit — JWT subject extractor** (`test_jwt.py`)
   Fixtures for bare role ARN, assumed-role ARN, missing `sub`, malformed base64url (with and without padding), three-segment vs two-segment tokens, oversized payloads. No JWT library.

3. **Integration (opt-in)** — `test_provider_real.py` gated by `RUN_AWS_STS_REAL=1` env var (skip via `pytest.mark.skipif`). Runs against real STS on the existing EC2 test box (`eu-north-1`, role `ktrue-iam-sts-test-role`) used by the librdkafka probe work. Off by default in CI; run manually or on a scheduled pipeline.

CI wiring: Semaphore already runs `pytest tests/unit/`. Just ensure the `tests` aggregate extra includes `oauthbearer-aws` so boto3 is installed in the CI env.

## 10. Operational notes

- **Region must be explicit.** No silent default, no IMDS sniffing. Misconfigured region → `ValueError` from `AwsStsTokenProvider.__init__`. Matches the librdkafka-side and Go/.NET decisions.
- **`duration_seconds` bounds:** 60–3600 (AWS-enforced). Default 300. Validate at construction.
- **Enablement prerequisite:** the AWS account must have run `aws iam enable-outbound-web-identity-federation` once. First `get_web_identity_token` on a non-enabled account returns `OutboundWebIdentityFederationDisabledException` — surface the `botocore.exceptions.ClientError` message verbatim via the raised exception (the C layer logs it to `set_token_failure`).
- **FIPS / VPC endpoints:** supported via `AwsOAuthConfig.sts_endpoint_url` → passed to `boto3.client('sts', endpoint_url=…)`.
- **Lambda:** the default credential chain reads `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` / `AWS_SESSION_TOKEN` that Lambda injects. Lambda role needs `sts:GetWebIdentityToken` permission; VPC-bound Lambdas need egress to the STS regional endpoint. Operational, not code.
- **EKS IRSA:** boto3 resolves `AWS_WEB_IDENTITY_TOKEN_FILE` + `AWS_ROLE_ARN` automatically via its credential provider chain. botocore handles token-file rotation (re-read per credential refresh).
- **Credential caching:** boto3 caches credentials via its internal resolver; we do not cache at the provider level. librdkafka drives the next token refresh via expiration — extra caching here would be redundant and could mask expiration.
- **GIL / threading:** `provider.token` is called from librdkafka's background thread with the GIL acquired (see `PyGILState_Ensure` at [confluent_kafka.c:2282](src/confluent_kafka/src/confluent_kafka.c#L2282)). boto3's sync calls release the GIL during the HTTP round-trip, so other Python threads are unblocked.

## 11. Open items

1. **Package naming.** Proposal `confluent_kafka.oauthbearer.aws` (subpackage path) + `oauthbearer-aws` (extra name). Alternatives: `confluent_kafka.auth.aws`, `confluent_kafka.oidc.aws`. Locks at first publish. Preferring `oauthbearer.aws` because it matches the existing `oauth_cb` config key and leaves room for sibling `confluent_kafka.oauthbearer.azure` / `.gcp`.

2. **`boto3` floor.** Proposal `>=1.42.25` (first release with `get_web_identity_token`). Cross-check with downstream consumers who may already pin older boto3 for the `rules` extra — tight floor is safer than loose, but confirm we're not wedging someone on boto3 1.35.

3. **Should the provider accept a pre-built `boto3.client('sts')` directly**, in addition to a `Session`? Would make mocking trivial in user tests. Small API addition; decide after first user feedback.

4. **Eager vs lazy credential resolution.** Aligning with Go/.NET decisions: lean eager. Call `boto3.Session.get_credentials()` in `__init__` and raise synchronously on failure, so misconfiguration surfaces at construction rather than on first broker contact.

5. **`confluent_kafka.aio` integration.** The aio client path still bottoms out in the same C callback infrastructure, so the sync `provider.token` method works there too. Confirm during implementation; document clearly. Do not build an `AsyncAwsStsTokenProvider` until demand is real — premature abstraction.

6. **CHANGELOG + README + INSTALL.md integration.** One line each at first release. Add `oauthbearer-aws` to the extras list in [INSTALL.md](INSTALL.md) (extras are not currently documented there — worth a short section) and the main [README.md](README.md).

7. **Ducktape / soaktest coverage.** The existing `tests/ducktape/` and `tests/soak/` suites don't cover OAUTHBEARER today. Skip for v1; add only if a gap is reported.

## 12. References

- AWS IAM Outbound Identity Federation announcement (2025-11-19).
- boto3 `get_web_identity_token` docs: [link](https://docs.aws.amazon.com/boto3/latest/reference/services/sts/client/get_web_identity_token.html).
- botocore 1.42.25 (2026-01-09) / boto3 1.42.25 (2026-01-12) — first releases with `GetWebIdentityToken`.
- Go-side design: `DESIGN_AWS_OAUTHBEARER.md` in `confluent-kafka-go` — complementary, API-aligned.
- .NET-side design: `DESIGN_AWS_OAUTHBEARER.md` in `confluent-kafka-dotnet` — complementary, API-aligned.
- librdkafka-native design: `DESIGN_AWS_OAUTHBEARER_V1.md` in `librdkafka` — identical wire protocol, shared probe results on EC2 `eu-north-1`.
- Existing optional-package template (extras + unconditional imports): [src/confluent_kafka/schema_registry/rules/encryption/awskms/](src/confluent_kafka/schema_registry/rules/encryption/awskms/), [src/confluent_kafka/schema_registry/avro.py](src/confluent_kafka/schema_registry/avro.py), [src/confluent_kafka/schema_registry/protobuf.py](src/confluent_kafka/schema_registry/protobuf.py).
- OAUTHBEARER C-extension hook: [src/confluent_kafka/src/confluent_kafka.c:2268-2352](src/confluent_kafka/src/confluent_kafka.c#L2268-L2352).
- Existing custom OAUTHBEARER example: [examples/oauth_producer.py:34-55](examples/oauth_producer.py#L34-L55).
- Python-side optional-deps pattern memory: `project_optional_deps_pattern_python.md` in this project's `~/.claude` memory dir.
