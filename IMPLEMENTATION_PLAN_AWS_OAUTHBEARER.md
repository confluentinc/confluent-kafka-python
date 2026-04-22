# Implementation Plan: AWS OAUTHBEARER Extra

**Companion document to** [DESIGN_AWS_OAUTHBEARER.md](DESIGN_AWS_OAUTHBEARER.md).
**Status:** Draft — ready to execute. No code written yet.
**Last updated:** 2026-04-22.
**Branch:** `dev_prashah_IAM_Python_POC`.

This document describes the execution path. For design rationale, public API shape, and rejected alternatives, see the design doc.

## Locked decisions

| Decision | Value |
|---|---|
| Distribution model | **Single PyPI distribution + PEP 621 extra**. No separate package. Matches every existing optional integration in this repo (`schemaregistry`, `avro`, `json`, `protobuf`, `rules`). |
| Extra name | `oauthbearer-aws` — selector: `pip install 'confluent-kafka[oauthbearer-aws]'` |
| Source subpackage | [src/confluent_kafka/oauthbearer/aws/](src/confluent_kafka/oauthbearer/aws/) |
| Requirements file | [requirements/requirements-oauthbearer-aws.txt](requirements/requirements-oauthbearer-aws.txt) |
| Test directory | [tests/unit/oauthbearer/aws/](tests/unit/oauthbearer/aws/) |
| Example | [examples/oauth_aws_producer.py](examples/oauth_aws_producer.py) |
| Python target | `>=3.8` (inherits from [pyproject.toml:17](pyproject.toml#L17)) |
| AWS SDK floor | `boto3>=1.42.25` — first PyPI release with `get_web_identity_token`, published 2026-01-12. Verified at M1 via pypi.org release history cross-check; recorded in [DESIGN_AWS_OAUTHBEARER.md](DESIGN_AWS_OAUTHBEARER.md) §4a. |
| Credential resolution | **Lazy** — construct `boto3.client('sts')` cheaply, resolve on first `provider.token()` call. Matches AWS SDK convention across .NET / Go / JS / Python. |
| Versioning | Lockstep with root [pyproject.toml:7](pyproject.toml#L7) `version` — **automatic**, because single distribution. No extra bookkeeping (unlike Go submodule tags or .NET `VersionPrefix`). |
| Mocking library | [botocore.stub.Stubber](https://botocore.amazonaws.com/v1/documentation/api/latest/reference/stubber.html) — standard, ships with `boto3`, zero new test-time deps. Fallback to `unittest.mock.patch` for cases Stubber can't cover. |
| Unconditional imports | `import boto3` at top of `provider.py` — **no `try/except ImportError` guards**. Matches repo convention (see non-negotiable gates). |
| Sync/async bridge | **Not needed.** `oauth_cb` is sync (C extension at [src/confluent_kafka/src/confluent_kafka.c:2268-2352](src/confluent_kafka/src/confluent_kafka.c#L2268-L2352)); `boto3.client('sts').get_web_identity_token` is sync. Direct call. |
| Release target | Deferred — picked at M8. Likely `2.15.0` cut after implementation merges. |

## Critical path

```
M1 ──┬─→ M3 ──→ M4 ──→ M5 ──┬─→ M7 ──→ M8
     │                      │
     └─→ M2 (parallel)      └─→ M6 (parallel)
```

**Total effort:** ~4–6 engineer-days. Deliverable in one working week solo, or split between a pair as M1+M2 and M3–M5.

## Non-negotiable gates enforced at every step

1. **Zero-cost-for-non-opt-in preserved.** After every PR, run the dep-graph check (M1 and post-implementation validation) against a clean `pip install confluent-kafka` consumer:
   ```bash
   python -m pip install confluent-kafka
   python -m pip show boto3 >/dev/null 2>&1 && echo LEAKED || echo OK
   python -c "import boto3" 2>&1 | grep -q ModuleNotFoundError && echo OK || echo LEAKED
   ```
   Both must print `OK`. If either prints `LEAKED`, the packaging has regressed — roll the PR back. The symmetric check with `pip install 'confluent-kafka[oauthbearer-aws]'` must print `LEAKED` twice (boto3 present). Asymmetry is the invariant.

2. **No new entry in the core `dependencies` list.** The new AWS SDK dep lives only in [requirements/requirements-oauthbearer-aws.txt](requirements/requirements-oauthbearer-aws.txt), routed through `[tool.setuptools.dynamic] optional-dependencies.oauthbearer-aws`. Touching [requirements/requirements.txt](requirements/requirements.txt) (core deps) invalidates the whole pattern.

3. **No `try: import boto3 except ImportError` guards anywhere in `src/confluent_kafka/oauthbearer/aws/`.** Repo convention: every optional integration uses unconditional top-level imports — [awskms/aws_driver.py:17-21](src/confluent_kafka/schema_registry/rules/encryption/awskms/aws_driver.py#L17-L21), [common/avro.py:10-11](src/confluent_kafka/schema_registry/common/avro.py#L10-L11), [common/protobuf.py:8](src/confluent_kafka/schema_registry/common/protobuf.py#L8). A missing extra must fail at `import` time (`ImportError`), not silently at runtime.

4. **`confluent_kafka/__init__.py` must not import anything under `oauthbearer/`.** Importing `confluent_kafka` with only the base install must never trigger `boto3` import. Verified in M1's dep-graph gate by running `python -c "import confluent_kafka; print(confluent_kafka.version())"` on a clean install and confirming exit code 0 without `boto3` being resolvable.

5. **Lockstep versioning.** Automatic via single distribution — the [pyproject.toml](pyproject.toml) `version` field governs everything. No manual coupling required, but the release rehearsal in M8 verifies the new subpackage ships correctly in the wheel.

6. **Do NOT set `sasl.oauthbearer.method`** anywhere in the new package's code, examples, or docs. That selects a librdkafka-native path and bypasses `oauth_cb`. Universal rule across all four language siblings — calling it out in the README is mandatory (M6).

---

## M1 — Scaffolding, extras wiring, and dep-graph gate

**Estimated effort:** 3–4 hours
**Parallelizable with:** M2
**Blocks:** M3, M4, M5

### Deliverables

- New directory [src/confluent_kafka/oauthbearer/](src/confluent_kafka/oauthbearer/) with:
  - `__init__.py` — empty (namespace marker only; no re-exports at this level to avoid triggering `aws/` imports).
  - [src/confluent_kafka/oauthbearer/aws/](src/confluent_kafka/oauthbearer/aws/) subdirectory with placeholder files (compile-only, no logic):
    - `__init__.py` — will re-export `AwsOAuthConfig` and `AwsStsTokenProvider` at M5; for now, `from .provider import *  # noqa: F401, F403` with `__all__ = []`.
    - `provider.py` — `import boto3` at top (unconditional), empty `AwsOAuthConfig` dataclass stub, empty `AwsStsTokenProvider` class stub.
    - `_jwt.py` — empty `extract_sub` function stub.
    - `py.typed` — zero-byte file, PEP 561 marker. Matches [src/confluent_kafka/py.typed](src/confluent_kafka/py.typed).

- [requirements/requirements-oauthbearer-aws.txt](requirements/requirements-oauthbearer-aws.txt):
  ```
  boto3>=1.42.25
  ```

- [pyproject.toml](pyproject.toml) edits to `[tool.setuptools.dynamic]`:
  ```toml
  optional-dependencies.oauthbearer-aws = { file = ["requirements/requirements-oauthbearer-aws.txt"] }

  optional-dependencies.dev = { file = [
      # ... existing entries ...
      "requirements/requirements-oauthbearer-aws.txt",   # NEW
  ] }

  optional-dependencies.tests = { file = [
      # ... existing entries ...
      "requirements/requirements-oauthbearer-aws.txt",   # NEW
  ] }

  optional-dependencies.all = { file = [
      # ... existing entries ...
      "requirements/requirements-oauthbearer-aws.txt",   # NEW
  ] }
  ```

- [pyproject.toml](pyproject.toml) edit to `[[tool.mypy.overrides]]`:
  ```toml
  [[tool.mypy.overrides]]
  module = ["confluent_kafka.oauthbearer.aws.*"]
  disable_error_code = ["assignment", "no-redef"]
  ```
  Only add this if mypy complains during initial scaffolding — don't pre-silence errors we haven't seen.

- Test scaffold at [tests/unit/oauthbearer/aws/](tests/unit/oauthbearer/aws/):
  - `__init__.py` — empty.
  - `test_scaffold.py`:
    ```python
    def test_placeholder():
        """Placeholder test so pytest discovers the directory."""
        import confluent_kafka.oauthbearer.aws  # noqa: F401
    ```

### Dep-graph invariant proof (new, required)

Two scratch virtualenvs, one opt-in, one opt-out. Record outputs in [DESIGN_AWS_OAUTHBEARER.md](DESIGN_AWS_OAUTHBEARER.md) §4 as new subsection `4c`, dated.

```bash
# Opt-OUT venv: base install only
python -m venv /tmp/ckpy-depcheck-out
source /tmp/ckpy-depcheck-out/bin/activate
pip install /path/to/confluent-kafka-python
pip show boto3 >/dev/null 2>&1 && echo LEAKED || echo OK       # must print OK
python -c "import boto3" 2>&1 | grep -q ModuleNotFoundError && echo OK || echo LEAKED  # must print OK
python -c "import confluent_kafka; print(confluent_kafka.version())"  # must succeed, prints (2, 14, 0, ...)
deactivate

# Opt-IN venv: install with extra
python -m venv /tmp/ckpy-depcheck-in
source /tmp/ckpy-depcheck-in/bin/activate
pip install '/path/to/confluent-kafka-python[oauthbearer-aws]'
pip show boto3 >/dev/null 2>&1 && echo LEAKED || echo OK       # must print LEAKED (boto3 present)
python -c "from confluent_kafka.oauthbearer.aws import AwsStsTokenProvider; print('ok')"
deactivate
```

### Exit criteria

- `pip install -e '.[oauthbearer-aws,tests]'` from repo root succeeds cleanly.
- `pytest tests/unit/oauthbearer/aws/ -v` runs (1 passing placeholder test).
- `python -c "import confluent_kafka; print(confluent_kafka.version())"` works on base install (no boto3 resolvable).
- `python -c "from confluent_kafka.oauthbearer.aws import *"` fails with `ImportError` on base install (without extra), succeeds with extra installed.
- Dep-graph invariant proof recorded in design doc §4c.
- `tox -e flake8,black,isort` clean for the new files.

---

## M2 — JWT `sub` extractor

**Estimated effort:** 2–3 hours
**Parallelizable with:** M1 (fully independent — zero deps beyond stdlib)
**Blocks:** M4

### Deliverables

- [src/confluent_kafka/oauthbearer/aws/_jwt.py](src/confluent_kafka/oauthbearer/aws/_jwt.py):
  ```python
  import base64
  import json

  _MAX_JWT_BYTES = 8 * 1024  # 8 KB ceiling; live AWS tokens are ~1.4 KB


  def extract_sub(jwt: str) -> str:
      """Extract the 'sub' claim from an unverified JWT payload.

      AWS already signed this token; verification is not our job — we just
      need the claim for the principal name passed to rd_kafka_oauthbearer_set_token.
      """
      if not jwt or len(jwt.encode('utf-8')) > _MAX_JWT_BYTES:
          raise ValueError("JWT is empty or exceeds size ceiling")
      parts = jwt.split('.')
      if len(parts) != 3:
          raise ValueError(f"JWT must have 3 dot-separated segments, got {len(parts)}")
      payload_b64 = parts[1]
      # base64url → base64, pad to multiple of 4
      padded = payload_b64 + '=' * (-len(payload_b64) % 4)
      try:
          decoded = base64.urlsafe_b64decode(padded.encode('ascii'))
          claims = json.loads(decoded)
      except (ValueError, json.JSONDecodeError) as exc:
          raise ValueError(f"JWT payload not valid base64url JSON: {exc}") from exc
      sub = claims.get('sub')
      if not isinstance(sub, str) or not sub:
          raise ValueError("JWT missing or empty 'sub' claim")
      return sub
  ```

- [tests/unit/oauthbearer/aws/test_jwt.py](tests/unit/oauthbearer/aws/test_jwt.py) covering:
  - Valid role ARN — `arn:aws:iam::123456789012:role/MyRole`.
  - Valid assumed-role ARN — different ARN shape.
  - Missing `sub` claim → `ValueError` containing `"sub"`.
  - Token with fewer than 3 dot-separated segments → `ValueError`.
  - Token with more than 3 segments → `ValueError`.
  - Malformed base64url in payload segment → `ValueError`.
  - Malformed JSON in decoded payload → `ValueError`.
  - Empty string input → `ValueError`.
  - Oversized input guard — construct an 8.1 KB string, assert `ValueError`. Rationale: live AWS tokens are ~1.4 KB per the cross-language validation (Go/.NET/JS all observed 1256 bytes); 8 KB is a generous ceiling that bounds attacker-controlled allocation.
  - Padded vs unpadded base64url — both decode correctly (AWS emits unpadded; spec allows either).
  - Non-string `sub` (JSON number) → `ValueError`.

### Exit criteria

- `pytest tests/unit/oauthbearer/aws/test_jwt.py -v` green, all 10+ cases pass.
- `extract_sub` is exported from `_jwt.py` but **not** re-exported from `aws/__init__.py` (internal helper; leading underscore signals intent).
- Zero new runtime `dependencies`; implementation uses only `base64` + `json` from stdlib.
- `tox -e flake8,black,mypy` clean for the new file.

---

## M3 — `AwsOAuthConfig` + validation

**Estimated effort:** 3–4 hours
**Depends on:** M1
**Blocks:** M4

### Deliverables

- [src/confluent_kafka/oauthbearer/aws/provider.py](src/confluent_kafka/oauthbearer/aws/provider.py) — `AwsOAuthConfig` dataclass per design §5c:
  ```python
  from dataclasses import dataclass, field
  from typing import Dict, Optional

  import boto3


  @dataclass
  class AwsOAuthConfig:
      region: str
      audience: str
      signing_algorithm: str = "ES384"
      duration_seconds: int = 300
      sts_endpoint_url: Optional[str] = None
      session: Optional[boto3.Session] = None
      sasl_extensions: Optional[Dict[str, str]] = None
      principal_name_override: Optional[str] = None

      def __post_init__(self) -> None:
          self._validate()

      def _validate(self) -> None:
          if not self.region or not isinstance(self.region, str):
              raise ValueError("region is required (non-empty string)")
          if not self.audience or not isinstance(self.audience, str):
              raise ValueError("audience is required (non-empty string)")
          if self.signing_algorithm not in ("ES384", "RS256"):
              raise ValueError(
                  f"signing_algorithm must be 'ES384' or 'RS256', got {self.signing_algorithm!r}"
              )
          if not (60 <= self.duration_seconds <= 3600):
              raise ValueError(
                  f"duration_seconds must be in [60, 3600], got {self.duration_seconds}"
              )
          if self.sts_endpoint_url is not None:
              if not self.sts_endpoint_url.startswith("https://"):
                  raise ValueError("sts_endpoint_url must start with 'https://'")
          if self.principal_name_override is not None and not self.principal_name_override:
              raise ValueError("principal_name_override, if set, must be non-empty")
  ```
  Validation runs in `__post_init__` — construction fails fast, synchronously, before any AWS SDK call.

### Tests (`tests/unit/oauthbearer/aws/test_config.py`)

- Validation failures:
  - Missing `region` → `TypeError` from dataclass (required positional).
  - Empty `region` → `ValueError` containing `"region"`.
  - Empty `audience` → `ValueError` containing `"audience"`.
  - `signing_algorithm="HS256"` → `ValueError`.
  - `duration_seconds=30` → `ValueError`.
  - `duration_seconds=7200` → `ValueError`.
  - `sts_endpoint_url="http://insecure.example.com"` → `ValueError`.
  - `principal_name_override=""` → `ValueError` (empty string distinct from None).
- Default values on valid config:
  - Unset `signing_algorithm` → `"ES384"`.
  - Unset `duration_seconds` → `300`.
  - Unset `sasl_extensions` → `None`.

### Exit criteria

- All tests pass.
- Validation throws synchronously; no network, no `boto3.client` instantiation in `AwsOAuthConfig`.
- `tox -e flake8,black,mypy` clean.

---

## M4 — `AwsStsTokenProvider` + STS mocking

**Estimated effort:** 1–1.5 engineer-days
**Depends on:** M2, M3
**Blocks:** M5, M7

### Deliverables

- [src/confluent_kafka/oauthbearer/aws/provider.py](src/confluent_kafka/oauthbearer/aws/provider.py) — `AwsStsTokenProvider` class:
  ```python
  from typing import Any, Dict, Tuple

  import boto3

  from . import _jwt


  class AwsStsTokenProvider:
      """Fetches OAUTHBEARER tokens via sts:GetWebIdentityToken.

      Thread-safe; share one instance across clients in a process.
      """

      def __init__(self, config: AwsOAuthConfig) -> None:
          self._cfg = config
          session = config.session or boto3.Session(region_name=config.region)
          client_kwargs: Dict[str, Any] = {"region_name": config.region}
          if config.sts_endpoint_url:
              client_kwargs["endpoint_url"] = config.sts_endpoint_url
          self._sts = session.client("sts", **client_kwargs)

      def token(self, oauthbearer_config: str = "") -> Tuple[str, float, str, Dict[str, str]]:
          """Mint a fresh JWT. Returns the 4-tuple shape the oauth_cb contract expects:
          (token_str, expiry_epoch_seconds_float, principal, extensions).
          """
          resp = self._sts.get_web_identity_token(
              Audience=[self._cfg.audience],
              SigningAlgorithm=self._cfg.signing_algorithm,
              DurationSeconds=self._cfg.duration_seconds,
          )
          jwt_str: str = resp["WebIdentityToken"]
          expiration = resp["Expiration"]  # tz-aware datetime in UTC
          expiry_epoch = expiration.timestamp()

          principal = (
              self._cfg.principal_name_override
              if self._cfg.principal_name_override is not None
              else _jwt.extract_sub(jwt_str)
          )
          extensions = self._cfg.sasl_extensions or {}
          return jwt_str, expiry_epoch, principal, extensions
  ```
- **No eager credential resolution.** `boto3.Session(region_name=...)` and `session.client('sts', ...)` do not invoke the credential chain; first `token()` call triggers it. Matches .NET/Go/JS lazy decision.

### Tests (`tests/unit/oauthbearer/aws/test_provider.py`) — with `botocore.stub.Stubber`

Standard stdlib-shipped mock for boto3 clients. Pattern:

```python
from botocore.stub import Stubber
import boto3


def make_provider_with_stub():
    cfg = AwsOAuthConfig(region="us-east-1", audience="https://example.com")
    provider = AwsStsTokenProvider(cfg)
    stubber = Stubber(provider._sts)
    return provider, stubber
```

Request-capture cases — assert the outgoing request via `Stubber.add_response(expected_params=...)`:

- **Audience passthrough:** `cfg.audience = "https://foo"` → stubber asserts `Audience = ["https://foo"]`.
- **SigningAlgorithm passthrough:** `cfg.signing_algorithm = "RS256"` → stubber asserts `SigningAlgorithm = "RS256"`.
- **DurationSeconds passthrough:** `cfg.duration_seconds = 900` → stubber asserts `DurationSeconds = 900`.
- **Default SigningAlgorithm:** unset → stubber asserts `SigningAlgorithm = "ES384"`.

Response-mapping cases — assert the returned 4-tuple:

- **Happy path:** stubber returns:
  ```python
  {
      "WebIdentityToken": "<valid 3-segment JWT with sub=arn:aws:iam::123:role/R>",
      "Expiration": datetime(2026, 4, 21, 6, 6, 47, 641000, tzinfo=timezone.utc),
  }
  ```
  Assert returned tuple:
  - `token_str` = the JWT.
  - `expiry_epoch` = `datetime(...).timestamp()` (expected value computed in-test, not hard-coded).
  - `principal` = `"arn:aws:iam::123:role/R"`.
  - `extensions` = `{}`.
- **`principal_name_override` takes precedence:** set `cfg.principal_name_override = "explicit"`, JWT `sub` is different. Assert returned `principal == "explicit"`.
- **`sasl_extensions` round-trip:** `cfg.sasl_extensions = {"logicalCluster": "lkc-abc"}` → returned tuple's 4th element is that same dict.
- **Malformed JWT in response:** `WebIdentityToken = "not-a-jwt"` → `ValueError` from `_jwt.extract_sub` propagates. Provider does **not** swallow — the C extension's `oauth_cb` handler at [confluent_kafka.c:2338](src/confluent_kafka/src/confluent_kafka.c#L2338) converts raised exceptions into `rd_kafka_oauthbearer_set_token_failure`.
- **STS error — AccessDenied:** `stubber.add_client_error(service_error_code="AccessDenied", service_message="...")` → `token()` raises `botocore.exceptions.ClientError`. Test asserts error code is preserved.
- **STS error — OutboundWebIdentityFederationDisabledException:** same pattern; different code. Asserts full error-code string reaches the caller.
- **Cancellation:** N/A — boto3 sync calls don't take a cancellation token. A client-level timeout is out of scope for v1 (see design §11 open item for logging hook — timeout is a similar future-concern).

Construction cases:

- **Lazy creds:** patch `boto3.Session.get_credentials` with a `Mock` that tracks calls; construct provider; assert mock was **not** called. Construct `boto3.client('sts')` does not resolve credentials.
- **Invalid region:** `cfg = AwsOAuthConfig(region="not-a-region", audience="...")` — AWS SDK accepts the string at construction; the error surfaces on first actual call. Document this behavior rather than adding our own region allowlist (AWS can add new regions; we don't want to block).
- **Pre-built session injection:** `cfg.session = boto3.Session(...)` → provider uses that session, not a fresh one. Verify via `provider._sts._endpoint.host` matching the session's configuration.

### Exit criteria

- All tests pass.
- No real AWS calls anywhere in `tests/unit/oauthbearer/aws/`.
- Outgoing `get_web_identity_token` input shape matches AWS wire expectations per the cross-language probe work (`Audience`/`SigningAlgorithm`/`DurationSeconds` named fields in CamelCase — boto3 marshals to the AWS query form).
- `tox -e flake8,black,mypy` clean.

---

## M5 — Client integration contract + `oauth_cb` wiring tests

**Estimated effort:** 3–4 hours
**Depends on:** M4
**Blocks:** M7

### Context

Unlike .NET / Go / JS, Python's OAUTHBEARER hook is **already synchronous** — the C extension's `oauth_cb` at [src/confluent_kafka/src/confluent_kafka.c:2268-2352](src/confluent_kafka/src/confluent_kafka.c#L2268-L2352) calls the Python callable directly, parses the returned tuple, and forwards it to `rd_kafka_oauthbearer_set_token`. There is no sync-over-async bridge to build.

The return contract is verified at line 2291:
```c
PyArg_ParseTuple(result, "sd|sO!", &token, &expiry, &principal, &PyDict_Type, &extensions)
```
→ `(str, float, [str, dict])` — token + expiry required, principal + extensions optional. Our `AwsStsTokenProvider.token` returns the full 4-tuple unconditionally.

### Deliverables

- [src/confluent_kafka/oauthbearer/aws/__init__.py](src/confluent_kafka/oauthbearer/aws/__init__.py) — public re-exports:
  ```python
  from .provider import AwsOAuthConfig, AwsStsTokenProvider

  __all__ = ["AwsOAuthConfig", "AwsStsTokenProvider"]
  ```

- Docstring on `AwsStsTokenProvider.token` explicitly citing the `oauth_cb` 4-tuple contract and the C-extension line that parses it, so future readers trace the contract back to authoritative source.

### Tests (`tests/unit/oauthbearer/aws/test_oauth_cb_contract.py`)

Two flavours of test — one at the pure-Python level (no librdkafka), one against a real client instantiation.

**Flavour 1 — contract-shape tests (no network, no librdkafka):**

- **Tuple shape:** `provider.token` returns a 4-tuple, unpackable as `(token, expiry, principal, extensions)`.
- **Types:** `token: str`, `expiry: float`, `principal: str`, `extensions: dict`. Assert via `isinstance`.
- **Epoch-seconds convention:** `expiry` is absolute unix epoch seconds (not milliseconds, not relative). Test: mock stubber returns `Expiration=datetime(2026,4,21,6,6,47,641000,tzinfo=UTC)`; assert `expiry == 1776836807.641` (computed via `.timestamp()` in the assertion to avoid hard-coded drift).
- **Timezone handling:** mock returns a tz-aware datetime; `.timestamp()` returns correct UTC epoch. Test an additional datetime in a non-UTC tz (e.g. `timezone(timedelta(hours=+5))`) and confirm the epoch math is correct.

**Flavour 2 — integration with `Producer`/`Consumer`/`AdminClient` (librdkafka involved):**

- Pattern from [tests/test_oauth_cb.py:38-52](tests/test_oauth_cb.py#L38-L52). Build a `Consumer` with `oauth_cb=provider.token`, `sasl.mechanisms=OAUTHBEARER`, `security.protocol=sasl_plaintext`. Assert `provider.token` is invoked during client init (C extension forces an initial refresh). Use `unittest.mock.patch.object(provider, "token", wraps=provider.token)` to verify invocation.
- Repeat for `Producer` and `AdminClient` — each must successfully wire the provider.
- **Error path:** patch `provider.token` to raise `RuntimeError`. Assert the client-init path does NOT crash the process; the error surfaces via the C extension's `rd_kafka_oauthbearer_set_token_failure` path (observed as a `KafkaException` or a failed connect later, not a raised exception at construction).

### Exit criteria

- All tests pass locally (`pytest tests/unit/oauthbearer/aws/ -v`).
- Public API surface matches design §5c exactly:
  ```bash
  python -c "from confluent_kafka.oauthbearer.aws import AwsOAuthConfig, AwsStsTokenProvider; print('ok')"
  ```
- `provider.token` is directly assignable to `oauth_cb` — no wrapping required by users.
- Cross-reference: [tests/test_oauth_cb.py](tests/test_oauth_cb.py) passes unchanged (regression guard — we haven't perturbed the existing oauth_cb contract).

---

## M6 — Example + README/INSTALL.md updates

**Estimated effort:** 2–3 hours
**Depends on:** M5
**Parallelizable with:** M7

### Deliverables

- [examples/oauth_aws_producer.py](examples/oauth_aws_producer.py) — mirrors [examples/oauth_producer.py](examples/oauth_producer.py) but uses `AwsStsTokenProvider` instead of the `requests`-based JWT flow:
  ```python
  #!/usr/bin/env python
  import argparse
  import logging

  from confluent_kafka import Producer
  from confluent_kafka.oauthbearer.aws import AwsOAuthConfig, AwsStsTokenProvider
  from confluent_kafka.serialization import StringSerializer


  def producer_config(args):
      provider = AwsStsTokenProvider(AwsOAuthConfig(
          region=args.region,
          audience=args.audience,
          duration_seconds=args.duration,
      ))
      return {
          "bootstrap.servers": args.bootstrap_servers,
          "security.protocol": "SASL_SSL",
          "sasl.mechanisms": "OAUTHBEARER",
          "oauth_cb": provider.token,
          "logger": logging.getLogger(__name__),
      }


  # ... delivery_report / main / argparse scaffold copied from oauth_producer.py ...

  if __name__ == "__main__":
      p = argparse.ArgumentParser(description="AWS STS GetWebIdentityToken OAUTHBEARER example")
      p.add_argument("-b", dest="bootstrap_servers", required=True)
      p.add_argument("-t", dest="topic", default="example_aws_oauth")
      p.add_argument("--region", required=True)
      p.add_argument("--audience", required=True)
      p.add_argument("--duration", type=int, default=3600)
      main(p.parse_args())
  ```

- [src/confluent_kafka/oauthbearer/aws/provider.py](src/confluent_kafka/oauthbearer/aws/provider.py) docstrings — full class/method docstrings on every public symbol. Reference the `oauth_cb` contract and the design doc location.

- [INSTALL.md](INSTALL.md) — add a short "Optional integrations" section (the file currently has no such section — extras aren't documented today):
  ```markdown
  ## Optional integrations

  Some integrations are opt-in via PyPI extras. Install with the bracket syntax:

  - `pip install 'confluent-kafka[schemaregistry]'` — Schema Registry client
  - `pip install 'confluent-kafka[avro,json,protobuf]'` — serializers
  - `pip install 'confluent-kafka[rules]'` — schema rules + KMS encryption
  - `pip install 'confluent-kafka[oauthbearer-aws]'` — AWS STS GetWebIdentityToken
    OAUTHBEARER token provider. See [DESIGN_AWS_OAUTHBEARER.md](DESIGN_AWS_OAUTHBEARER.md).
  ```

- [README.md](README.md) — add a single bullet in whichever section lists features or integrations (check current structure first; don't force a new section if one already fits).

- Short README at [src/confluent_kafka/oauthbearer/aws/README.md](src/confluent_kafka/oauthbearer/aws/README.md) (only if the subpackage docs help someone browsing the source tree — otherwise the design doc is sufficient):
  - Overview (2 paragraphs).
  - Minimum AWS SDK version note (`boto3>=1.42.25`).
  - One-line integration snippet.
  - The **"do NOT set `sasl.oauthbearer.method`"** warning (design §2 and universal rule across all four language siblings).
  - IAM prerequisite: `aws iam enable-outbound-web-identity-federation` run once per account by an admin (design §10).
  - Pointer to [DESIGN_AWS_OAUTHBEARER.md](DESIGN_AWS_OAUTHBEARER.md) and [IMPLEMENTATION_PLAN_AWS_OAUTHBEARER.md](IMPLEMENTATION_PLAN_AWS_OAUTHBEARER.md).

### Exit criteria

- `python examples/oauth_aws_producer.py --help` succeeds (syntactic + import check).
- Full invocation against the shared EC2 test box + a Confluent Cloud-style broker succeeds — deferred to M7 reproduction run; no need to duplicate.
- `tox -e flake8,black,isort` clean for the new example.
- `pip install -e '.[oauthbearer-aws]'` followed by `python -c "from confluent_kafka.oauthbearer.aws import *; help(AwsStsTokenProvider)"` produces readable docs.

---

## M7 — Real-AWS integration test (scaffold for manual E2E)

**Estimated effort:** 3–4 hours (test code + one validation run)
**Depends on:** M4
**Parallelizable with:** M6

### Deliverables

- [tests/integration/oauthbearer/aws/test_provider_real.py](tests/integration/oauthbearer/aws/test_provider_real.py):
  ```python
  import os
  import re
  import time

  import pytest

  from confluent_kafka.oauthbearer.aws import AwsOAuthConfig, AwsStsTokenProvider


  @pytest.mark.skipif(
      os.environ.get("RUN_AWS_STS_REAL") != "1",
      reason="Set RUN_AWS_STS_REAL=1 and provide AWS credentials to run.",
  )
  def test_get_web_identity_token_real():
      cfg = AwsOAuthConfig(
          region=os.environ.get("AWS_REGION", "eu-north-1"),
          audience=os.environ.get("AUDIENCE", "https://api.example.com"),
          duration_seconds=300,
      )
      provider = AwsStsTokenProvider(cfg)
      token, expiry, principal, extensions = provider.token("")

      assert re.match(r"^[A-Za-z0-9\-_]+\.[A-Za-z0-9\-_]+\.[A-Za-z0-9\-_]+$", token)
      now = time.time()
      assert expiry > now
      assert expiry < now + 10 * 60
      assert re.match(r"^arn:aws:iam::\d+:role/.+$", principal)
      assert extensions == {}


  @pytest.mark.skipif(
      os.environ.get("RUN_AWS_STS_REAL") != "1",
      reason="Set RUN_AWS_STS_REAL=1 and provide AWS credentials to run.",
  )
  def test_token_length_matches_cross_language_observation():
      """Live AWS STS responses are 1256 chars on the shared test role.
      Matches Go / .NET / JS / librdkafka observations exactly.
      """
      cfg = AwsOAuthConfig(region="eu-north-1", audience="https://api.example.com")
      provider = AwsStsTokenProvider(cfg)
      token, *_ = provider.token("")
      assert len(token) == 1256, f"expected 1256 chars, got {len(token)}"
  ```

- [tests/integration/oauthbearer/aws/__init__.py](tests/integration/oauthbearer/aws/__init__.py) — empty.

- [tests/integration/oauthbearer/aws/TESTING.md](tests/integration/oauthbearer/aws/TESTING.md):
  - Required env vars (`RUN_AWS_STS_REAL=1`, optionally `AWS_REGION`, `AUDIENCE`).
  - EC2 role prerequisites: `sts:GetWebIdentityToken` permission; account-level `aws iam enable-outbound-web-identity-federation` executed.
  - Run command:
    ```bash
    RUN_AWS_STS_REAL=1 AWS_REGION=eu-north-1 AUDIENCE=https://api.example.com \
      pytest tests/integration/oauthbearer/aws/ -v
    ```
  - Reference the EC2 test box (`ktrue-iam-sts-test-role` in `eu-north-1`, account `708975691912`) shared with the Go / .NET / JS / librdkafka M7 runs — same role, same account, same prerequisites already satisfied. Byte-identical 1256-char JWT expected.

- [tox.ini](tox.ini) — confirm `testpaths = tests` includes `tests/integration/` (it does today), and that the default `pytest` invocation skips these tests when `RUN_AWS_STS_REAL` is unset. Verify with a local dry run.

### Scope boundary

This milestone verifies the package MINTS a valid token. It does **not** verify the token is accepted by a Kafka broker — that requires Confluent Cloud OIDC trust configuration, an admin action outside this implementation's scope. Broker-side acceptance is a manual E2E step the owner drives separately.

### Exit criteria

- Default `pytest tests/` skips the real-AWS tests (proven locally with `RUN_AWS_STS_REAL` unset).
- Owner runs the test on the shared EC2 box with env vars set and reports green. Attach test output to the PR.
- JWT length matches cross-language observation (1256 chars on `ktrue-iam-sts-test-role`).

---

## M8 — Release wiring

**Estimated effort:** 2–3 hours
**Depends on:** M1–M7 all complete

### Deliverables

1. **Verify [setup.py](setup.py) and [MANIFEST.in](MANIFEST.in)** — confirm the new subpackage (`confluent_kafka/oauthbearer/aws/`) and the `py.typed` marker are picked up by the wheel build. Build a wheel locally and inspect:
   ```bash
   python -m build --wheel
   unzip -l dist/confluent_kafka-2.15.0-*.whl | grep oauthbearer
   ```
   Expected: `confluent_kafka/oauthbearer/__init__.py`, `confluent_kafka/oauthbearer/aws/__init__.py`, `confluent_kafka/oauthbearer/aws/provider.py`, `confluent_kafka/oauthbearer/aws/_jwt.py`, `confluent_kafka/oauthbearer/aws/py.typed` all present. Fix MANIFEST.in if anything is missing.

2. **[CHANGELOG.md](CHANGELOG.md)** — add under the target version header:
   ```markdown
   ## Python Client

   ### Enhancements

   - Added optional PyPI extra `confluent-kafka[oauthbearer-aws]` — AWS STS
     `GetWebIdentityToken` OAUTHBEARER token provider. Opt-in; users not
     installing the extra see zero change in their `confluent-kafka`
     dependency graph. See `DESIGN_AWS_OAUTHBEARER.md` and
     `IMPLEMENTATION_PLAN_AWS_OAUTHBEARER.md` for details.
   ```

3. **[README.md](README.md)** — add a one-line entry under whichever section lists features or integrations (design §2 — the "Optional integrations" bullet list). Mirror the shape used for existing extras (`schemaregistry`, `avro`, `rules`).

4. **[INSTALL.md](INSTALL.md)** — the "Optional integrations" section landed in M6; verify the `oauthbearer-aws` entry is there and points at the design doc.

5. **[.semaphore/semaphore.yml](.semaphore/semaphore.yml)** — verify the existing test stages pick up `tests/unit/oauthbearer/aws/` automatically. The `tests` aggregate extra includes `oauthbearer-aws` (per M1), so boto3 is installed in any CI env running `pip install .[tests]`. If a stage runs a narrower install, add `oauthbearer-aws` to its pin list.

6. **Verify repo-wide tooling**:
   - `tox -e py38,py39,py310,py311,py312,py313` passes (at least one 3.x row, ideally all).
   - `tox -e flake8,black,isort,mypy` clean.
   - `pip install -e '.[dev]'` brings in boto3 (via `oauthbearer-aws` being folded into `dev`).
   - `python -c "import confluent_kafka; print(confluent_kafka.version())"` still works on a base install in a fresh venv.

7. **Release rehearsal** (local):
   - Bump [pyproject.toml:7](pyproject.toml#L7) `version = "2.14.0"` → `"2.15.0rc0"` on a scratch branch.
   - `python -m build` → inspect `dist/*.whl` and `dist/*.tar.gz`:
     - Wheel contains the new subpackage files (verified above).
     - `pip install dist/confluent_kafka-2.15.0rc0-*.whl` into a scratch venv — confirm the base install still doesn't pull boto3.
     - `pip install 'dist/confluent_kafka-2.15.0rc0-*.whl[oauthbearer-aws]'` — confirm boto3 is pulled, and `from confluent_kafka.oauthbearer.aws import AwsStsTokenProvider` succeeds.
   - Roll back the version bump (do not commit).

### Exit criteria

- All CI targets green.
- Rehearsal resolution confirmed (wheel builds, installs both ways, imports as expected).
- CHANGELOG + README + INSTALL.md updates merged.
- `pyproject.toml` `version` field ready to bump at the target release tag.

---

## Post-implementation validation — to be executed after M8

To be filled in by the owner after the real validation run on the shared EC2 box (`ktrue-iam-sts-test-role` in `eu-north-1`, account `708975691912` — same box used by the Go / .NET / JS post-implementation validations). Template below; measurements live in subsections 1a/1b/2a/2b.

### Phase 0 — Prep

Two scratch virtualenvs in `/tmp/` outside the repo, each running `pip install` against a wheel built from the branch. Wheel-based (not editable) install is what users actually experience.

**0a. SSH to the EC2 box and clone/update the repo**

```bash
ssh ec2-user@<your-ec2-box>
cd ~
git clone https://github.com/confluentinc/confluent-kafka-python.git
cd confluent-kafka-python
git checkout dev_prashah_IAM_Python_POC
git pull --ff-only
```

**0b. Verify Python 3.11+ and pip are available**

```bash
python3.11 --version   # or whichever Python the CI targets
python3.11 -m pip --version
```

**0c. Build the wheel**

```bash
python3.11 -m venv /tmp/build-venv
source /tmp/build-venv/bin/activate
pip install build
python -m build --wheel
deactivate
ls -la dist/*.whl
REPO=$HOME/confluent-kafka-python
WHEEL=$(ls $REPO/dist/confluent_kafka-*.whl)
```

**0d. Confirm AWS prerequisites**

```bash
TOKEN=$(curl -s -X PUT "http://169.254.169.254/latest/api/token" \
  -H "X-aws-ec2-metadata-token-ttl-seconds: 21600")
curl -s -H "X-aws-ec2-metadata-token: $TOKEN" \
  http://169.254.169.254/latest/meta-data/iam/security-credentials/
# Expect: ktrue-iam-sts-test-role

aws sts get-web-identity-token \
  --audience https://api.example.com \
  --signing-algorithm RS256 \
  --region eu-north-1 > /dev/null && echo OK
```

**0e. Wipe any prior scratch dirs**

```bash
rm -rf /tmp/aws-consumer /tmp/no-aws-consumer
```

### Phase 1 — Scenario 1 (OPTS IN, with AWS)

**1a. Create venv and install with extra**

```bash
python3.11 -m venv /tmp/aws-consumer
source /tmp/aws-consumer/bin/activate
pip install "$WHEEL[oauthbearer-aws]"
```

**1b. Write `/tmp/aws-consumer/app.py` — minimal program that mints a real JWT**

```python
import os
import time

from confluent_kafka.oauthbearer.aws import AwsOAuthConfig, AwsStsTokenProvider

provider = AwsStsTokenProvider(AwsOAuthConfig(
    region=os.environ.get("AWS_REGION", "eu-north-1"),
    audience=os.environ.get("AUDIENCE", "https://api.example.com"),
    duration_seconds=300,
))
token, expiry, principal, extensions = provider.token("")

print(f"JWT length     : {len(token)} chars")
print(f"Principal      : {principal}")
print(f"Expiry (epoch) : {expiry}")
print(f"Expires in     : {int(expiry - time.time())}s")
```

**1c. Measure**

```bash
echo "site-packages size:" ; du -sh /tmp/aws-consumer/lib/python*/site-packages ; du -sb /tmp/aws-consumer/lib/python*/site-packages
echo "boto3 packages:"     ; pip list | grep -iE '^(boto|botocore|s3transfer|jmespath|urllib3)'
echo "installed count:"    ; pip list | wc -l
```

**1d. Runtime proof** — actually mint a JWT against real AWS:

```bash
AWS_REGION=eu-north-1 AUDIENCE=https://api.example.com python /tmp/aws-consumer/app.py
```

Expected output (matches Go / .NET / JS / librdkafka cross-language observation):

```
JWT length     : 1256 chars
Principal      : arn:aws:iam::708975691912:role/ktrue-iam-sts-test-role
Expiry (epoch) : <unix-seconds>
Expires in     : 299s
```

```bash
deactivate
```

### Phase 2 — Scenario 2 (OPTS OUT, no AWS)

**2a. Create venv and install without extra**

```bash
python3.11 -m venv /tmp/no-aws-consumer
source /tmp/no-aws-consumer/bin/activate
pip install "$WHEEL"
# NOTE: no [oauthbearer-aws] selector.
```

**2b. Write `/tmp/no-aws-consumer/app.py` — proves `confluent_kafka` loads without any AWS SDK**

```python
import confluent_kafka

print(f"confluent_kafka version: {confluent_kafka.version()}")
print(f"librdkafka version: {confluent_kafka.libversion()}")

try:
    import boto3
    print("LEAKED: boto3 is importable")
except ImportError:
    print("OK: boto3 is not installed")

try:
    from confluent_kafka.oauthbearer.aws import AwsStsTokenProvider
    print("LEAKED: oauthbearer.aws imported successfully")
except ImportError as exc:
    print(f"OK: oauthbearer.aws import fails as expected: {exc}")
```

**2c. Measure — these numbers prove the invariant**

```bash
echo "site-packages size:" ; du -sh /tmp/no-aws-consumer/lib/python*/site-packages ; du -sb /tmp/no-aws-consumer/lib/python*/site-packages
echo "boto3 check:"        ; pip show boto3 >/dev/null 2>&1 && echo LEAKED || echo OK
echo "installed count:"    ; pip list | wc -l
```

Expected: `boto3 check: OK`, installed count just the base dep list (typing-extensions on Py<3.11, or empty on ≥3.11 after accounting for pip itself).

**2d. Runtime proof** — core still works without boto3:

```bash
python /tmp/no-aws-consumer/app.py
```

Expected:
```
confluent_kafka version: (2, 15, 0, ...)
librdkafka version: (2, ...)
OK: boto3 is not installed
OK: oauthbearer.aws import fails as expected: No module named 'boto3'
```

```bash
deactivate
```

### Phase 3 — Cleanup

```bash
rm -rf /tmp/aws-consumer /tmp/no-aws-consumer /tmp/build-venv
rm -f $REPO/dist/*.whl $REPO/dist/*.tar.gz
```

One `sts:GetWebIdentityToken` call recorded in CloudTrail per Scenario 1 run.

### The zero-cost property, quantified — template (to be filled)

| Metric | Scenario 1 (opt-in) | Scenario 2 (opt-out) | Delta |
|---|---|---|---|
| `site-packages` size | _to measure_ | _to measure_ | _to measure_ |
| Installed packages (`pip list`) | _to measure_ | _to measure_ | _to measure_ |
| `boto3` resolvable | yes | **no** | — |
| `confluent_kafka.oauthbearer.aws` importable | yes | **no (ModuleNotFoundError)** | — |
| Real JWT minted at runtime | **expected 1256 chars**, principal `arn:aws:iam::708975691912:role/ktrue-iam-sts-test-role`, expiry ≤ 300s | n/a | — |

Expected shape: the delta should be entirely boto3 + botocore + their transitives (urllib3, jmespath, s3transfer, python-dateutil). Everything else (`confluent_kafka.cimpl` extension, `confluent_kafka/__init__.py`, librdkafka shared object) byte-identical between the two venvs.

### Cross-language mint consistency — expected

1256-char JWT should reproduce exactly across all five clients on the same EC2 box / role / audience. Fill in once the Python run completes:

| Client | JWT length | Principal | Source |
|---|---|---|---|
| librdkafka (C) | 1256 | `arn:aws:iam::708975691912:role/ktrue-iam-sts-test-role` | librdkafka project memory, Probe A / M7 |
| Go (submodule) | 1256 | same | `confluent-kafka-go` IMPL_PLAN Scenario 1 |
| .NET (NuGet) | 1256 | same | `confluent-kafka-dotnet` IMPL_PLAN Scenario 1 |
| JS (npm) | 1256 | same | `confluent-kafka-javascript` IMPL_PLAN Scenario 1 |
| **Python (extra, this)** | _expected 1256_ | _expected same_ | this plan, Scenario 1 |

AWS STS is the single source of truth; every client's glue layer is proven correct by convergence on the same byte count and principal.

---

## Open items for later (not blocking implementation)

1. **Release target version** — pick at tag time. Leaning toward `2.15.0` cut after implementation merges.
2. **`confluent_kafka.aio` validation** — the aio producer/consumer at [src/confluent_kafka/aio/](src/confluent_kafka/aio/) still bottoms out in the same C `oauth_cb` callback infrastructure, so the sync `provider.token` should work unchanged. Confirm with an integration test during M7 if the aio path is reachable there; otherwise defer until user demand appears.
3. **Additional token providers** — `confluent_kafka.oauthbearer.azure`, `.gcp`. The subpackage + per-cloud extra pattern reserves space; implementations are independent future work.
4. **boto3 floor bump cadence** — if the `rules` extra's boto3 floor ever rises above `1.42.25`, keep `oauthbearer-aws` in step to minimize peer-dep pin conflicts for users who install both.
5. **Logging hook** — users wanting to plug in their own `logging.Logger` for boto3 diagnostics. boto3 uses the stdlib `logging` module — users can configure it globally today; a dedicated hook is only worthwhile if someone requests it.
6. **Request timeout** — boto3's default read/connect timeout (60s) applies; `AwsOAuthConfig` does not expose a knob. If users report broker refresh timeouts caused by slow STS regional endpoints, add `AwsOAuthConfig.client_timeout_seconds`. Defer until reported.
7. **Credential caching** — boto3 caches via its internal credential resolver; we do not cache at the provider level. librdkafka drives refresh cadence via token expiration. Document in the provider docstring; no code change needed.

---

## References

- [DESIGN_AWS_OAUTHBEARER.md](DESIGN_AWS_OAUTHBEARER.md) — full design doc.
- Go plan (template): `IMPLEMENTATION_PLAN_AWS_OAUTHBEARER.md` in `confluent-kafka-go`.
- .NET plan (template): `IMPLEMENTATION_PLAN_AWS_OAUTHBEARER.md` in `confluent-kafka-dotnet`.
- JS plan (template): `IMPLEMENTATION_PLAN_AWS_OAUTHBEARER.md` in `confluent-kafka-javascript`.
- Existing optional-package precedents (extras + unconditional imports): [src/confluent_kafka/schema_registry/rules/encryption/awskms/aws_driver.py:17-21](src/confluent_kafka/schema_registry/rules/encryption/awskms/aws_driver.py#L17-L21), [src/confluent_kafka/schema_registry/common/avro.py:10-11](src/confluent_kafka/schema_registry/common/avro.py#L10-L11), [src/confluent_kafka/schema_registry/common/protobuf.py:8](src/confluent_kafka/schema_registry/common/protobuf.py#L8).
- OAUTHBEARER C-extension hook: [src/confluent_kafka/src/confluent_kafka.c:2268-2352](src/confluent_kafka/src/confluent_kafka.c#L2268-L2352).
- Existing custom `oauth_cb` example: [examples/oauth_producer.py:34-55](examples/oauth_producer.py#L34-L55).
- Existing `oauth_cb` unit tests: [tests/test_oauth_cb.py:38-69](tests/test_oauth_cb.py#L38-L69).
- Extras plumbing: [pyproject.toml:101-143](pyproject.toml#L101-L143), [requirements/](requirements/).
- boto3 `get_web_identity_token` API: [boto3 docs](https://docs.aws.amazon.com/boto3/latest/reference/services/sts/client/get_web_identity_token.html).
- Python-side project memory: `project_python_aws_oauthbearer.md` and `project_optional_deps_pattern_python.md` in this project's `~/.claude` memory dir.
