# Real-AWS integration tests — AWS OAUTHBEARER provider

These tests invoke the real AWS STS `GetWebIdentityToken` API and are **opt-in
only**. Default `pytest` runs skip them — the gate is the environment variable
`RUN_AWS_STS_REAL=1`.

## Prerequisites

1. **AWS credentials** resolvable via the default boto3 chain (env vars / EKS
   IRSA / ECS / IMDSv2 / shared profile / SSO).
2. **IAM permission** on the caller: `sts:GetWebIdentityToken`.
3. **Account-level enablement**: an admin must have run
   `aws iam enable-outbound-web-identity-federation` once on the target
   account. First call on an un-enabled account returns
   `OutboundWebIdentityFederationDisabledException`.

## Shared EC2 test environment

Used by the Go, .NET, JS, and librdkafka implementations:

| Field | Value |
|---|---|
| IAM role | `ktrue-iam-sts-test-role` |
| Region | `eu-north-1` |
| Account | `708975691912` |
| Expected JWT length | **1256 chars** (identical across all language clients) |
| Audience | `https://api.example.com` |

## Location note

This test lives under `tests/unit/oauthbearer/aws/test_provider_real.py`
even though it hits real AWS. Reason: `tests/integration/conftest.py`
eagerly imports `trivup.clusters.KafkaCluster`, which is only meaningful
for broker-side integration tests. The STS provider has no Kafka-broker
dependency, so it's kept with the rest of the provider's tests and gated
purely on `RUN_AWS_STS_REAL`.

## Run

```bash
cd confluent-kafka-python
pip install -e '.[oauthbearer-aws,tests]'

RUN_AWS_STS_REAL=1 \
    AWS_REGION=eu-north-1 \
    AUDIENCE=https://api.example.com \
    pytest tests/unit/oauthbearer/aws/test_provider_real.py -v
```

Optional env vars:

- `DURATION_SECONDS` (default `300`) — STS `DurationSeconds` request parameter.
- `EXPECTED_TOKEN_LEN` (default `1256`) — override if running on a different
  role/audience with a different observed length.

## Scope boundary

These tests verify the provider **mints** a valid JWT. They do **not** verify
the token is accepted by a Kafka broker — broker-side OIDC trust
configuration is a separate admin step outside this implementation's scope.
Broker-accept testing is driven manually.
