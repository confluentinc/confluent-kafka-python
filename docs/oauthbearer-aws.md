AWS IAM-based authentication for Kafka clients via the **OAUTHBEARER** SASL mechanism, delivered as the optional `confluent-kafka[oauthbearer-aws]` install extra. When the application runs on AWS compute (EC2, EKS, ECS, Fargate, or Lambda) with an IAM role attached, the client mints short-lived JSON Web Tokens through AWS STS `GetWebIdentityToken` (via `boto3`) and hands them to the broker as the SASL bearer credential — refreshing them automatically before they expire.

**Activation is config-only.** Install the extra and set the three configuration keys below; no application code changes are required at the integration site. Applications that do not install the extra pull **zero AWS dependencies**.

# Installation

``` bash
pip install 'confluent-kafka[oauthbearer-aws]'
```

This adds `boto3` to the dependency graph. Without the extra, nothing AWS is imported or installed.

# Quick start

The minimum configuration is the two required `sasl.oauthbearer.config` keys — `region` and `audience` — alongside the standard SASL/OAUTHBEARER settings:

``` python
from confluent_kafka import Consumer

consumer = Consumer({
    'bootstrap.servers': 'pkc-xxxx.aws.confluent.cloud:9092',
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'OAUTHBEARER',

    'sasl.oauthbearer.method': 'oidc',
    'sasl.oauthbearer.metadata.authentication.type': 'aws_iam',
    'sasl.oauthbearer.config': 'region=us-east-1,audience=https://confluent.cloud/oidc',

    'group.id': 'my-group',
    'auto.offset.reset': 'earliest',
})
consumer.subscribe(['my-topic'])
```

The same SASL keys apply unchanged to `Producer` and `AdminClient`.

# All options

Every supported key, set on a single comma-separated `sasl.oauthbearer.config` string, with SASL extensions supplied through the separate `sasl.oauthbearer.extensions` key:

``` python
conf = {
    'bootstrap.servers': 'pkc-xxxx.aws.confluent.cloud:9092',
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'OAUTHBEARER',

    'sasl.oauthbearer.method': 'oidc',
    'sasl.oauthbearer.metadata.authentication.type': 'aws_iam',
    'sasl.oauthbearer.config': (
        'region=us-east-1,'
        'audience=https://confluent.cloud/oidc,'
        'duration_seconds=900,'
        'signing_algorithm=ES384,'
        'sts_endpoint=https://sts-fips.us-east-1.amazonaws.com,'
        'aws_debug=console,'
        'tag_team=platform,'
        'tag_environment=prod'
    ),
    'sasl.oauthbearer.extensions': (
        'logicalCluster=lkc-abc,'
        'identityPoolId=pool-xyz'
    ),
}
```

# Configuration

`sasl.oauthbearer.config` is a single string of comma-separated `key=value` pairs, e.g. `region=us-east-1,audience=https://confluent.cloud/oidc`. Leading and trailing whitespace around each pair is ignored, but do **not** put spaces directly around the `=` — `region = us-east-1` is read as a key named `region` *with a trailing space* and rejected. Each pair splits on its first `=`, so a value may itself contain `=` (no escaping needed); to include a literal comma in a value, escape it with a backslash (`\,`). Any key not listed below is rejected with a descriptive error.

## Required

| Key | Description |
|----|----|
| `region` | AWS region used for the STS `GetWebIdentityToken` call (e.g. `us-east-1`). |
| `audience` | The OIDC audience the broker expects; must match the audience configured on the IAM role's trust relationship (e.g. `https://confluent.cloud/oidc`). |

## Optional

| Key | Default | Description |
|----|----|----|
| `duration_seconds` | `300` | Requested token lifetime in seconds (60–3600). The client auto-refreshes the token at roughly 80% of this value. |
| `signing_algorithm` | `ES384` | JWT signature algorithm: `ES384` or `RS256`. |
| `sts_endpoint` | SDK default | Override the STS endpoint URL — e.g. a FIPS or VPC (PrivateLink) endpoint. |
| `aws_debug` | `none` | AWS SDK diagnostic logging: `none` or `console`. |
| `tag_<name>` | — | Adds a custom claim to the minted token (repeatable; up to 50). For example `tag_team=platform` adds a `team` tag with value `platform`. |

## Signing algorithm

`ES384` (the default) produces compact ECDSA signatures and is recommended for most deployments. Use `RS256` only when an RSA-based signature is specifically required.

## AWS SDK diagnostic logging

`aws_debug` controls AWS SDK log output, which is useful when validating the credential chain or diagnosing STS failures:

- `none` (default) — the client does not touch AWS SDK logging.
- `console` — enables verbose `botocore` logging to standard error.

``` text
# Quiet (production)
region=us-east-1,audience=https://confluent.cloud/oidc

# Verbose AWS SDK output while debugging
region=us-east-1,audience=https://confluent.cloud/oidc,aws_debug=console
```

# SASL extensions

SASL/OAUTHBEARER extensions (for example, a Confluent Cloud logical cluster and identity pool) are configured through the separate `sasl.oauthbearer.extensions` key — **not** inside `sasl.oauthbearer.config` — as a comma-separated list of `key=value` pairs:

``` python
'sasl.oauthbearer.extensions': 'logicalCluster=lkc-abc,identityPoolId=pool-xyz'
```

# Prerequisites

- The application runs with AWS credentials resolvable by `boto3`'s default credential chain — an EC2 instance profile, EKS IRSA, an ECS/Fargate task role, a Lambda execution role, environment variables, or a shared credentials file.
- The IAM role is permitted to mint a web identity token via STS `GetWebIdentityToken` for the requested `audience`.
- The `audience` value matches the audience configured on the IAM role's trust relationship and the broker-side identity provider.

# Common pitfalls

- **\`\`sasl.oauthbearer.method=oidc\`\` is mandatory.** When `sasl.oauthbearer.metadata.authentication.type=aws_iam` is set, omitting or changing the method raises a `ValueError` at client construction.
- **Set \`\`security.protocol=SASL_SSL\`\` and \`\`sasl.mechanisms=OAUTHBEARER\`\`.** Without them the OAUTHBEARER token-refresh path never engages and authentication silently does not happen.
- **\`\`sasl.oauthbearer.config\`\` must be present and non-empty** — it carries the required `region` and `audience`. A missing or empty value raises a `ValueError` naming the missing keys.
- **Install the extra, not \`\`boto3\`\` directly.** If the marker is set but the extra is not installed, client construction raises an `ImportError` pointing to `pip install 'confluent-kafka[oauthbearer-aws]'`.
- **\`\`audience\`\` mismatches fail at STS.** If the configured `audience` does not match the IAM role's trust relationship, the `GetWebIdentityToken` call is rejected and no token is issued.
- **\`\`botocore\[crt\]\`\` may be needed for \`\`aws login\`\` credential sessions.** Like any boto3 application, this integration uses boto3's default credential chain — so if your active AWS profile uses an `aws login` session (a `login_session` entry), boto3 raises `MissingDependencyException` asking you to `pip install "botocore[crt]"`. This does not occur when credentials come from an attached IAM role (as on AWS compute). Fix: `pip install "botocore[crt]"`, or use a credential source without a login session.

# Requirements

- The `oauthbearer-aws` extra installs `boto3` (a version recent enough to expose the STS `GetWebIdentityToken` API).

# Further reading

A complete, runnable producer/consumer/admin example is available at [examples/oauth_oidc_ccloud_aws_iam.py](https://github.com/confluentinc/confluent-kafka-python/blob/master/examples/oauth_oidc_ccloud_aws_iam.py).
