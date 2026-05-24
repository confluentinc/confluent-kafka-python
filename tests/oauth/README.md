# OAUTHBEARER integration test fixture

Local Kafka broker configured to accept OAUTHBEARER SASL via Apache Kafka's
**unsecured** validator (KIP-255). The broker accepts unsigned JWT-shaped
tokens whose claims have the right shape and a future `exp`. No external
OAuth server, no signing key, no Docker required.

> **Never use this config in production.** The unsecured validator does
> not verify signatures.

## Prerequisites

A built Apache Kafka source tree at `$KAFKA_HOME` (default
`~/projects/kafka`). The scripts call `kafka-storage.sh` and
`kafka-server-start.sh` from `$KAFKA_HOME/bin`.

```sh
# In ~/projects/kafka, if not already built:
./gradlew jar
```

## Running

```sh
# Foreground (Ctrl+C to stop)
./start_broker.sh

# Background
./start_broker.sh -daemon
./stop_broker.sh --clean
```

The broker listens on:

- `localhost:9094` — client traffic, SASL_PLAINTEXT + OAUTHBEARER
- `localhost:9093` — KRaft controller (internal)

Storage goes to `/tmp/kraft-oauth-test-logs` and is wiped by
`stop_broker.sh --clean`.

## Producing a token from the client

The broker accepts JWTs of the form:

```
base64url({"alg":"none","typ":"JWT"}) . base64url({"sub":"alice","iat":<now>,"exp":<future>}) .
```

Note the trailing dot — the signature segment is empty. A helper that
constructs this for use in `oauth_cb` lives at
[`unsecured_token.py`](unsecured_token.py) (added in Step 2 of the plan).

## Re-authentication window

`connections.max.reauth.ms=10000` forces the broker to disconnect any
client that hasn't re-authenticated within 10 s. Combined with a client
token whose lifetime is shorter than 10 s, this exercises the
token-refresh code path inside a single test run.
