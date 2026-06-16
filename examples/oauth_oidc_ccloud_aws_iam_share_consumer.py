#!/usr/bin/env python3
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
#
"""KIP-932 ShareConsumer round-trip against Confluent Cloud with AWS IAM auth.

A ShareConsumer variation of the AWS-IAM-Examples producer
(https://github.com/confluentinc/AWS-IAM-Examples). It reuses that example's
custom OAUTHBEARER callback verbatim — mint a short-lived JWT via AWS STS
GetWebIdentityToken (boto3) and hand it to librdkafka — but instead of only
producing, it does a full round-trip:

    1. (AdminClient) ensure the topic exists
    2. (AdminClient) set share.auto.offset.reset=earliest on the share group —
       a per-group, broker-side config (default "latest") — so it reads from the
       start. The consumer's own auto.offset.reset does NOT control this.
    3. (Producer) produce N messages
    4. (ShareConsumer) subscribe and poll until all N are consumed or a deadline
       passes (implicit acknowledgement — each poll auto-acks the previous batch)

All three clients authenticate the same way: SASL_SSL / OAUTHBEARER with the
AWS-IAM oauth_cb. No Kafka API keys are involved.

This is meant to run on AWS compute (EC2/EKS/ECS) whose IAM role the Confluent
Cloud identity pool trusts, exactly like the AWS-IAM-Examples setup — boto3
picks up the role credentials automatically and the role only needs
sts:GetWebIdentityToken.

Config comes from env vars (mirrors the example's ConfigMap); bootstrap may
also be passed as argv[1]:

    BOOTSTRAP_SERVERS     Confluent Cloud bootstrap (host:9092)  [required; or argv[1]]
    KAFKA_LOGICAL_CLUSTER CC logical cluster id (lkc-xxxxx)      [required]
    IDENTITY_POOL_ID      CC identity pool id (pool-xxxxx)       [required]
    OIDC_AUDIENCE         STS token audience / JWT aud claim     [default https://confluent.cloud/oidc]
    AWS_STS_REGION        STS region                             [default us-east-2]
    KAFKA_TOPIC           topic name                             [default share_aws_iam_demo]
    GROUP_ID              share group id                         [default share-aws-iam-<rand>]
    MESSAGE_COUNT         messages to round-trip                 [default 20]
    CONSUMER_KIND         share | regular                        [default share]

These env var names match the autowire harness (opt_in_success.py), so the
same exported values work for both.

Acknowledgement is implicit: each poll() auto-acknowledges the previous
batch, and close() flushes the final one.

Usage:
    python oauth_oidc_ccloud_aws_iam_share_consumer.py [bootstrap.servers]
"""

import json
import os
import random
import sys
import time
import uuid
from datetime import datetime, timezone

import boto3
from confluent_kafka import Consumer, KafkaException, Producer, ShareConsumer
from confluent_kafka.admin import (
    AdminClient,
    AlterConfigOpType,
    ConfigEntry,
    ConfigResource,
    NewTopic,
    ResourceType,
)


def _ts():
    return datetime.now(timezone.utc).isoformat()


TOKEN_LIFETIME_SECONDS = 300


def get_aws_iam_token(audience, aws_region):
    """Mint a JWT via AWS STS GetWebIdentityToken.

    boto3 resolves the AWS credentials from the ambient role (EC2 instance
    profile, EKS Pod Identity, etc.); the role needs only
    sts:GetWebIdentityToken. The `audience` becomes the JWT's `aud` claim and
    must match what the Confluent Cloud identity provider expects (the same
    value the autowire scenario passes as `audience=` in
    sasl.oauthbearer.config, e.g. https://confluent.cloud/oidc).

    The lifetime is fixed at 300s on purpose: Confluent's identity-pool roles
    cap the web-identity-token duration (the official autowire extra also
    requests 300s), and longer values are rejected by IAM with AccessDenied —
    not the SessionDurationEscalation error — so there is nothing to fall back
    from. librdkafka simply refreshes the short-lived token as needed.

    :returns: (token_string, lifetime_seconds)
    """
    print(f"[{_ts()}] Requesting AWS IAM token...")
    sts_client = boto3.client('sts', region_name=aws_region)

    try:
        response = sts_client.get_web_identity_token(
            Audience=[audience],
            SigningAlgorithm='ES384',
            DurationSeconds=TOKEN_LIFETIME_SECONDS,
        )
    except Exception as exc: 
        sys.stderr.write(f"[{_ts()}] STS GetWebIdentityToken failed: {exc}\n")
        raise

    print(f"[{_ts()}] Got token (expires in {TOKEN_LIFETIME_SECONDS}s)")
    return response['WebIdentityToken'], TOKEN_LIFETIME_SECONDS


def make_oauth_cb(oauth_config):
    """Build the confluent-kafka oauth_cb closure.

    The callback returns the 4-tuple confluent-kafka expects:
    (token, absolute_expiry_epoch_seconds, principal, extensions_dict).
    The logicalCluster + identityPoolId SASL extensions route the token to the
    right CC cluster and identity pool — these mirror the
    sasl.oauthbearer.extensions the autowire scenario sends. The same callback
    is used by the producer, consumer, and admin client; for a ShareConsumer it
    flows through the binding's oauth_cb handler, which sets the token on the
    share consumer's underlying client.
    """
    def oauth_cb(_config_str):
        token, lifetime = get_aws_iam_token(
            oauth_config['audience'], oauth_config['aws_region'])
        return (token,
                time.time() + lifetime,
                "",
                {"logicalCluster": oauth_config['logical_cluster'],
                 "identityPoolId": oauth_config['identity_pool_id']})

    return oauth_cb


def common_config(bootstrap, oauth_cb):
    """Base SASL_SSL / OAUTHBEARER config shared by all three clients."""
    return {
        'bootstrap.servers': bootstrap,
        'security.protocol': 'SASL_SSL',
        'sasl.mechanisms': 'OAUTHBEARER',
        # Custom token source: with oauth_cb set, librdkafka never tries the
        # built-in OIDC token endpoint.
        'oauth_cb': oauth_cb,
    }


def ensure_topic(admin, topic):
    """Create the topic if it doesn't already exist (CC disables auto-create)."""
    fs = admin.create_topics([NewTopic(topic, num_partitions=1, replication_factor=3)])
    for t, fut in fs.items():
        try:
            fut.result()
            print(f"[{_ts()}] Created topic {t}")
        except Exception as exc:  # noqa: BLE001
            if "already exists" in str(exc).lower():
                print(f"[{_ts()}] Topic {t} already exists — continuing")
            else:
                raise


def set_share_group_offset_reset(admin, group, reset):
    """
    Set share.auto.offset.reset on the share group (KIP-932).
    """
    res = ConfigResource(
        ResourceType.GROUP,
        group,
        incremental_configs=[
            ConfigEntry('share.auto.offset.reset', reset,
                        incremental_operation=AlterConfigOpType.SET),
        ],
    )
    try:
        for fut in admin.incremental_alter_configs([res]).values():
            fut.result()
        print(f"[{_ts()}] Set share.auto.offset.reset={reset} on group {group}")
    except Exception as exc:  # noqa: BLE001
        # Most likely the identity pool lacks ALTER on the group. Without this
        # the group defaults to "latest" and will consume 0.
        sys.stderr.write(
            f"[{_ts()}] WARNING: could not set share.auto.offset.reset={reset} "
            f"on group {group}: {exc}\n"
            f"  Grant ALTER on the group to the identity pool, or set the broker "
            f"default group.share.auto.offset.reset=earliest.\n")


def produce_messages(producer, topic, count):
    """Produce `count` JSON messages; return the number confirmed delivered."""
    delivered = [0]

    def on_delivery(err, msg):
        if err is not None:
            print(f"[{_ts()}] Delivery failed: {err}")
        else:
            delivered[0] += 1

    for i in range(count):
        payload = {
            'timestamp': _ts(),
            'message_number': i + 1,
            'value': random.randint(1, 100),
            'source': 'aws-iam-share-roundtrip',
        }
        producer.produce(topic, key=f'key-{i}', value=json.dumps(payload),
                         callback=on_delivery)
        producer.poll(0)

    producer.flush(30)
    print(f"[{_ts()}] Produced {delivered[0]}/{count} messages")
    return delivered[0]


def main():
    # Env var names mirror the autowire harness (opt_in_success.py) so this
    # scenario can run with the same environment/values.
    bootstrap = sys.argv[1] if len(sys.argv) > 1 else os.environ.get('BOOTSTRAP_SERVERS')
    logical_cluster = os.environ.get('KAFKA_LOGICAL_CLUSTER')
    identity_pool_id = os.environ.get('IDENTITY_POOL_ID')
    audience = os.environ.get('OIDC_AUDIENCE', 'https://confluent.cloud/oidc')
    aws_region = os.environ.get('AWS_STS_REGION', 'us-east-2')
    topic = os.environ.get('KAFKA_TOPIC', 'share_aws_iam_demo')
    group = os.environ.get('GROUP_ID', f'share-aws-iam-{uuid.uuid4().hex[:8]}')
    message_count = int(os.environ.get('MESSAGE_COUNT', '20'))
    # 'share' (default) uses a KIP-932 ShareConsumer; 'regular' uses a classic
    # Consumer, whose start offset is a client-side config (auto.offset.reset),
    # so it needs no group-level ALTER — useful when the identity pool can't set
    # share.auto.offset.reset on the group.
    consumer_kind = os.environ.get('CONSUMER_KIND', 'share').lower()

    missing = [k for k, v in (('BOOTSTRAP_SERVERS', bootstrap),
                              ('KAFKA_LOGICAL_CLUSTER', logical_cluster),
                              ('IDENTITY_POOL_ID', identity_pool_id)) if not v]
    if missing:
        sys.stderr.write(f"Missing required config: {', '.join(missing)}\n")
        return 2

    print("=" * 70)
    print(f"Confluent Cloud round-trip with AWS IAM auth (consumer={consumer_kind})")
    print(f"  bootstrap={bootstrap}  cluster={logical_cluster}  pool={identity_pool_id}")
    print(f"  region={aws_region}  audience={audience}")
    print(f"  topic={topic}  group={group}  messages={message_count}")
    print("=" * 70)

    oauth_config = {
        'logical_cluster': logical_cluster,
        'identity_pool_id': identity_pool_id,
        'audience': audience,
        'aws_region': aws_region,
    }
    oauth_cb = make_oauth_cb(oauth_config)
    base = common_config(bootstrap, oauth_cb)

    # 1. Ensure the topic exists.
    admin = AdminClient(dict(base))
    ensure_topic(admin, topic)

    # 2. For a share group, pin its broker-side start offset to earliest before
    #    the consumer joins. (A regular consumer controls this client-side via
    #    auto.offset.reset, so it needs no group ALTER.)
    if consumer_kind == 'share':
        set_share_group_offset_reset(admin, group, 'earliest')

    # 3. Produce.
    producer = Producer(dict(base, **{'client.id': 'aws-iam-producer', 'acks': 'all'}))
    produced = produce_messages(producer, topic, message_count)

    # 4. Consume from the start.
    consumed = 0
    if consumer_kind == 'share':
        sc = ShareConsumer(dict(base, **{'group.id': group,
                                         'client.id': 'aws-iam-share-consumer'}))
        try:
            sc.subscribe([topic])
            print(f"[{_ts()}] Consuming via ShareConsumer "
                  f"(share.auto.offset.reset=earliest)...")
            deadline = time.time() + 60
            while consumed < produced and time.time() < deadline:
                for msg in sc.poll(timeout=1.0):
                    if msg.error() is not None:
                        sys.stderr.write(f"%% per-message error: {msg.error()}\n")
                        continue
                    consumed += 1
                    print(f"[{_ts()}] consumed {msg.topic()}[{msg.partition()}]"
                          f"@{msg.offset()}: {msg.value().decode()}")
        except KafkaException as exc:
            sys.stderr.write(f"%% Kafka error (auth or connectivity?): {exc}\n")
        finally:
            sc.close()
    else:
        # Regular consumer: start offset is a client-side config, so no group
        # ALTER is needed.
        c = Consumer(dict(base, **{'group.id': group,
                                   'client.id': 'aws-iam-consumer',
                                   'auto.offset.reset': 'earliest'}))
        try:
            c.subscribe([topic])
            print(f"[{_ts()}] Consuming via regular Consumer "
                  f"(auto.offset.reset=earliest)...")
            deadline = time.time() + 60
            while consumed < produced and time.time() < deadline:
                msg = c.poll(timeout=1.0)
                if msg is None:
                    continue
                if msg.error() is not None:
                    sys.stderr.write(f"%% per-message error: {msg.error()}\n")
                    continue
                consumed += 1
                print(f"[{_ts()}] consumed {msg.topic()}[{msg.partition()}]"
                      f"@{msg.offset()}: {msg.value().decode()}")
        except KafkaException as exc:
            sys.stderr.write(f"%% Kafka error (auth or connectivity?): {exc}\n")
        finally:
            c.close()

    ok = consumed >= message_count
    print("=" * 70)
    print(f"{'PASS' if ok else 'FAIL'} — produced={produced} consumed={consumed} "
          f"(round-trip via AWS STS GetWebIdentityToken)")
    print("=" * 70)
    return 0 if ok else 1


if __name__ == '__main__':
    sys.exit(main())
