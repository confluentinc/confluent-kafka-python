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
    2. (ShareConsumer) subscribe + warm up so the share group's start offset
       is pinned to "latest" before we produce
    3. (Producer) produce N messages
    4. (ShareConsumer) poll the batch until all N are consumed or a deadline
       passes (implicit acknowledgement — each poll auto-acks the previous batch)

All three clients authenticate the same way: SASL_SSL / OAUTHBEARER with the
AWS-IAM oauth_cb. No Kafka API keys are involved.

This is meant to run on AWS compute (EC2/EKS/ECS) whose IAM role the Confluent
Cloud identity pool trusts, exactly like the AWS-IAM-Examples setup — boto3
picks up the role credentials automatically and the role only needs
sts:GetWebIdentityToken.

Config comes from env vars (mirrors the example's ConfigMap); bootstrap may
also be passed as argv[1]:

    BOOTSTRAP_SERVERS   Confluent Cloud bootstrap (host:9092)   [required]
    LOGICAL_CLUSTER     CC logical cluster id (lkc-xxxxx)       [required]
    AWS_ACCOUNT_ID      12-digit account id; the STS audience   [required]
    AWS_DEFAULT_REGION  STS region                              [default us-west-2]
    KAFKA_TOPIC         topic name                              [default share_aws_iam_demo]
    GROUP_ID            share group id                          [default share-aws-iam-<rand>]
    MESSAGE_COUNT       messages to round-trip                  [default 20]

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
from confluent_kafka import KafkaException, Producer, ShareConsumer
from confluent_kafka.admin import AdminClient, NewTopic


def _ts():
    return datetime.now(timezone.utc).isoformat()


def get_aws_iam_token(client_id, aws_region):
    """Mint a JWT via AWS STS GetWebIdentityToken.

    Lifted from AWS-IAM-Examples/producer.py. boto3 resolves the AWS
    credentials from the ambient role (EKS Pod Identity, EC2 instance
    profile, etc.); the role needs only sts:GetWebIdentityToken.

    :returns: (token_string, lifetime_seconds)
    """
    print(f"[{_ts()}] Requesting AWS IAM token...")
    sts_client = boto3.client('sts', region_name=aws_region)

    # Some accounts cap the session duration; fall back to shorter ones.
    for duration in (3600, 1800, 900):
        try:
            response = sts_client.get_web_identity_token(
                Audience=[client_id],
                SigningAlgorithm='ES384',
                DurationSeconds=duration,
            )
            print(f"[{_ts()}] Got token (expires in {duration}s)")
            return response['WebIdentityToken'], duration
        except sts_client.exceptions.SessionDurationEscalationException:
            print(f"[{_ts()}] {duration}s duration not allowed, trying shorter...")
            continue

    raise RuntimeError("Could not get AWS IAM token with any duration")


def make_oauth_cb(oauth_config):
    """Build the confluent-kafka oauth_cb closure.

    The callback returns the 4-tuple confluent-kafka expects:
    (token, absolute_expiry_epoch_seconds, principal, extensions_dict).
    The logicalCluster SASL extension routes the token to the right CC
    cluster. The same callback is used by the producer, consumer, and admin
    client; for a ShareConsumer it flows through the binding's oauth_cb
    handler, which sets the token on the share consumer's underlying client.
    """
    def oauth_cb(_config_str):
        token, lifetime = get_aws_iam_token(
            oauth_config['aws_account_id'], oauth_config['aws_region'])
        return (token,
                time.time() + lifetime,
                "",
                {"logicalCluster": oauth_config['logical_cluster']})

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
    bootstrap = sys.argv[1] if len(sys.argv) > 1 else os.environ.get('BOOTSTRAP_SERVERS')
    logical_cluster = os.environ.get('LOGICAL_CLUSTER')
    aws_account_id = os.environ.get('AWS_ACCOUNT_ID')
    aws_region = os.environ.get('AWS_DEFAULT_REGION', 'us-west-2')
    topic = os.environ.get('KAFKA_TOPIC', 'share_aws_iam_demo')
    group = os.environ.get('GROUP_ID', f'share-aws-iam-{uuid.uuid4().hex[:8]}')
    message_count = int(os.environ.get('MESSAGE_COUNT', '20'))

    missing = [k for k, v in (('BOOTSTRAP_SERVERS', bootstrap),
                              ('LOGICAL_CLUSTER', logical_cluster),
                              ('AWS_ACCOUNT_ID', aws_account_id)) if not v]
    if missing:
        sys.stderr.write(f"Missing required config: {', '.join(missing)}\n")
        return 2

    print("=" * 70)
    print("Confluent Cloud ShareConsumer round-trip with AWS IAM auth")
    print(f"  bootstrap={bootstrap}  cluster={logical_cluster}  region={aws_region}")
    print(f"  topic={topic}  group={group}  messages={message_count}")
    print("=" * 70)

    oauth_config = {
        'logical_cluster': logical_cluster,
        'aws_account_id': aws_account_id,
        'aws_region': aws_region,
    }
    oauth_cb = make_oauth_cb(oauth_config)
    base = common_config(bootstrap, oauth_cb)

    # 1. Ensure the topic exists.
    admin = AdminClient(dict(base))
    ensure_topic(admin, topic)
    sc_conf = dict(base, **{'group.id': group, 'client.id': 'aws-iam-share-consumer'})

    sc = ShareConsumer(sc_conf)
    consumed = 0
    try:
        sc.subscribe([topic])
        print(f"[{_ts()}] Warming up share consumer (joining group)...")
        warmup_deadline = time.time() + 8
        while time.time() < warmup_deadline:
            sc.poll(timeout=1.0) 
        producer = Producer(dict(base, **{'client.id': 'aws-iam-producer', 'acks': 'all'}))
        produced = produce_messages(producer, topic, message_count)
        print(f"[{_ts()}] Consuming via ShareConsumer...")
        deadline = time.time() + 60
        while consumed < produced and time.time() < deadline:
            messages = sc.poll(timeout=1.0)  
            for msg in messages:
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

    ok = consumed >= message_count
    print("=" * 70)
    print(f"{'PASS' if ok else 'FAIL'} — produced={message_count} consumed={consumed} "
          f"(round-trip via AWS STS GetWebIdentityToken)")
    print("=" * 70)
    return 0 if ok else 1


if __name__ == '__main__':
    sys.exit(main())
