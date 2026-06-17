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
"""Testcase-6: count OAUTHBEARER token refresh / reauth cycles (AWS IAM, KIP-932).

Long-running soak. The oauth_cb reports a short token lifetime
(REAUTH_INTERVAL_SECONDS, default 75s -> librdkafka refreshes ~every 60s), and a
share consumer is kept live for RUN_SECONDS (default 300s) while a trickle of
messages is produced and consumed. We count how many times the consumer's token
is refreshed/reauthenticated and confirm consumption keeps working throughout.

PASS = multiple refresh cycles occurred, messages kept being consumed across
them, and no authentication error was raised.

Env (same required vars as Testcase-1/2) plus:
    REAUTH_INTERVAL_SECONDS  reported token lifetime (refresh ~80% of it)  [default 75]
    RUN_SECONDS              how long to run the soak                      [default 300]

Usage:
    python Testcase-6_reauth_refresh_cycles_aws_iam.py [bootstrap.servers]
"""

import json
import os
import sys
import time
import uuid
from datetime import datetime, timezone

import boto3
from confluent_kafka import KafkaException, Producer, ShareConsumer
from confluent_kafka.admin import (
    AdminClient,
    AlterConfigOpType,
    ConfigEntry,
    ConfigResource,
    NewTopic,
    ResourceType,
)

STS_DURATION_SECONDS = 300


def _ts():
    return datetime.now(timezone.utc).isoformat()


def get_aws_iam_token(audience, aws_region):
    sts = boto3.client('sts', region_name=aws_region)
    return sts.get_web_identity_token(
        Audience=[audience], SigningAlgorithm='ES384',
        DurationSeconds=STS_DURATION_SECONDS)['WebIdentityToken']


def make_oauth_cb(cfg, stats, reported_lifetime):
    def oauth_cb(_config_str):
        token = get_aws_iam_token(cfg['audience'], cfg['aws_region'])
        stats['fetches'] += 1
        return (token, time.time() + reported_lifetime, "",
                {"logicalCluster": cfg['logical_cluster'],
                 "identityPoolId": cfg['identity_pool_id']})
    return oauth_cb


def common_config(bootstrap, oauth_cb):
    return {
        'bootstrap.servers': bootstrap,
        'security.protocol': 'SASL_SSL',
        'sasl.mechanisms': 'OAUTHBEARER',
        'oauth_cb': oauth_cb,
    }


def ensure_topic(admin, topic):
    for t, fut in admin.create_topics(
            [NewTopic(topic, num_partitions=1, replication_factor=3)]).items():
        try:
            fut.result()
            print(f"[{_ts()}] Created topic {t}")
        except Exception as exc:  # noqa: BLE001
            if "already exists" in str(exc).lower():
                print(f"[{_ts()}] Topic {t} already exists — continuing")
            else:
                raise


def set_share_group_offset_reset(admin, group, reset):
    res = ConfigResource(
        ResourceType.GROUP, group,
        incremental_configs=[
            ConfigEntry('share.auto.offset.reset', reset,
                        incremental_operation=AlterConfigOpType.SET)])
    for fut in admin.incremental_alter_configs([res]).values():
        fut.result()
    print(f"[{_ts()}] Set share.auto.offset.reset={reset} on group {group}")


def produce_batch(producer, topic, count, start):
    for i in range(count):
        producer.produce(topic, key=f'k{start + i}',
                         value=json.dumps({'n': start + i, 'ts': _ts()}))
    return count - producer.flush(15)


def main():
    bootstrap = sys.argv[1] if len(sys.argv) > 1 else os.environ.get('BOOTSTRAP_SERVERS')
    logical_cluster = os.environ.get('KAFKA_LOGICAL_CLUSTER')
    identity_pool_id = os.environ.get('IDENTITY_POOL_ID')
    audience = os.environ.get('OIDC_AUDIENCE', 'https://confluent.cloud/oidc')
    aws_region = os.environ.get('AWS_STS_REGION', 'eu-north-1')
    topic = os.environ.get('KAFKA_TOPIC', f'share_aws_iam_refresh_{uuid.uuid4().hex[:8]}')
    group = os.environ.get('GROUP_ID', f'share-aws-iam-refresh-{uuid.uuid4().hex[:8]}')
    token_lifetime = int(os.environ.get('REAUTH_INTERVAL_SECONDS', '75'))
    run_seconds = int(os.environ.get('RUN_SECONDS', '300'))

    missing = [k for k, v in (('BOOTSTRAP_SERVERS', bootstrap),
                              ('KAFKA_LOGICAL_CLUSTER', logical_cluster),
                              ('IDENTITY_POOL_ID', identity_pool_id)) if not v]
    if missing:
        sys.stderr.write(f"Missing required config: {', '.join(missing)}\n")
        return 2

    expected = int(run_seconds / (0.8 * token_lifetime)) if token_lifetime else 0
    print("=" * 70)
    print("Testcase-6: OAUTHBEARER token refresh / reauth cycle count (AWS IAM)")
    print(f"  cluster={logical_cluster}  pool={identity_pool_id}  region={aws_region}")
    print(f"  topic={topic}  group={group}")
    print(f"  reported_lifetime={token_lifetime}s  run={run_seconds}s  "
          f"~expected refresh cycles={expected}")
    print("=" * 70)

    cfg = {'logical_cluster': logical_cluster, 'identity_pool_id': identity_pool_id,
           'audience': audience, 'aws_region': aws_region}

    # Separate callback/counter for the consumer so we count only its refreshes.
    setup_cb = make_oauth_cb(cfg, {'fetches': 0}, token_lifetime)
    consumer_stats = {'fetches': 0}
    consumer_cb = make_oauth_cb(cfg, consumer_stats, token_lifetime)
    auth_errors = []

    admin = AdminClient(common_config(bootstrap, setup_cb))
    ensure_topic(admin, topic)
    set_share_group_offset_reset(admin, group, 'earliest')
    producer = Producer(dict(common_config(bootstrap, setup_cb),
                             **{'client.id': 'aws-iam-producer', 'acks': 'all'}))

    def on_error(err):
        s = str(err)
        if 'authentication' in s.lower() or 'sasl' in s.lower():
            auth_errors.append(s)

    sc = ShareConsumer(dict(common_config(bootstrap, consumer_cb),
                            **{'group.id': group,
                               'client.id': 'aws-iam-share-consumer',
                               'error_cb': on_error}))

    produced = 0
    consumed = 0
    baseline = None
    last_log = 0.0
    try:
        sc.subscribe([topic])
        deadline = time.time() + run_seconds
        while time.time() < deadline:
            produced += produce_batch(producer, topic, 3, produced)
            for msg in sc.poll(timeout=2.0):
                if msg.error() is None:
                    consumed += 1
            if baseline is None:
                baseline = consumer_stats['fetches']  # after the first poll/connect
            cycles = consumer_stats['fetches'] - (baseline or 0)
            now = time.time()
            if now - last_log >= 30:
                remaining = int(deadline - now)
                print(f"[{_ts()}] refresh cycles so far={cycles}  consumed={consumed}  "
                      f"(~{remaining}s left)")
                last_log = now
            time.sleep(10)
    except KafkaException as exc:
        sys.stderr.write(f"%% Kafka error: {exc}\n")
    finally:
        sc.close()

    cycles = consumer_stats['fetches'] - (baseline if baseline is not None
                                          else consumer_stats['fetches'])
    ok = cycles >= 2 and consumed > 0 and not auth_errors
    print("=" * 70)
    print(f"{'PASS' if ok else 'FAIL'} — refresh_cycles={cycles} (~expected {expected}) "
          f"consumed={consumed} produced={produced} auth_errors={len(auth_errors)}")
    if auth_errors:
        print(f"  auth errors: {auth_errors[:3]}")
    print("=" * 70)
    return 0 if ok else 1


if __name__ == '__main__':
    sys.exit(main())
