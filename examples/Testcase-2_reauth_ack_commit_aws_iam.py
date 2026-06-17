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
"""Testcase-2: ShareConsumer ack/commit across SASL reauth (AWS IAM, KIP-932).

Validates the fix in the closed PR "Let share ops through during SASL reauth":

    consume records -> reauth happens -> acknowledge + commit those records
    -> the ack/commit must still succeed.

We force frequent reauth from the application side: the oauth_cb reports a SHORT
token lifetime (REAUTH_INTERVAL_SECONDS, default 30s) even though the STS token
is actually valid for 300s, so librdkafka refreshes the token — and
re-authenticates the long-lived producer / share-consumer connections —
repeatedly. Throughout the run we produce, share-consume in EXPLICIT
acknowledgement mode, acknowledge(ACCEPT), and commit_sync, asserting that every
commit succeeds and that at least one token refresh/reauth occurred.

(The reported lifetime drives client-side token refresh; the on-the-wire SASL
reauthentication cadence is the broker's connections.max.reauth.ms, which isn't
ours to set on CC — so run long enough, via RUN_SECONDS, to span it.)

Env (same required vars as Testcase-1) plus:
    REAUTH_INTERVAL_SECONDS  reported token lifetime; forces refresh  [default 30]
    RUN_SECONDS              max time to run the consume loop         [default 150]
    MESSAGE_COUNT            messages produced up front               [default 60]

Run on AWS compute whose IAM role the CC identity pool trusts.

Usage:
    python Testcase-2_reauth_ack_commit_aws_iam.py [bootstrap.servers]
"""

import json
import os
import sys
import time
import uuid
from datetime import datetime, timezone

import boto3
from confluent_kafka import AcknowledgeType, KafkaException, Producer, ShareConsumer
from confluent_kafka.admin import (
    AdminClient,
    AlterConfigOpType,
    ConfigEntry,
    ConfigResource,
    NewTopic,
    ResourceType,
)

# Max the CC identity-pool role allows; longer is rejected with AccessDenied.
STS_DURATION_SECONDS = 300


def _ts():
    return datetime.now(timezone.utc).isoformat()


def get_aws_iam_token(audience, aws_region):
    """Mint a JWT via AWS STS GetWebIdentityToken (ambient EC2 role creds)."""
    sts_client = boto3.client('sts', region_name=aws_region)
    resp = sts_client.get_web_identity_token(
        Audience=[audience],
        SigningAlgorithm='ES384',
        DurationSeconds=STS_DURATION_SECONDS,
    )
    return resp['WebIdentityToken']


def make_oauth_cb(cfg, stats, reported_lifetime):
    """DEFAULT-mode callback that reports a short lifetime to force reauth.

    The STS token is valid for STS_DURATION_SECONDS, but we tell librdkafka it
    expires in `reported_lifetime`, so it refreshes (and reauthenticates) sooner.
    """
    def oauth_cb(_config_str):
        token = get_aws_iam_token(cfg['audience'], cfg['aws_region'])
        stats['fetches'] += 1
        print(f"[{_ts()}] oauth_cb fired (token fetch #{stats['fetches']}); "
              f"reporting {reported_lifetime}s lifetime")
        return (token,
                time.time() + reported_lifetime,
                "",
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
    """Produce `count` messages; return the number confirmed delivered."""
    for i in range(count):
        producer.produce(topic, key=f'k{start + i}',
                         value=json.dumps({'n': start + i, 'ts': _ts()}))
    remaining = producer.flush(15)
    return count - remaining


def drain_and_commit(sc):
    """Poll once, ACCEPT every record, commit_sync; return (consumed, errors)."""
    batch = [m for m in sc.poll(timeout=2.0) if m.error() is None]
    if not batch:
        return 0, 0
    for msg in batch:
        sc.acknowledge(msg, AcknowledgeType.ACCEPT)
    result = sc.commit_sync(timeout=10.0)
    errs = [e for e in result.values() if e is not None]
    if errs:
        sys.stderr.write(f"[{_ts()}] commit_sync errors: {errs}\n")
    return len(batch), len(errs)


def main():
    bootstrap = sys.argv[1] if len(sys.argv) > 1 else os.environ.get('BOOTSTRAP_SERVERS')
    logical_cluster = os.environ.get('KAFKA_LOGICAL_CLUSTER')
    identity_pool_id = os.environ.get('IDENTITY_POOL_ID')
    audience = os.environ.get('OIDC_AUDIENCE', 'https://confluent.cloud/oidc')
    aws_region = os.environ.get('AWS_STS_REGION', 'eu-north-1')
    topic = os.environ.get('KAFKA_TOPIC', f'share_aws_iam_reauth_{uuid.uuid4().hex[:8]}')
    group = os.environ.get('GROUP_ID', f'share-aws-iam-reauth-{uuid.uuid4().hex[:8]}')
    reauth_interval = int(os.environ.get('REAUTH_INTERVAL_SECONDS', '30'))
    run_seconds = int(os.environ.get('RUN_SECONDS', '150'))
    message_count = int(os.environ.get('MESSAGE_COUNT', '60'))

    missing = [k for k, v in (('BOOTSTRAP_SERVERS', bootstrap),
                              ('KAFKA_LOGICAL_CLUSTER', logical_cluster),
                              ('IDENTITY_POOL_ID', identity_pool_id)) if not v]
    if missing:
        sys.stderr.write(f"Missing required config: {', '.join(missing)}\n")
        return 2

    print("=" * 70)
    print("Testcase-2: ShareConsumer ack/commit across SASL reauth (AWS IAM)")
    print(f"  cluster={logical_cluster}  pool={identity_pool_id}  region={aws_region}")
    print(f"  topic={topic}  group={group}")
    print(f"  reauth_interval={reauth_interval}s  run={run_seconds}s  messages={message_count}")
    print("=" * 70)

    stats = {'fetches': 0}
    cfg = {'logical_cluster': logical_cluster, 'identity_pool_id': identity_pool_id,
           'audience': audience, 'aws_region': aws_region}
    base = common_config(bootstrap, make_oauth_cb(cfg, stats, reauth_interval))

    admin = AdminClient(dict(base))
    ensure_topic(admin, topic)
    set_share_group_offset_reset(admin, group, 'earliest')

    # Produce the whole batch up front.
    producer = Producer(dict(base, **{'client.id': 'aws-iam-producer', 'acks': 'all'}))
    produced = produce_batch(producer, topic, message_count, 0)
    print(f"[{_ts()}] Produced {produced}/{message_count} up front")

    sc = ShareConsumer(dict(base, **{'group.id': group,
                                     'client.id': 'aws-iam-share-consumer',
                                     'share.acknowledgement.mode': 'explicit'}))

    committed = 0
    commit_errors = 0
    fetches_before_loop = None
    try:
        sc.subscribe([topic])
        deadline = time.time() + run_seconds
        while time.time() < deadline:
            consumed, errs = drain_and_commit(sc)
            committed += consumed
            commit_errors += errs
            if fetches_before_loop is None:
                # Baseline after the consumer's first poll/token fetch; any later
                # fetch then counts as a genuine refresh/reauth.
                fetches_before_loop = stats['fetches']
            if consumed:
                print(f"[{_ts()}] committed {consumed} "
                      f"(total committed={committed}/{produced}, "
                      f"token fetches={stats['fetches']})")
            # Stop once everything is committed AND a refresh/reauth has occurred,
            # so at least one commit lands after a reauth.
            if committed >= produced and stats['fetches'] > fetches_before_loop:
                break
            time.sleep(2)
    except KafkaException as exc:
        sys.stderr.write(f"%% Kafka error: {exc}\n")
    finally:
        sc.close()

    reauth_observed = fetches_before_loop is not None and stats['fetches'] > fetches_before_loop
    ok = committed >= produced and commit_errors == 0 and reauth_observed
    print("=" * 70)
    print(f"{'PASS' if ok else 'FAIL'} — produced={produced} committed={committed} "
          f"commit_errors={commit_errors} token_fetches={stats['fetches']} "
          f"reauth_observed={reauth_observed}")
    print("=" * 70)
    return 0 if ok else 1


if __name__ == '__main__':
    sys.exit(main())
