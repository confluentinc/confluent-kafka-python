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
"""Testcase-7: KIP-932 ShareConsumer round-trip against Confluent Cloud with
OAUTHBEARER **OIDC** mode (built-in token fetch, client-credentials grant).

This is the OIDC counterpart of Testcase-1 (AWS IAM / DEFAULT mode). The crucial
difference: there is **no oauth_cb and no boto3**. With
sasl.oauthbearer.method=oidc, librdkafka itself POSTs to the OIDC token endpoint
(client-credentials grant), parses/caches/refreshes the JWT, and runs the SASL
handshake. confluent-kafka-python just passes the config straight through.

It does a full round-trip, exactly like Testcase-1:

    1. (AdminClient) ensure the topic exists
    2. (AdminClient) set share.auto.offset.reset=earliest on the share group
       (a per-group, broker-side config; the consumer's own auto.offset.reset
       does NOT control this) -- needs ResourceOwner on the group
    3. (Producer) produce N messages
    4. (ShareConsumer) subscribe and poll until all N are consumed (implicit ack)

All three clients use the same OIDC config.

Config comes from env vars (the clientSecret is a SECRET -- pass it via the
environment, never commit it):

    BOOTSTRAP_SERVERS      Confluent Cloud bootstrap (host:9092)  [required; or argv[1]]
    KAFKA_LOGICAL_CLUSTER  CC logical cluster id (lkc-xxxxx)      [required]
    IDENTITY_POOL_ID       CC identity pool id (pool-xxxxx)       [required]
    OIDC_CLIENT_ID         OAuth client id                        [required]
    OIDC_CLIENT_SECRET     OAuth client secret                    [required]
    OIDC_TOKEN_ENDPOINT    IdP token endpoint URL                 [required]
    OIDC_SCOPE             OAuth scope                            [optional]
    KAFKA_TOPIC            topic name                             [default share_oidc_<rand>]
    GROUP_ID              share group id                          [default share-oidc-<rand>]
    MESSAGE_COUNT         messages to round-trip                  [default 20]

Usage:
    python Testcase-7_basic_share_consumer_oidc.py [bootstrap.servers]
"""

import json
import os
import random
import sys
import time
import uuid
from datetime import datetime, timezone

from confluent_kafka import KafkaException, Producer, ShareConsumer
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


def common_config(cfg):
    """Base SASL_SSL / OAUTHBEARER-OIDC config shared by all three clients.

    No oauth_cb: librdkafka fetches and refreshes the token itself from the
    OIDC token endpoint using the client-credentials grant. The logicalCluster +
    identityPoolId extensions route the token to the right CC cluster + pool;
    here they are a config STRING (not a callback return value).
    """
    conf = {
        'bootstrap.servers': cfg['bootstrap'],
        'security.protocol': 'SASL_SSL',
        'sasl.mechanisms': 'OAUTHBEARER',
        'sasl.oauthbearer.method': 'oidc',
        'sasl.oauthbearer.client.id': cfg['client_id'],
        'sasl.oauthbearer.client.secret': cfg['client_secret'],
        'sasl.oauthbearer.token.endpoint.url': cfg['token_endpoint'],
        'sasl.oauthbearer.extensions':
            f"logicalCluster={cfg['logical_cluster']},"
            f"identityPoolId={cfg['identity_pool_id']}",
        # Quiet the KIP-714 client-telemetry INFO logs.
        'enable.metrics.push': False,
    }
    if cfg.get('scope'):
        conf['sasl.oauthbearer.scope'] = cfg['scope']
    return conf


def ensure_topic(admin, topic):
    """Create the topic if it doesn't already exist (CC disables auto-create)."""
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
    """Set share.auto.offset.reset on the share group (KIP-932, broker-side)."""
    res = ConfigResource(
        ResourceType.GROUP, group,
        incremental_configs=[
            ConfigEntry('share.auto.offset.reset', reset,
                        incremental_operation=AlterConfigOpType.SET)])
    try:
        for fut in admin.incremental_alter_configs([res]).values():
            fut.result()
        print(f"[{_ts()}] Set share.auto.offset.reset={reset} on group {group}")
    except Exception as exc:  # noqa: BLE001
        sys.stderr.write(
            f"[{_ts()}] WARNING: could not set share.auto.offset.reset={reset} "
            f"on group {group}: {exc}\n"
            f"  Grant ResourceOwner on the group to the identity pool, or set the "
            f"broker default group.share.auto.offset.reset=earliest.\n")


def produce_messages(producer, topic, count):
    """Produce `count` JSON messages; return the number confirmed delivered."""
    delivered = [0]

    def on_delivery(err, msg):
        if err is not None:
            print(f"[{_ts()}] Delivery failed: {err}")
        else:
            delivered[0] += 1

    for i in range(count):
        payload = {'timestamp': _ts(), 'message_number': i + 1,
                   'value': random.randint(1, 100), 'source': 'oidc-share-roundtrip'}
        producer.produce(topic, key=f'key-{i}', value=json.dumps(payload),
                         callback=on_delivery)
        producer.poll(0)

    producer.flush(30)
    print(f"[{_ts()}] Produced {delivered[0]}/{count} messages")
    return delivered[0]


def main():
    bootstrap = sys.argv[1] if len(sys.argv) > 1 else os.environ.get('BOOTSTRAP_SERVERS')
    cfg = {
        'bootstrap': bootstrap,
        'logical_cluster': os.environ.get('KAFKA_LOGICAL_CLUSTER'),
        'identity_pool_id': os.environ.get('IDENTITY_POOL_ID'),
        'client_id': os.environ.get('OIDC_CLIENT_ID'),
        'client_secret': os.environ.get('OIDC_CLIENT_SECRET'),
        'token_endpoint': os.environ.get('OIDC_TOKEN_ENDPOINT'),
        'scope': os.environ.get('OIDC_SCOPE'),
    }
    topic = os.environ.get('KAFKA_TOPIC', f'share_oidc_{uuid.uuid4().hex[:8]}')
    group = os.environ.get('GROUP_ID', f'share-oidc-{uuid.uuid4().hex[:8]}')
    message_count = int(os.environ.get('MESSAGE_COUNT', '20'))

    missing = [k for k, v in (('BOOTSTRAP_SERVERS', bootstrap),
                              ('KAFKA_LOGICAL_CLUSTER', cfg['logical_cluster']),
                              ('IDENTITY_POOL_ID', cfg['identity_pool_id']),
                              ('OIDC_CLIENT_ID', cfg['client_id']),
                              ('OIDC_CLIENT_SECRET', cfg['client_secret']),
                              ('OIDC_TOKEN_ENDPOINT', cfg['token_endpoint'])) if not v]
    if missing:
        sys.stderr.write(f"Missing required config: {', '.join(missing)}\n")
        return 2

    print("=" * 70)
    print("Confluent Cloud ShareConsumer round-trip with OAUTHBEARER OIDC mode")
    print(f"  bootstrap={bootstrap}  cluster={cfg['logical_cluster']}  "
          f"pool={cfg['identity_pool_id']}")
    print(f"  token_endpoint={cfg['token_endpoint']}  scope={cfg['scope']}")
    print(f"  topic={topic}  group={group}  messages={message_count}")
    print("=" * 70)

    base = common_config(cfg)
    admin = AdminClient(dict(base))
    ensure_topic(admin, topic)
    set_share_group_offset_reset(admin, group, 'earliest')

    producer = Producer(dict(base, **{'client.id': 'oidc-producer', 'acks': 'all'}))
    produced = produce_messages(producer, topic, message_count)

    consumed = 0
    sc = ShareConsumer(dict(base, **{'group.id': group,
                                     'client.id': 'oidc-share-consumer'}))
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

    ok = consumed >= message_count
    print("=" * 70)
    print(f"{'PASS' if ok else 'FAIL'} — produced={produced} consumed={consumed} "
          f"(round-trip via OAUTHBEARER OIDC / client-credentials)")
    print("=" * 70)
    return 0 if ok else 1


if __name__ == '__main__':
    sys.exit(main())
