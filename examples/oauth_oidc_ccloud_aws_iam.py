#!/usr/bin/env python
# -*- coding: utf-8 -*-
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

"""End-to-end example for AWS IAM OAUTHBEARER authentication.

Activation is config-only: setting
``sasl.oauthbearer.metadata.authentication.type=aws_iam`` is enough.

Install:
    pip install 'confluent-kafka[oauthbearer-aws]'

Runs on AWS compute (EC2 / EKS / ECS / Fargate / Lambda) with an IAM role
attached — boto3's default credential chain resolves it, no static keys.

To run:
    python oauth_oidc_ccloud_aws_iam.py \\
        -b pkc-xxxx.aws.confluent.cloud:9092 \\
        --region us-east-1 \\
        --audience https://confluent.cloud/oidc \\
        --extensions logicalCluster=lkc-abc,identityPoolId=pool-xyz
"""

import argparse
import logging
import time
import uuid

from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.serialization import StringSerializer


def common_config(args):
    """SASL config shared by Producer, Consumer, and AdminClient."""
    conf = {
        'bootstrap.servers': args.bootstrap_servers,
        'security.protocol': 'SASL_SSL',
        'sasl.mechanisms': 'OAUTHBEARER',
        'sasl.oauthbearer.method': 'oidc',
        'sasl.oauthbearer.metadata.authentication.type': 'aws_iam',
        'sasl.oauthbearer.config': f'region={args.region} '
        f'audience={args.audience} '
        f'duration_seconds={args.duration_seconds}',
        'debug': 'security',
    }

    if args.extensions:
        conf['sasl.oauthbearer.extensions'] = args.extensions

    return conf


def consumer_config(args, group_id):
    cfg = common_config(args)
    cfg['group.id'] = group_id
    cfg['auto.offset.reset'] = 'earliest'
    cfg['enable.auto.offset.store'] = False  # commit offsets manually
    return cfg


def create_topic(admin_conf, topic_name, num_partitions=1, replication_factor=3):
    admin = AdminClient(admin_conf)
    futures = admin.create_topics(
        [
            NewTopic(topic_name, num_partitions=num_partitions, replication_factor=replication_factor),
        ]
    )
    for topic, future in futures.items():
        try:
            future.result()
            print(f"[admin] Topic '{topic}' created " f"({num_partitions} partition(s), RF={replication_factor})")
        except Exception as exc:
            print(f"[admin] Failed to create topic '{topic}': {exc}")
            raise


def delivery_report(err, msg):
    if err is not None:
        print(f"[producer] Delivery failed: {err}")
        return
    print(
        f"[producer] Produced to {msg.topic()} [{msg.partition()}] "
        f"at offset {msg.offset()}: {msg.value().decode('utf-8')}"
    )


def main(args):
    # Unique topic + group per run so the example is self-contained.
    topic_name = f"aws-iam-{uuid.uuid4()}"
    group_id = f"aws-iam-consumer-{uuid.uuid4()}"

    p_conf = common_config(args)
    c_conf = consumer_config(args, group_id)
    a_conf = common_config(args)

    logging.basicConfig(level=logging.INFO)

    print("\n=== AWS IAM OAUTHBEARER end-to-end example ===")
    print(f"bootstrap.servers:     {args.bootstrap_servers}")
    print(f"region:                {args.region}")
    print(f"audience:              {args.audience}")
    print(f"duration_seconds:      {args.duration_seconds} " f"(auto-refresh at ~{int(args.duration_seconds * 0.8)}s)")
    print(f"run-for:               {args.run_for}s")
    print(f"topic (generated):     {topic_name}")
    print(f"group.id (generated):  {group_id}\n")

    create_topic(a_conf, topic_name)

    producer = Producer(p_conf)
    consumer = Consumer(c_conf)
    consumer.subscribe([topic_name])
    serializer = StringSerializer('utf_8')

    start = time.time()
    end_at = start + args.run_for
    produced = 0
    consumed = 0

    print(
        f"[loop] Producing/consuming for {args.run_for}s — "
        f"watch the debug=security logs for token-refresh events.\n"
    )

    try:
        while time.time() < end_at:
            elapsed = time.time() - start
            msg = f"hello-from-aws-iam T+{elapsed:.1f}s"

            producer.produce(
                topic_name,
                value=serializer(msg),
                on_delivery=delivery_report,
            )
            producer.poll(0)
            produced += 1

            received = consumer.poll(1.0)
            if received is None:
                pass  # poll timeout, no message yet
            elif received.error() is not None:
                print(f"[consumer] error: {received.error()}")
            else:
                consumer.store_offsets(received)
                consumed += 1
                print(
                    f"[consumer] Received from "
                    f"{received.topic()} [{received.partition()}] "
                    f"at offset {received.offset()}: "
                    f"{received.value().decode('utf-8')}"
                )

            time.sleep(args.interval)
    except KeyboardInterrupt:
        print("\n[main] Interrupted — flushing.")
    finally:
        print(f"\n[summary] Produced {produced}, consumed {consumed} " f"in {time.time() - start:.1f}s. Flushing...")
        producer.flush(timeout=10)
        consumer.close()
        print("[summary] Done.")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='End-to-end OAUTHBEARER example via AWS IAM autowire ' '(produce + consume + admin).',
    )
    parser.add_argument('-b', dest='bootstrap_servers', required=True, help='Bootstrap broker(s) (host[:port])')
    parser.add_argument('--region', required=True, help='AWS region (e.g. us-east-1)')
    parser.add_argument(
        '--audience',
        required=True,
        help='OIDC audience claim the broker expects ' '(e.g. https://confluent.cloud/oidc)',
    )
    parser.add_argument(
        '--extensions',
        default=None,
        help='Optional sasl.oauthbearer.extensions value ' '(comma-separated key=value pairs)',
    )
    parser.add_argument(
        '--duration-seconds',
        dest='duration_seconds',
        type=int,
        default=60,
        help='STS DurationSeconds (default 60 = AWS minimum); ' 'librdkafka auto-refreshes at ~80%% of it.',
    )
    parser.add_argument(
        '--run-for', dest='run_for', type=int, default=120, help='Run duration in seconds (default 120).'
    )
    parser.add_argument('--interval', type=float, default=5.0, help='Seconds between produce calls (default 5).')

    main(parser.parse_args())
