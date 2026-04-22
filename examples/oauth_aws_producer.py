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
#
# OAUTHBEARER example using AWS IAM Outbound Identity Federation
# (sts:GetWebIdentityToken, GA 2025-11-19).
#
# Prerequisites:
#   pip install 'confluent-kafka[oauthbearer-aws]'
#
#   AWS credentials available via the standard boto3 chain
#   (env vars, EKS IRSA, ECS, IMDSv2, shared profile, SSO, ...).
#
#   The AWS account must have run `aws iam enable-outbound-web-identity-federation`
#   once, and the principal must have `sts:GetWebIdentityToken` permission.
#
# Do NOT set `sasl.oauthbearer.method` — that selects the librdkafka-native
# path and bypasses the oauth_cb callback.

import argparse
import logging

from confluent_kafka import Producer
from confluent_kafka.oauthbearer.aws import AwsOAuthConfig, AwsStsTokenProvider
from confluent_kafka.serialization import StringSerializer


def producer_config(args):
    provider = AwsStsTokenProvider(
        AwsOAuthConfig(
            region=args.region,
            audience=args.audience,
            duration_seconds=args.duration,
        )
    )
    return {
        "bootstrap.servers": args.bootstrap_servers,
        "security.protocol": "SASL_SSL",
        "sasl.mechanisms": "OAUTHBEARER",
        "oauth_cb": provider.token,
        "logger": logging.getLogger(__name__),
    }


def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed for record {msg.key()}: {err}")
        return
    print(
        f"Record {msg.key()} delivered to {msg.topic()} "
        f"[{msg.partition()}] at offset {msg.offset()}"
    )


def main(args):
    producer = Producer(producer_config(args))
    serializer = StringSerializer("utf_8")
    print(f"Producing records to topic {args.topic}. ^C to exit.")
    try:
        while True:
            producer.poll(0.0)
            line = input("> ")
            parts = line.split(args.delimiter)
            if len(parts) == 2:
                producer.produce(
                    topic=args.topic,
                    key=serializer(parts[0]),
                    value=serializer(parts[1]),
                    on_delivery=delivery_report,
                )
            else:
                producer.produce(
                    topic=args.topic,
                    value=serializer(parts[0]),
                    on_delivery=delivery_report,
                )
    except KeyboardInterrupt:
        pass
    finally:
        print(f"\nFlushing {len(producer)} records...")
        producer.flush()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="OAUTHBEARER producer example using AWS STS GetWebIdentityToken."
    )
    parser.add_argument(
        "-b", dest="bootstrap_servers", required=True,
        help="Bootstrap broker(s) (host[:port])",
    )
    parser.add_argument(
        "-t", dest="topic", default="example_aws_oauth_producer", help="Topic name",
    )
    parser.add_argument(
        "-d", dest="delimiter", default="|",
        help="Key|Value delimiter (default '|')",
    )
    parser.add_argument(
        "--region", required=True,
        help="AWS region of the STS endpoint (e.g. 'us-east-1').",
    )
    parser.add_argument(
        "--audience", required=True,
        help="OIDC audience claim the relying party (broker) expects.",
    )
    parser.add_argument(
        "--duration", type=int, default=3600,
        help="STS token lifetime in seconds (60-3600, default 3600).",
    )
    main(parser.parse_args())
