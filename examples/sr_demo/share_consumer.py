#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# KIP-932 SHARE-consumer variant of consumer.py.
#
# A share consumer reads a topic like a QUEUE: many consumers in the same share
# group cooperatively consume the same partitions, and each record is
# acknowledged individually instead of committing partition offsets. This uses
# DeserializingShareConsumer, which deserializes each record's value with the
# Avro + Schema Registry deserializer inside poll().
#
#     python share_consumer.py                 # read using each record's writer schema
#     python share_consumer.py --schema v2      # resolve everything through v2 (old records get email='')
#     python share_consumer.py -n 2
#     python share_consumer.py --loop           # keep polling the queue forever (Ctrl-C to stop)
#
# ── REQUIREMENTS (this is NOT the published-package path) ─────────────────────
#   * The LOCAL KIP-932 build of confluent_kafka from this branch. ShareConsumer
#     is not in the published wheel installed in the .demo venv, so run this with
#     the repo's dev build, e.g.:
#         PYTHONPATH=/private/tmp/confluent-kafka-python/src python share_consumer.py
#     (after building the C extension against a librdkafka with KIP-932 support).
#   * A broker with SHARE GROUPS ENABLED (e.g. group.share.enable=true). The slim
#     docker-compose.demo.yaml does not enable them by default.

import argparse
import time

from confluent_kafka import DeserializingShareConsumer
from confluent_kafka.schema_registry.avro import AvroDeserializer

from _common import BROKERS, SCHEMAS, STRATEGY_CONF, TOPIC, schema_registry_client


def main():
    parser = argparse.ArgumentParser(description="KIP-932 share consumer for the Schema Registry demo")
    parser.add_argument(
        "--schema",
        choices=["v1", "v2"],
        default=None,
        help="reader schema to resolve against; default: each record's writer schema",
    )
    parser.add_argument("--group", default="sr-demo-share", help="share group id")
    parser.add_argument("-n", "--count", type=int, default=10, help="max records to read before stopping")
    parser.add_argument("--timeout", type=float, default=15.0, help="give up after this many seconds")
    parser.add_argument("--loop", action="store_true", help="poll forever until Ctrl-C (ignores --count/--timeout)")
    args = parser.parse_args()

    sr = schema_registry_client()
    reader_schema = SCHEMAS[args.schema] if args.schema else None
    value_deserializer = AvroDeserializer(sr, reader_schema, conf=dict(STRATEGY_CONF))

    consumer = DeserializingShareConsumer(
        {
            "bootstrap.servers": BROKERS,
            "group.id": args.group,  # a SHARE group (queue semantics), not a classic consumer group
            "value.deserializer": value_deserializer,
        }
    )
    consumer.subscribe([TOPIC])
    print(
        f"share-consuming '{TOPIC}' in share group '{args.group}'"
        + (f" through reader schema {args.schema}" if args.schema else " (writer schema)")
        + (" — polling forever, Ctrl-C to stop" if args.loop else "")
    )

    seen = 0
    deadline = None if args.loop else time.time() + args.timeout
    try:
        while args.loop or (seen < args.count and time.time() < deadline):
            # poll() returns a LIST (a batch) — unlike the classic Consumer's single message.
            for msg in consumer.poll(1.0):
                if msg.error():
                    # Includes broker errors AND per-record deserialization failures
                    # (raw bytes are preserved). In a real queue you'd REJECT these
                    # via consumer.acknowledge(msg, AcknowledgeType.REJECT).
                    print("  record error:", msg.error())
                    continue
                seen += 1
                print(f"  [{seen}] {msg.value()}")
            # The next poll() implicitly acknowledges the batch above. If we crashed
            # first, the broker would redeliver those records to another consumer in
            # the share group once the acquisition lock expires.
    except KeyboardInterrupt:
        print(f"\nstopping after {seen} record(s)...")
    finally:
        consumer.close()

    if seen == 0:
        print("  (no records — has the producer run, and are share groups enabled on the broker?)")


if __name__ == "__main__":
    main()
