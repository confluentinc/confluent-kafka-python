#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Consumer role for the demo. Reads User records and deserializes them, optionally
# resolving every record against a chosen reader schema.
#
#     python consumer.py                  # read using each record's WRITER schema
#     python consumer.py --schema v2      # read everything THROUGH the v2 schema:
#                                         #   old v1 records come back with email='' (default filled in)
#     python consumer.py -n 5             # read up to 5 records then stop
#     python consumer.py --loop           # keep polling forever (Ctrl-C to stop)
#
# A fresh consumer group is used on every run, so it always reads from the start
# of the topic.

import argparse
import time
from uuid import uuid4

from confluent_kafka import Consumer
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext

from _common import BROKERS, SCHEMAS, STRATEGY_CONF, TOPIC, schema_registry_client


def main():
    parser = argparse.ArgumentParser(description="Avro consumer for the Schema Registry demo")
    parser.add_argument(
        "--schema",
        choices=["v1", "v2"],
        default=None,
        help="reader schema to resolve against; default: use each record's writer schema",
    )
    parser.add_argument("-n", "--count", type=int, default=10, help="max records to read before stopping")
    parser.add_argument("--timeout", type=float, default=15.0, help="give up after this many seconds")
    parser.add_argument("--loop", action="store_true", help="poll forever until Ctrl-C (ignores --count/--timeout)")
    args = parser.parse_args()

    sr = schema_registry_client()
    reader_schema = SCHEMAS[args.schema] if args.schema else None
    deserializer = AvroDeserializer(sr, reader_schema, conf=dict(STRATEGY_CONF))

    consumer = Consumer(
        {
            "bootstrap.servers": BROKERS,
            "group.id": f"sr-demo-{uuid4()}",  # fresh group => always read from the start
            "auto.offset.reset": "earliest",
        }
    )
    consumer.subscribe([TOPIC])
    print(
        f"reading from '{TOPIC}'"
        + (f" through reader schema {args.schema}" if args.schema else " (writer schema)")
        + (" — polling forever, Ctrl-C to stop" if args.loop else "")
    )

    ctx = SerializationContext(TOPIC, MessageField.VALUE)
    seen = 0
    deadline = None if args.loop else time.time() + args.timeout
    try:
        while args.loop or (seen < args.count and time.time() < deadline):
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print("  consumer error:", msg.error())
                continue
            record = deserializer(msg.value(), ctx)
            seen += 1
            print(f"  [{seen}] {record}")
    except KeyboardInterrupt:
        print(f"\nstopping after {seen} record(s)...")
    finally:
        consumer.close()

    if seen == 0:
        print("  (no records — has the producer run yet?)")


if __name__ == "__main__":
    main()
