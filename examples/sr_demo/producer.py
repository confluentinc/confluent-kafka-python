#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Producer role for the demo. Serializes a User record with Avro + Schema Registry
# framing and sends it to Kafka.
#
#     python producer.py                              # send one v1 record and exit
#     python producer.py --schema v2                  # send a v2 record (includes 'email')
#     python producer.py --schema v2 --name Linus --email linus@example.com
#     python producer.py -n 3                          # send 3 records and exit
#     python producer.py --loop                        # stream records forever (Ctrl-C to stop)
#     python producer.py --loop --interval 0.5          # one record every 0.5s
#     python producer.py --no-auto-register            # require the schema to already be registered

import argparse
import time

from confluent_kafka import Producer
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext

from _common import BROKERS, SCHEMAS, STRATEGY_CONF, TOPIC, schema_registry_client


def delivery_report(err, msg):
    if err is not None:
        print(f"  delivery FAILED: {err}")
    else:
        print(f"  delivered to {msg.topic()} [partition {msg.partition()}] @ offset {msg.offset()}")


def main():
    parser = argparse.ArgumentParser(description="Avro producer for the Schema Registry demo")
    parser.add_argument("--schema", choices=["v1", "v2"], default="v1", help="schema version to serialize with")
    parser.add_argument("--name", default="Ada")
    parser.add_argument("--favorite-number", type=int, default=7)
    parser.add_argument("--favorite-color", default="green")
    parser.add_argument("--email", default="ada@example.com", help="only used with --schema v2")
    parser.add_argument("-n", "--count", type=int, default=1, help="number of records to send (ignored with --loop)")
    parser.add_argument("--loop", action="store_true", help="produce forever until Ctrl-C")
    parser.add_argument("--interval", type=float, default=1.0, help="seconds between records in --loop mode")
    parser.add_argument(
        "--no-auto-register",
        action="store_true",
        help="require the schema to already be registered (strict governance)",
    )
    args = parser.parse_args()

    sr = schema_registry_client()
    conf = dict(STRATEGY_CONF)
    conf["auto.register.schemas"] = not args.no_auto_register
    serializer = AvroSerializer(sr, SCHEMAS[args.schema], conf=conf)

    base = {
        "name": args.name,
        "favorite_number": args.favorite_number,
        "favorite_color": args.favorite_color,
    }
    if args.schema == "v2":
        base["email"] = args.email

    producer = Producer({"bootstrap.servers": BROKERS})
    ctx = SerializationContext(TOPIC, MessageField.VALUE)

    print(
        f"producing to '{TOPIC}' with schema {args.schema}"
        + (f", one every {args.interval}s until Ctrl-C" if args.loop else f" ({args.count} record(s))")
    )

    i = 0
    try:
        while args.loop or i < args.count:
            record = dict(base)
            # make each record distinct when streaming or sending more than one
            if args.loop or args.count > 1:
                record["name"] = f"{args.name}-{i}"
                record["favorite_number"] = args.favorite_number + i
            payload = serializer(record, ctx)
            if i == 0:
                print(f"  wire prefix = {payload[:5].hex()} (magic byte + 4-byte schema id)")
            print(f"  producing: {record}")
            producer.produce(TOPIC, value=payload, on_delivery=delivery_report)
            producer.poll(0)  # serve delivery callbacks without blocking
            i += 1
            if args.loop:
                time.sleep(args.interval)
    except KeyboardInterrupt:
        print(f"\nstopping after {i} record(s)...")
    finally:
        producer.flush()


if __name__ == "__main__":
    main()
