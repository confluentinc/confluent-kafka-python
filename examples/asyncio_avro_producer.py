#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright 2025 Confluent Inc.
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

# A minimal example demonstrating the AsyncIO Avro producer with Schema Registry.
#
# The AsyncAvroSerializerBuilder is handed to AsyncSerializingProducer through
# the 'value.serializer.builder' configuration property; the producer then
# constructs the Schema Registry client and the serializer, gives them the
# Kafka cluster id when they need it, and closes them along with itself.
# produce() is called with the value object itself rather than with bytes.

import argparse
import asyncio

from confluent_kafka.aio import AsyncSerializingProducer
from confluent_kafka.schema_registry.avro import AsyncAvroSerializerBuilder


async def main(args):
    # Configure the Schema Registry client the builder will create
    sr_conf = {'url': args.schema_registry}
    if args.sr_api_key and args.sr_api_secret:
        sr_conf['basic.auth.user.info'] = f"{args.sr_api_key}:{args.sr_api_secret}"

    # Example Avro schema
    schema_str = '{"type": "record", "name": "User", "fields": [{"name": "name", "type": "string"}]}'

    # Construction is asynchronous, as the builder's serializer is
    producer = await AsyncSerializingProducer(
        {
            'bootstrap.servers': args.bootstrap_servers,
            'value.serializer.builder': AsyncAvroSerializerBuilder(schema_registry_config=sr_conf, schema=schema_str),
        }
    )

    try:
        # The value is serialized by the producer before being queued
        delivery_future = await producer.produce(args.topic, value={'name': 'alice'})
        await producer.flush()
        msg = await delivery_future
        print(f"Produced to {msg.topic()} [{msg.partition()}] @ {msg.offset()}")
    finally:
        # Closes the producer, then the serializer and its Schema Registry client
        await producer.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='AsyncIO Avro producer example')
    parser.add_argument('-b', dest='bootstrap_servers', required=True, help='Bootstrap broker(s) (host[:port])')
    parser.add_argument('-s', dest='schema_registry', required=True, help='Schema Registry (http(s)://host[:port])')
    parser.add_argument('--sr-api-key', dest='sr_api_key', default=None, help='Confluent Cloud SR API key (optional)')
    parser.add_argument(
        '--sr-api-secret', dest='sr_api_secret', default=None, help='Confluent Cloud SR API secret (optional)'
    )
    parser.add_argument('-t', dest='topic', default='example_asyncio_avro', help='Topic name')
    args = parser.parse_args()

    asyncio.run(main(args))
