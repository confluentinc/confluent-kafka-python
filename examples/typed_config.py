#!/usr/bin/env python
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
#

#
# Example demonstrating type-safe configuration for Producer and Consumer
# using TypedDict. This provides IDE autocompletion, type checking, and
# documentation for commonly used configuration properties.
#
# For the full list of configuration properties, see:
# https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md
#
# Usage:
#   python typed_config.py <bootstrap-brokers> <topic>
#

import sys
from typing import Callable, List, Literal, Optional, Union

try:
    from typing import TypedDict
except ImportError:
    from typing_extensions import TypedDict

from confluent_kafka import Consumer, KafkaError, KafkaException, Producer


class CommonConfig(TypedDict, total=False):
    """Configuration properties common to both Producer and Consumer.

    All properties use the librdkafka dot-notation naming convention.
    For the full list, see:
    https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md
    """

    # Connection
    bootstrap_servers: str
    client_id: str
    metadata_max_age_ms: int
    socket_timeout_ms: int
    connections_max_idle_ms: int

    # Authentication
    security_protocol: Literal['plaintext', 'ssl', 'sasl_plaintext', 'sasl_ssl']
    sasl_mechanism: Literal['GSSAPI', 'PLAIN', 'SCRAM-SHA-256', 'SCRAM-SHA-512', 'OAUTHBEARER']
    sasl_username: str
    sasl_password: str

    # SSL
    ssl_ca_location: str
    ssl_certificate_location: str
    ssl_key_location: str
    ssl_key_password: str

    # Callbacks
    error_cb: Callable
    stats_cb: Callable
    throttle_cb: Callable

    # Debug
    debug: str


class ProducerConfig(CommonConfig, total=False):
    """Type-safe configuration for the Kafka Producer.

    Extends CommonConfig with producer-specific properties.
    """

    # Delivery
    acks: Union[int, Literal['all']]
    compression_type: Literal['none', 'gzip', 'snappy', 'lz4', 'zstd']
    batch_size: int
    linger_ms: int
    max_in_flight_requests_per_connection: int
    retries: int
    retry_backoff_ms: int
    delivery_timeout_ms: int

    # Idempotence & Transactions
    enable_idempotence: bool
    transactional_id: str
    transaction_timeout_ms: int

    # Buffering
    queue_buffering_max_messages: int
    queue_buffering_max_kbytes: int
    message_max_bytes: int

    # Partitioning
    partitioner: Literal['consistent', 'consistent_random', 'random', 'fnv1a', 'fnv1a_random', 'murmur2',
                          'murmur2_random']


class ConsumerConfig(CommonConfig, total=False):
    """Type-safe configuration for the Kafka Consumer.

    Extends CommonConfig with consumer-specific properties.
    """

    # Group membership
    group_id: str
    group_instance_id: str
    session_timeout_ms: int
    heartbeat_interval_ms: int
    max_poll_interval_ms: int

    # Offset management
    auto_offset_reset: Literal['smallest', 'earliest', 'beginning', 'largest', 'latest', 'end', 'error']
    enable_auto_commit: bool
    auto_commit_interval_ms: int
    enable_auto_offset_store: bool

    # Fetching
    fetch_min_bytes: int
    fetch_max_bytes: int
    max_partition_fetch_bytes: int
    fetch_wait_max_ms: int

    # Partition assignment
    partition_assignment_strategy: str


def to_config_dict(typed_config: dict) -> dict:
    """Convert a TypedDict config with underscore keys to dot-notation keys.

    The confluent-kafka-python library accepts configuration as a dictionary
    with dot-notation keys (e.g., 'bootstrap.servers'). This helper converts
    the underscore-style keys used in TypedDict definitions to the expected
    dot-notation format.

    Keys containing '_cb' (callbacks) are kept as-is since they are
    Python-specific configuration, not librdkafka properties.

    Args:
        typed_config: A dictionary with underscore-style keys.

    Returns:
        A new dictionary with dot-notation keys.
    """
    result = {}
    for key, value in typed_config.items():
        if key.endswith('_cb'):
            result[key] = value
        else:
            result[key.replace('_', '.')] = value
    return result


def print_config(config: dict, label: str) -> None:
    """Print a configuration dictionary in a readable format."""
    print(f'\n{label}:')
    for key, value in config.items():
        if callable(value):
            print(f'  {key}: <callback>')
        else:
            print(f'  {key}: {value}')


if __name__ == '__main__':
    if len(sys.argv) != 3:
        sys.stderr.write('Usage: %s <bootstrap-brokers> <topic>\n' % sys.argv[0])
        sys.exit(1)

    broker = sys.argv[1]
    topic = sys.argv[2]

    def error_callback(err):
        print(f'Error: {err}')
        if err.code() == KafkaError._ALL_BROKERS_DOWN:
            raise KafkaException(err)

    # Producer configuration with type safety.
    # IDE autocompletion will suggest valid keys and flag type mismatches.
    producer_config: ProducerConfig = {
        'bootstrap_servers': broker,
        'client_id': 'typed-config-example-producer',
        'acks': 'all',
        'compression_type': 'snappy',
        'linger_ms': 5,
        'batch_size': 65536,
        'enable_idempotence': True,
        'retries': 5,
        'error_cb': error_callback,
    }

    producer_dict = to_config_dict(producer_config)
    print_config(producer_dict, 'Producer configuration')

    # Consumer configuration with type safety.
    consumer_config: ConsumerConfig = {
        'bootstrap_servers': broker,
        'client_id': 'typed-config-example-consumer',
        'group_id': 'typed-config-example-group',
        'auto_offset_reset': 'earliest',
        'enable_auto_commit': True,
        'auto_commit_interval_ms': 5000,
        'session_timeout_ms': 30000,
        'max_poll_interval_ms': 300000,
        'fetch_min_bytes': 1,
        'error_cb': error_callback,
    }

    consumer_dict = to_config_dict(consumer_config)
    print_config(consumer_dict, 'Consumer configuration')

    # Create Producer and produce messages
    p = Producer(producer_dict)

    def delivery_callback(err, msg):
        if err:
            sys.stderr.write(f'Delivery failed: {err}\n')
        else:
            print(f'Delivered to {msg.topic()} [{msg.partition()}] @ {msg.offset()}')

    print(f'\nProducing 5 messages to topic "{topic}"...')
    for i in range(5):
        p.produce(
            topic,
            key=f'key-{i}',
            value=f'typed-config message {i}',
            callback=delivery_callback,
        )
        p.poll(0)
    p.flush()

    # Create Consumer and consume messages
    c = Consumer(consumer_dict)
    c.subscribe([topic])

    print(f'\nConsuming messages from topic "{topic}"...')
    msg_count = 0
    try:
        while msg_count < 5:
            msg = c.poll(timeout=5.0)
            if msg is None:
                print('No more messages, stopping.')
                break
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                print(f'Consumer error: {msg.error()}')
                break
            print(
                f'Consumed: key={msg.key().decode("utf-8")}, '
                f'value={msg.value().decode("utf-8")}, '
                f'partition={msg.partition()}, offset={msg.offset()}'
            )
            msg_count += 1
    except KeyboardInterrupt:
        pass
    finally:
        c.close()

    print(f'\nDone. Produced and consumed {msg_count} messages with typed configuration.')
