#!/usr/bin/env python
#
# Copyright 2016 Confluent Inc.
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
# Example demonstrating context manager usage for Producer, Consumer, and AdminClient.
# Context managers ensure proper cleanup of resources when exiting the 'with' block.
#

from confluent_kafka import Producer, Consumer, KafkaError
from confluent_kafka.admin import AdminClient, NewTopic
import sys


def main():
    if len(sys.argv) < 2:
        sys.stderr.write('Usage: %s <bootstrap-brokers>\n' % sys.argv[0])
        sys.exit(1)

    broker = sys.argv[1]
    topic = 'context-manager-example'

    # Example 1: AdminClient with context manager
    # Automatically destroys the admin client when exiting the 'with' block
    print("=== AdminClient Context Manager Example ===")
    admin_conf = {'bootstrap.servers': broker}

    with AdminClient(admin_conf) as admin:
        # Create a topic using AdminClient
        topic_obj = NewTopic(topic, num_partitions=1, replication_factor=1)
        futures = admin.create_topics([topic_obj])

        # Wait for the operation to complete
        for topic_name, future in futures.items():
            try:
                future.result()  # The result itself is None
                print(f"Topic '{topic_name}' created successfully")
            except Exception as e:
                print(f"Failed to create topic '{topic_name}': {e}")

        # Poll to ensure callbacks are processed
        admin.poll(timeout=1.0)

    # AdminClient is automatically destroyed here, no need for manual cleanup

    # Example 2: Producer with context manager
    # Automatically flushes pending messages and destroys the producer
    print("\n=== Producer Context Manager Example ===")
    producer_conf = {'bootstrap.servers': broker}

    def delivery_callback(err, msg):
        if err:
            print(f'Message failed delivery: {err}')
        else:
            print(f'Message delivered to {msg.topic()} [{msg.partition()}] @ offset {msg.offset()}')

    with Producer(producer_conf) as producer:
        # Produce some messages
        for i in range(5):
            value = f'Message {i} from context manager example'
            producer.produce(
                topic,
                key=f'key-{i}',
                value=value.encode('utf-8'),
                callback=delivery_callback
            )
            # Poll for delivery callbacks
            producer.poll(0)

        print(f"Produced 5 messages to topic '{topic}'")

    # Producer automatically flushes all pending messages and destroys here
    # No need to call producer.flush() or manually clean up

    # Example 3: Consumer with context manager
    # Automatically closes the consumer (leaves consumer group, commits offsets)
    print("\n=== Consumer Context Manager Example ===")
    consumer_conf = {
        'bootstrap.servers': broker,
        'group.id': 'context-manager-example-group',
        'auto.offset.reset': 'earliest'
    }

    with Consumer(consumer_conf) as consumer:
        # Subscribe to the topic
        consumer.subscribe([topic])

        # Consume messages
        msg_count = 0
        try:
            while msg_count < 5:
                msg = consumer.poll(timeout=1.0)
                if msg is None:
                    continue

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        # End of partition, try next message
                        continue
                    else:
                        print(f'Consumer error: {msg.error()}')
                        break

                print(f'Consumed message: key={msg.key().decode("utf-8")}, '
                      f'value={msg.value().decode("utf-8")}, '
                      f'partition={msg.partition()}, offset={msg.offset()}')
                msg_count += 1
        except KeyboardInterrupt:
            print('Consumer interrupted by user')

    # Consumer automatically calls close() here (leaves group, commits offsets)
    # No need to manually call consumer.close()

    print("\n=== All examples completed successfully! ===")


if __name__ == '__main__':
    main()
