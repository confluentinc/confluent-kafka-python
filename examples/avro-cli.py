#!/usr/bin/env python
#
# Copyright 2018 Confluent Inc.
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

import argparse
from uuid import uuid4

from six.moves import input

from confluent_kafka import avro

# Parse Schema used for serializing User class
record_schema = avro.loads("""
    {
        "namespace": "confluent.io.examples.serialization.avro",
        "name": "User",
        "type": "record",
        "fields": [
            {"name": "name", "type": "string"},
            {"name": "favorite_number", "type": "int"},
            {"name": "favorite_color", "type": "string"}
        ]
    }
""")


class User(object):
    """
        User stores the deserialized user Avro record.
    """

    # Use __slots__ to explicitly declare all data members.
    __slots__ = ["name", "favorite_number", "favorite_color", "id"]

    def __init__(self, name=None, favorite_number=None, favorite_color=None):
        self.name = name
        self.favorite_number = favorite_number
        self.favorite_color = favorite_color
        # Unique id used to track produce request success/failures.
        # Do *not* include in the serialized object.
        self.id = uuid4()

    def to_dict(self):
        """
            The Avro Python library does not support code generation.
            For this reason we must provide a dict representation of our class for serialization.
        """
        return {
            "name": self.name,
            "favorite_number": self.favorite_number,
            "favorite_color": self.favorite_color
        }


def on_delivery(err, msg, obj):
    """
        Handle delivery reports served from producer.poll.
        This callback takes an extra argument, obj.
        This allows the original contents to be included for debugging purposes.
    """
    if err is not None:
        print('Message {} delivery failed for user {} with error {}'.format(
            obj.id, obj.name, err))
    else:
        print('Message {} successfully produced to {} [{}] at offset {}'.format(
            obj.id, msg.topic(), msg.partition(), msg.offset()))


def produce(topic, conf):
    """
        Produce User records
    """

    from confluent_kafka.avro import AvroProducer

    producer = AvroProducer(conf, default_value_schema=record_schema)

    print("Producing user records to topic {}. ^c to exit.".format(topic))
    while True:
        # Instantiate new User, populate fields, produce record, execute callbacks.
        record = User()
        try:
            record.name = input("Enter name: ")
            record.favorite_number = int(input("Enter favorite number: "))
            record.favorite_color = input("Enter favorite color: ")

            # The message passed to the delivery callback will already be serialized.
            # To aid in debugging we provide the original object to the delivery callback.
            producer.produce(topic=topic, value=record.to_dict(),
                             callback=lambda err, msg, obj=record: on_delivery(err, msg, obj))
            # Serve on_delivery callbacks from previous asynchronous produce()
            producer.poll(0)
        except KeyboardInterrupt:
            break
        except ValueError:
            print("Invalid input, discarding record...")
            continue

    print("\nFlushing records...")
    producer.flush()


def consume(topic, conf):
    """
        Consume User records
    """
    from confluent_kafka.avro import AvroConsumer
    from confluent_kafka.avro.serializer import SerializerError

    print("Consuming user records from topic {} with group {}. ^c to exit.".format(topic, conf["group.id"]))

    c = AvroConsumer(conf, reader_value_schema=record_schema)
    c.subscribe([topic])

    while True:
        try:
            msg = c.poll(1)

            # There were no messages on the queue, continue polling
            if msg is None:
                continue

            if msg.error():
                print("Consumer error: {}".format(msg.error()))
                continue

            record = User(msg.value())
            print("name: {}\n\tfavorite_number: {}\n\tfavorite_color: {}\n".format(
                record.name, record.favorite_number, record.favorite_color))
        except SerializerError as e:
            # Report malformed record, discard results, continue polling
            print("Message deserialization failed {}".format(e))
            continue
        except KeyboardInterrupt:
            break

    print("Shutting down consumer..")
    c.close()


def main(args):
    # handle common configs
    conf = {'bootstrap.servers': args.bootstrap_servers,
            'schema.registry.url': args.schema_registry}

    if args.userinfo:
        conf['schema.registry.basic.auth.credentials.source'] = 'USER_INFO'
        conf['schema.registry.basic.auth.user.info'] = args.userinfo

    if args.mode == "produce":
        produce(args.topic, conf)
    else:
        # Fallback to earliest to ensure all messages are consumed
        conf['group.id'] = args.group
        conf['auto.offset.reset'] = "earliest"
        consume(args.topic, conf)


if __name__ == '__main__':
    # To use the provided cluster execute <source root>/tests/docker/bin/cluster_up.sh.
    # Defaults assume the use of the provided test cluster.
    parser = argparse.ArgumentParser(description="Example client for handling Avro data")
    parser.add_argument('-b', dest="bootstrap_servers",
                        default="localhost:29092", help="Bootstrap broker(s) (host[:port])")
    parser.add_argument('-s', dest="schema_registry",
                        default="http://localhost:8083", help="Schema Registry (http(s)://host[:port]")
    parser.add_argument('-t', dest="topic", default="example_avro",
                        help="Topic name")
    parser.add_argument('-u', dest="userinfo", default="ckp_tester:test_secret",
                        help="Userinfo (username:password); requires Schema Registry with HTTP basic auth enabled")
    parser.add_argument('mode', choices=['produce', 'consume'],
                        help="Execution mode (produce | consume)")
    parser.add_argument('-g', dest="group", default="example_avro",
                        help="Consumer group; required if running 'consumer' mode")

    main(parser.parse_args())
