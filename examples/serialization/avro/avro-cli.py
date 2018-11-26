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

from six.moves import input

from confluent_kafka import avro as avro

record_schema_str = """
    {
        "type": "record",
        "name": "User",
        "fields": [
            {"name": "name", "type": "string"},
            {"name": "favorite_number",  "type": ["int", "null"]},
            {"name": "favorite_color", "type": ["string", "null"]}
        ]
    }
"""


class ParserOpts(object):
    def _build(self):
        conf = {'bootstrap.servers': self.bootstrap_servers,
                'schema.registry.url': self.schema_registry}

        if self.userinfo:
            conf['schema.registry.basic.auth.credentials.source'] = 'USER_INFO'
            conf['schema.registry.basic.auth.user.info'] = self.userinfo
        return conf

    def producer_conf(self):
        return self._build()

    def consumer_conf(self):
        return dict({"group.id": self.group}, **self._build())


class User(dict):
    _schema = avro.loads(record_schema_str)

    def __init__(self, conf=None):
        if not conf:
            conf = {"name": "anonymous",
                    "favorite_number": 0,
                    "favorite_color": ""}
        super(User, self).__init__(conf)

    @classmethod
    def schema(cls):
        return cls._schema

    def prompt(self):
        self['name'] = input("Enter name:")
        num = input("Enter favorite number:")
        if num == '':
            num = 0
        self['favorite_number'] = int(num)
        self['favorite_color'] = input("Enter favorite color:")


def on_delivery(err, msg):
    if err is not None:
        print('Message delivery failed ({} [{}]): %{}'.format(msg.topic(), str(msg.partition()), err))
        return 0
    elif err is not None:
        print('Message delivery failed {}'.format(err))
        return 0
    else:
        print('Message delivered to {} [{}] at offset [{}]: {}'.format(
              msg.topic(), msg.partition(), msg.offset(), msg.value()))
        return 1


def produce(args):
    from confluent_kafka.avro import AvroProducer

    record = User()
    topic = args.topic
    conf = args.producer_conf()

    producer = AvroProducer(conf, default_value_schema=record.schema())

    print("producing user records to topic {}. ^c to exit.".format(topic))

    while True:
        try:
            record.prompt()
            producer.produce(topic=topic, partition=0, value=record, callback=on_delivery)
            producer.poll(0.1)
        except KeyboardInterrupt:
            break
        except ValueError:
            print("Invalid input,  discarding record...")
            continue
        record = User()

    print("\nFlushing records...")
    producer.flush()


def consume(args):
    from confluent_kafka.avro import AvroConsumer
    from confluent_kafka import KafkaError
    from confluent_kafka.avro.serializer import SerializerError

    topic = args.topic
    conf = args.consumer_conf()
    print("consuming user records from topic {}".format(topic))

    c = AvroConsumer(conf)
    c.subscribe([topic])

    while True:
        try:
            msg = c.poll(1)

        except SerializerError as e:
            print("Message deserialization failed {}".format(e))
            break

        if msg is None:
            continue

        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                break
            else:
                print(msg.error())
                break

        record = User(msg.value())
        print("\nusername: {}\n\tfavorite_number:{}\n\tfavorite_color:{}".format(
              record["name"], record["favorite_number"], record["favorite_color"]))

    c.close()


def main():
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
    conf = ParserOpts()
    parser.parse_args(namespace=conf)

    if conf.mode == "produce":
        produce(conf)
    else:
        consume(conf)


if __name__ == '__main__':
    main()
