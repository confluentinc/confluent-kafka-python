#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright 2024 Confluent Inc.
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


# A simple example demonstrating use of ProtobufDeserializer.
#
# To regenerate Protobuf classes you must first install the protobuf
# compiler. Once installed you may call protoc directly or use make.
#
# See the protocol buffer docs for instructions on installing and using protoc.
# https://developers.google.com/protocol-buffers/docs/pythontutorial
#
# After installing protoc execute the following command from the examples
# directory to regenerate the user_pb2 module.
# `make`

import argparse

# Protobuf generated class; resides at ./protobuf/user_pb2.py
import protobuf.user_pb2 as user_pb2

from confluent_kafka import Consumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.protobuf import ProtobufDeserializer
from confluent_kafka.schema_registry.rules.encryption.awskms.aws_driver import AwsKmsDriver
from confluent_kafka.schema_registry.rules.encryption.azurekms.azure_driver import AzureKmsDriver
from confluent_kafka.schema_registry.rules.encryption.encrypt_executor import FieldEncryptionExecutor
from confluent_kafka.schema_registry.rules.encryption.gcpkms.gcp_driver import GcpKmsDriver
from confluent_kafka.schema_registry.rules.encryption.hcvault.hcvault_driver import HcVaultKmsDriver
from confluent_kafka.schema_registry.rules.encryption.localkms.local_driver import LocalKmsDriver
from confluent_kafka.serialization import MessageField, SerializationContext


def main(args):
    # Register the KMS drivers and the field-level encryption executor
    AwsKmsDriver.register()
    AzureKmsDriver.register()
    GcpKmsDriver.register()
    HcVaultKmsDriver.register()
    LocalKmsDriver.register()
    FieldEncryptionExecutor.register()

    topic = args.topic

    sr_conf = {'url': args.schema_registry}
    schema_registry_client = SchemaRegistryClient(sr_conf)
    rule_conf = None
    # KMS credentials can be passed as follows
    # rule_conf = {'secret.access.key': 'xxx', 'access.key.id': 'yyy'}
    # Alternatively, the KMS credentials can be set via environment variables

    protobuf_deserializer = ProtobufDeserializer(
        user_pb2.User, {'use.deprecated.format': False}, schema_registry_client, rule_conf=rule_conf
    )

    consumer_conf = {
        'bootstrap.servers': args.bootstrap_servers,
        'group.id': args.group,
        'auto.offset.reset': "earliest",
    }

    consumer = Consumer(consumer_conf)
    consumer.subscribe([topic])

    while True:
        try:
            # SIGINT can't be handled when polling, limit timeout to 1 second.
            msg = consumer.poll(1.0)
            if msg is None:
                continue

            user = protobuf_deserializer(msg.value(), SerializationContext(topic, MessageField.VALUE))

            if user is not None:
                print(
                    "User record {}:\n"
                    "\tname: {}\n"
                    "\tfavorite_number: {}\n"
                    "\tfavorite_color: {}\n".format(msg.key(), user.name, user.favorite_number, user.favorite_color)
                )
        except KeyboardInterrupt:
            break

    consumer.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="ProtobufDeserializer example")
    parser.add_argument('-b', dest="bootstrap_servers", required=True, help="Bootstrap broker(s) (host[:port])")
    parser.add_argument('-s', dest="schema_registry", required=True, help="Schema Registry (http(s)://host[:port]")
    parser.add_argument('-t', dest="topic", default="example_serde_protobuf", help="Topic name")
    parser.add_argument('-g', dest="group", default="example_serde_protobuf", help="Consumer group")

    main(parser.parse_args())
