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


# A simple example demonstrating use of JSONSerializer.

import argparse
from uuid import uuid4

from confluent_kafka.schema_registry.rules.encryption.encrypt_executor import \
    FieldEncryptionExecutor

from confluent_kafka.schema_registry.rules.encryption.localkms.local_driver import \
    LocalKmsDriver

from confluent_kafka.schema_registry.rules.encryption.hcvault.hcvault_driver import \
    HcVaultKmsDriver

from confluent_kafka.schema_registry.rules.encryption.gcpkms.gcp_driver import \
    GcpKmsDriver

from confluent_kafka.schema_registry.rules.encryption.azurekms.azure_driver import \
    AzureKmsDriver

from confluent_kafka.schema_registry.rules.encryption.awskms.aws_driver import \
    AwsKmsDriver
from six.moves import input

from confluent_kafka import Producer
from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient, Rule, \
    RuleKind, RuleMode, RuleParams, Schema, RuleSet
from confluent_kafka.schema_registry.json_schema import JSONSerializer


class User(object):
    """
    User record

    Args:
        name (str): User's name

        favorite_number (int): User's favorite number

        favorite_color (str): User's favorite color

        address(str): User's address; confidential
    """

    def __init__(self, name, address, favorite_number, favorite_color):
        self.name = name
        self.favorite_number = favorite_number
        self.favorite_color = favorite_color
        # address should not be serialized, see user_to_dict()
        self._address = address


def user_to_dict(user, ctx):
    """
    Returns a dict representation of a User instance for serialization.

    Args:
        user (User): User instance.

        ctx (SerializationContext): Metadata pertaining to the serialization
            operation.

    Returns:
        dict: Dict populated with user attributes to be serialized.
    """

    # User._address must not be serialized; omit from dict
    return dict(name=user.name,
                favorite_number=user.favorite_number,
                favorite_color=user.favorite_color)


def delivery_report(err, msg):
    """
    Reports the success or failure of a message delivery.

    Args:
        err (KafkaError): The error that occurred on None on success.
        msg (Message): The message that was produced or failed.
    """

    if err is not None:
        print("Delivery failed for User record {}: {}".format(msg.key(), err))
        return
    print('User record {} successfully produced to {} [{}] at offset {}'.format(
        msg.key(), msg.topic(), msg.partition(), msg.offset()))


def main(args):
    # Register the KMS drivers and the field-level encryption executor
    AwsKmsDriver.register()
    AzureKmsDriver.register()
    GcpKmsDriver.register()
    HcVaultKmsDriver.register()
    LocalKmsDriver.register()
    FieldEncryptionExecutor.register()

    topic = args.topic
    kek_name = args.kek_name
    kms_type = args.kms_type
    kms_key_id = args.kms_key_id

    schema_str = """
    {
      "$schema": "http://json-schema.org/draft-07/schema#",
      "title": "User",
      "description": "A Confluent Kafka Python User",
      "type": "object",
      "properties": {
        "name": {
          "description": "User's name",
          "type": "string",
          "confluent:tags": ["PII"]
        },
        "favorite_number": {
          "description": "User's favorite number",
          "type": "number",
          "exclusiveMinimum": 0
        },
        "favorite_color": {
          "description": "User's favorite color",
          "type": "string"
        }
      },
      "required": [ "name", "favorite_number", "favorite_color" ]
    }
    """
    schema_registry_conf = {'url': args.schema_registry}
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)

    rule = Rule(
        "test-encrypt",
        "",
        RuleKind.TRANSFORM,
        RuleMode.WRITEREAD,
        "ENCRYPT",
        ["PII"],
        RuleParams({
            "encrypt.kek.name": kek_name,
            "encrypt.kms.type": kms_type,
            "encrypt.kms.key.id": kms_key_id
        }),
        None,
        None,
        "ERROR,NONE",
        False
    )

    subject = f"{topic}-value"
    schema_registry_client.register_schema(subject, Schema(
        schema_str,
        "JSON",
        [],
        None,
        RuleSet(None, [rule])
    ))

    ser_conf = {'auto.register.schemas': False, 'use.latest.version': True}
    rule_conf = None
    # KMS credentials can be passed as follows
    # rule_conf = {'secret.access.key': 'xxx', 'access.key.id': 'yyy'}
    # Alternatively, the KMS credentials can be set via environment variables
    json_serializer = JSONSerializer(schema_str,
                                     schema_registry_client,
                                     user_to_dict,
                                     conf=ser_conf,
                                     rule_conf=rule_conf)

    string_serializer = StringSerializer('utf_8')

    producer = Producer({'bootstrap.servers': args.bootstrap_servers})

    print("Producing user records to topic {}. ^C to exit.".format(topic))
    while True:
        # Serve on_delivery callbacks from previous calls to produce()
        producer.poll(0.0)
        try:
            user_name = input("Enter name: ")
            user_address = input("Enter address: ")
            user_favorite_number = int(input("Enter favorite number: "))
            user_favorite_color = input("Enter favorite color: ")
            user = User(name=user_name,
                        address=user_address,
                        favorite_color=user_favorite_color,
                        favorite_number=user_favorite_number)
            producer.produce(topic=topic,
                             key=string_serializer(str(uuid4())),
                             value=json_serializer(user, SerializationContext(topic, MessageField.VALUE)),
                             on_delivery=delivery_report)
        except KeyboardInterrupt:
            break
        except ValueError:
            print("Invalid input, discarding record...")
            continue

    print("\nFlushing records...")
    producer.flush()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="JSONSerailizer example")
    parser.add_argument('-b', dest="bootstrap_servers", required=True,
                        help="Bootstrap broker(s) (host[:port])")
    parser.add_argument('-s', dest="schema_registry", required=True,
                        help="Schema Registry (http(s)://host[:port]")
    parser.add_argument('-t', dest="topic", default="example_serde_json",
                        help="Topic name")

    parser.add_argument('-kn', dest="kek_name", required=True,
                        help="KEK name")
    parser.add_argument('-kt', dest="kms_type", required=True,
                        help="KMS type, one of aws-kms, azure-kms, gcp-kms, hcvault")
    parser.add_argument('-ki', dest="kms_key_id", required=True,
                        help="KMS key id, such as an ARN")

    main(parser.parse_args())
