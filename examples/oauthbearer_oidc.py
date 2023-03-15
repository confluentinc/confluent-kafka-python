import confluent_kafka
from confluent_kafka import Producer,Consumer
from confluent_kafka.serialization import StringSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.admin import (AdminClient, NewTopic, NewPartitions, ConfigResource, ConfigSource,
                                   AclBinding, AclBindingFilter, ResourceType, ResourcePatternType, AclOperation,
                                   AclPermissionType)

def delivery_report(err, msg):
    """
    Reports the failure or success of a message delivery.

    Args:
        err (KafkaError): The error that occurred on None on success.

        msg (Message): The message that was produced or failed.

    Note:
        In the delivery report callback the Message.key() and Message.value()
        will be the binary format as encoded by any configured Serializers and
        not the same object that was passed to produce().
        If you wish to pass the original object(s) for key and value to delivery
        report callback we recommend a bound callback or lambda where you pass
        the objects along.

    """
    if err is not None:
        print('Delivery failed for User record {}: {}'.format(msg.key(), err))
        return
    print('User record {} successfully produced to {} [{}] at offset {}'.format(
        msg.key(), msg.topic(), msg.partition(), msg.offset()))

def main(args):
    commonProperties = {
                            "bootstrap.servers":                   bootstrapServers,
                            "security.protocol":                   "SASL_SSL",
                            "sasl.mechanism":                      "OAUTHBEARER",
                            "sasl.oauthbearer.method":             "OIDC",
                            "sasl.oauthbearer.client.id":          "<oauthbearer_client_id>",
                            "sasl.oauthbearer.client.secret":      "<oauthbearer_client_secret>",
                            "sasl.oauthbearer.token.endpoint.url": "<token_endpoint_url>",
                            "sasl.oauthbearer.extensions":         extensions,
                            "sasl.oauthbearer.scope":              "<scope>",
                        }
    producerProperties = {}
    consumerProperties = {
                                "group.id":                 "oauthbearer_oidc_example",
                                "auto.offset.reset":        "earliest",
                                "enable.auto.offset.store": "false",
                         }
    for k,v in commonProperties:
            producerProperties[k] = v
            consumerProperties[k] = v
    

    # Create Admin client
    admin_client = AdminClient(commonProperties)
    admin_client.create_topics([NewTopic('python-ouathbearer-oidc', num_partitions=3, replication_factor=1)])
    producer = Producer(producerProperties)
    consumer = Consumer(consumerProperties)

    consumer.subscribe(['python-ouathbearer-oidc'])
    serializer = StringSerializer('utf_8')

    producer.produce(topic='python-ouathbearer-oidc',
                    key=serializer("Producer_Message"),
                    value=serializer("Hello There, from Producer!"),
                    on_delivery=delivery_report)
    producer.flush()
    msg = consumer.poll(timeout=3.0)
    print(msg.value)
    admin_client.close()
    producer.close()
    consumer.close()

if __name__ == '__main__':
    main()
