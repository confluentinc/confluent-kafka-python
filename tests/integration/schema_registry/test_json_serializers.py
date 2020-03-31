import pytest
from confluent_kafka.cimpl import TopicPartition

from confluent_kafka.schema_registry.json_schema import JsonSerializer, JsonDeserializer
from confluent_kafka.serialization import SerializationError


def test_json_record_serialization(kafka_cluster, load_file):
    """
    Tests basic JsonSerializer and JsonDeserializer basic functionality

    product.json from:
        https://json-schema.org/learn/getting-started-step-by-step.html

    Args:
        kafka_cluster (KafkaClusterFixture): cluster fixture

        load_file (callable(str)): JSON Schema file reader

    """
    topic = kafka_cluster.create_topic("serialization-json")
    sr = kafka_cluster.schema_registry({'url': 'http://localhost:8081'})

    schema_str = load_file("product.json")
    value_serializer = JsonSerializer(sr, schema_str)
    value_deserializer = JsonDeserializer(sr, schema_str)

    producer = kafka_cluster.producer(value_serializer=value_serializer)

    record = {"productId": 1,
              "productName": "An ice sculpture",
              "price": 12.50,
              "tags": ["cold", "ice"],
              "dimensions": {
                  "length": 7.0,
                  "width": 12.0,
                  "height": 9.5
              },
              "warehouseLocation": {
                  "latitude": -78.75,
                  "longitude": 20.4
              }}

    producer.produce(topic, value=record, partition=0)
    producer.flush()

    consumer = kafka_cluster.consumer(value_deserializer=value_deserializer)
    consumer.assign([TopicPartition(topic, 0)])

    msg = consumer.poll()
    actual = msg.value()

    assert all([actual[k] == v for k, v in record.items()])


def test_json_record_serialization_incompatible(kafka_cluster, load_file):
    """
    Tests Serializer validation functionality.

    product.json from:
        https://json-schema.org/learn/getting-started-step-by-step.html

    Args:
        kafka_cluster (KafkaClusterFixture): cluster fixture

        load_file (callable(str)): JSON Schema file reader

    """
    topic = kafka_cluster.create_topic("serialization-json")
    sr = kafka_cluster.schema_registry({'url': 'http://localhost:8081'})

    schema_str = load_file("product.json")
    value_serializer = JsonSerializer(sr, schema_str)
    producer = kafka_cluster.producer(value_serializer=value_serializer)

    record = {"contractorId": 1,
              "contractorName": "David Davidson",
              "contractRate": 1250,
              "trades": ["mason"]}

    with pytest.raises(SerializationError,
                       match=r"(.*) is a required property"):
        producer.produce(topic, value=record, partition=0)


def test_json_record_serialization_invalid(kafka_cluster, load_file):
    """
    Ensures ValueError raise if  JSON Schema definition lacks Title annotation.

    Args:
        kafka_cluster (KafkaClusterFixture): cluster fixture

        load_file (callable(str)): JSON Schema file reader

    """
    sr = kafka_cluster.schema_registry({'url': 'http://localhost:8081'})
    schema_str = load_file('invalid.json')

    with pytest.raises(ValueError,
                       match="Missing required JSON schema annotation Title"):
        JsonSerializer(sr, schema_str)
