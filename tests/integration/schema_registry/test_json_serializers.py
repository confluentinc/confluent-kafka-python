import pytest
from confluent_kafka.cimpl import TopicPartition

from confluent_kafka.schema_registry.json_schema import JsonSerializer, JsonDeserializer
from confluent_kafka.serialization import SerializationError


class _TestProduct(object):
    def __init__(self, product_id, name, price, tags, dimensions, location):
        self.product_id = product_id
        self.name = name
        self.price = price
        self.tags = tags
        self.dimensions = dimensions
        self.location = location


def _testProduct_to_dict(ctx, product_obj):
    """
    Returns testProduct instance in dict format.

    Args:
        ctx (SerializationContext): Metadata pertaining to the serialization
                operation.

        product_obj (_TestProduct): testProduct instance.

    Returns:
        dict: product_obj as a dictionary.

    """
    return {"productId": product_obj.product_id,
            "productName": product_obj.name,
            "price": product_obj.price,
            "tags": product_obj.tags,
            "dimensions": product_obj.dimensions,
            "warehouseLocation": product_obj.location}


def _testProduct_from_dict(ctx, product_dict):
    """
    Returns testProduct instance from it's dict format.

    Args:
        ctx (SerializationContext): Metadata pertaining to the serialization
                operation.

        product_dict (dict): testProduct in dict format.

    Returns:
        _TestProduct: product_obj instance.

    """
    return _TestProduct(product_dict['productId'],
                        product_dict['productName'],
                        product_dict['price'],
                        product_dict['tags'],
                        product_dict['dimensions'],
                        product_dict['warehouseLocation'])


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


def test_json_record_serialization_custom(kafka_cluster, load_file):
    topic = kafka_cluster.create_topic("serialization-json")
    sr = kafka_cluster.schema_registry({'url': 'http://localhost:8081'})

    schema_str = load_file("product.json")
    value_serializer = JsonSerializer(sr, schema_str, _testProduct_to_dict)
    value_deserializer = JsonDeserializer(sr, schema_str, _testProduct_from_dict)

    producer = kafka_cluster.producer(value_serializer=value_serializer)

    record = _TestProduct(product_id=1,
                          name="The ice sculpture",
                          price=12.50,
                          tags=["cold", "ice"],
                          dimensions={"length": 7.0,
                                     "width": 12.0,
                                     "height": 9.5},
                          location={"latitude": -78.75,
                                   "longitude": 20.4})

    producer.produce(topic, value=record, partition=0)
    producer.flush()

    consumer = kafka_cluster.consumer(value_deserializer=value_deserializer)
    consumer.assign([TopicPartition(topic, 0)])

    msg = consumer.poll()
    actual = msg.value()

    assert all([getattr(actual, attribute) == getattr(record, attribute)
                for attribute in vars(record)])
