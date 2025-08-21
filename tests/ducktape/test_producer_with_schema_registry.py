import json
import time
from uuid import uuid4
from ducktape.tests.test import Test

from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry._sync.json_schema import JSONSerializer
from confluent_kafka.schema_registry._sync.protobuf import ProtobufSerializer
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext, StringSerializer
from tests.ducktape.services.kafka import KafkaClient
from tests.integration.schema_registry.data.proto import PublicTestProto_pb2

try:
    from confluent_kafka import Producer
except ImportError:
    # Handle case where confluent_kafka is not installed
    Producer = None
    KafkaError = None

class SimpleProducerTestWithSchemaRegistry(Test):
    """Test producer functionality with Schema Registry integration"""

    def __init__(self, test_context):
        super(SimpleProducerTestWithSchemaRegistry, self).__init__(test_context=test_context)

        # Set up Kafka client and Schema Registry client
        self.kafka = KafkaClient(test_context, bootstrap_servers="localhost:9092")
        self.schema_registry_client = SchemaRegistryClient({'url': 'http://localhost:8081'})

    def setUp(self):
        """Set up test environment"""
        self.logger.info("Verifying connection to external Kafka at localhost:9092")

        if not self.kafka.verify_connection():
            raise Exception("Cannot connect to Kafka at localhost:9092. "
                            "Please ensure Kafka is running.")

        self.logger.info("Successfully connected to Kafka")

    def calculate_and_verify_results(self, start_time, messages_sent, delivered_messages, failed_messages):
        """Calculate throughput and verify results"""
        actual_duration = time.time() - start_time
        send_throughput = messages_sent / actual_duration
        delivery_throughput = len(delivered_messages) / actual_duration

        # Log results
        self.logger.info("Time-based production results:")
        self.logger.info("  Duration: %.2f seconds", actual_duration)
        self.logger.info("  Messages sent: %d", messages_sent)
        self.logger.info("  Messages delivered: %d", len(delivered_messages))
        self.logger.info("  Messages failed: %d", len(failed_messages))
        self.logger.info("  Send throughput: %.2f msg/s", send_throughput)
        self.logger.info("  Delivery throughput: %.2f msg/s", delivery_throughput)

        # Verify results
        assert messages_sent > 0, "No messages were sent during test duration"
        assert len(delivered_messages) > 0, "No messages were delivered"
        assert send_throughput > 10, f"Send throughput too low: {send_throughput:.2f} msg/s"

    def produce_messages(self, producer, topic_name, serializer, string_serializer, test_duration, message_value_func):
        """Produce messages using the given serializer"""
        delivered_messages = []
        failed_messages = []

        def delivery_callback(err, msg):
            """Delivery report callback"""
            if err is not None:
                self.logger.error("Message delivery failed: %s", err)
                failed_messages.append(err)
            else:
                delivered_messages.append(msg)

        self.logger.info("Producing messages for %.1f seconds to topic %s", test_duration, topic_name)
        start_time = time.time()
        messages_sent = 0

        while time.time() - start_time < test_duration:
            message_value = message_value_func(messages_sent)
            try:
                producer.produce(
                    topic=topic_name,
                    key=string_serializer(str(uuid4())),
                    value=serializer(message_value, SerializationContext(topic_name, MessageField.VALUE)),
                    callback=delivery_callback
                )
                messages_sent += 1
                if messages_sent % 100 == 0:
                    producer.poll(0)
            except BufferError:
                producer.poll(0.001)
                continue

        producer.flush(timeout=30)
        self.calculate_and_verify_results(start_time, messages_sent, delivered_messages, failed_messages)

    def test_basic_produce_with_avro_serialization(self):
        """Test producing messages with Avro serialization using Schema Registry"""
        if Producer is None:
            self.logger.error("confluent_kafka not available, skipping test")
            return

        topic_name = "test-topic-schema-registry"
        test_duration = 5.0  # 5 seconds

        # Create topic
        self.kafka.create_topic(topic_name, partitions=1, replication_factor=1)
        topic_ready = self.kafka.wait_for_topic(topic_name, max_wait_time=30)
        assert topic_ready, f"Topic {topic_name} was not created within timeout"

        # Define Avro schema
        avro_schema = {
            "type": "record",
            "name": "User",
            "fields": [
                {"name": "name", "type": "string"},
                {"name": "age", "type": "int"}
            ]
        }
        avro_schema_str = json.dumps(avro_schema)

        # Create serializers
        string_serializer = StringSerializer('utf8')
        avro_serializer = AvroSerializer(schema_registry_client=self.schema_registry_client, schema_str=avro_schema_str)

        # Configure producer
        producer_config = {
            'bootstrap.servers': self.kafka.bootstrap_servers(),
            'client.id': 'ducktape-test-producer-schema-registry',
        }

        self.logger.info("Creating producer with config: %s", producer_config)
        producer = Producer(producer_config)

        # Produce messages
        self.produce_messages(
            producer,
            topic_name,
            avro_serializer,
            string_serializer,
            test_duration,
            lambda messages_sent: {'name': f"User{messages_sent}", 'age': messages_sent}
        )

    def test_basic_produce_with_json_serialization(self):
        """Test producing messages with JSON serialization using Schema Registry"""
        if Producer is None:
            self.logger.error("confluent_kafka not available, skipping test")
            return

        topic_name = "test-topic-json-serialization"
        test_duration = 5.0  # 5 seconds

        # Create topic
        self.kafka.create_topic(topic_name, partitions=1, replication_factor=1)
        topic_ready = self.kafka.wait_for_topic(topic_name, max_wait_time=30)
        assert topic_ready, f"Topic {topic_name} was not created within timeout"

        # Define JSON schema
        json_schema = {
            "type": "object",
            "properties": {
                "name": {"type": "string"},
                "age": {"type": "integer"}
            },
            "required": ["name", "age"]
        }
        json_schema_str = json.dumps(json_schema)

        # Create serializers
        string_serializer = StringSerializer('utf8')
        json_serializer = JSONSerializer(json_schema_str, self.schema_registry_client)

        # Configure producer
        producer_config = {
            'bootstrap.servers': self.kafka.bootstrap_servers(),
            'client.id': 'ducktape-test-producer-json-serialization',
        }

        self.logger.info("Creating producer with config: %s", producer_config)
        producer = Producer(producer_config)

        # Produce messages
        self.produce_messages(
            producer,
            topic_name,
            json_serializer,
            string_serializer,
            test_duration,
            lambda messages_sent: {'name': f"User{messages_sent}", 'age': messages_sent}
        )

    def test_basic_produce_with_protobuf_serialization(self):
        """Test producing messages with Protobuf serialization using Schema Registry"""
        if Producer is None:
            self.logger.error("confluent_kafka not available, skipping test")
            return

        topic_name = "test-topic-protobuf-serialization"
        test_duration = 5.0  # 5 seconds

        # Create topic
        self.kafka.create_topic(topic_name, partitions=1, replication_factor=1)
        topic_ready = self.kafka.wait_for_topic(topic_name, max_wait_time=30)
        assert topic_ready, f"Topic {topic_name} was not created within timeout"

        # Create serializers
        string_serializer = StringSerializer('utf8')
        protobuf_serializer = ProtobufSerializer(PublicTestProto_pb2.TestMessage, self.schema_registry_client, {'use.deprecated.format': False})

        # Configure producer
        producer_config = {
            'bootstrap.servers': self.kafka.bootstrap_servers(),
            'client.id': 'ducktape-test-producer-protobuf-serialization',
        }

        self.logger.info("Creating producer with config: %s", producer_config)
        producer = Producer(producer_config)

        # Produce messages
        self.produce_messages(
            producer,
            topic_name,
            protobuf_serializer,
            string_serializer,
            test_duration,
            lambda messages_sent: PublicTestProto_pb2.TestMessage(
                test_string=f"example string",
                test_bool=True,
                test_bytes=b'example bytes',
                test_double=1.0,
                test_float=12.0,
                test_fixed32=1,
                test_fixed64=1,
            )
        )
