import json
import time
from ducktape.tests.test import Test

from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext
from tests.ducktape.services.kafka import KafkaClient

try:
    from confluent_kafka import Producer, KafkaError
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

    def test_basic_produce_with_sr(self):
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
        # Create Avro serializer
        avro_serializer = AvroSerializer(schema_registry_client=self.schema_registry_client, schema_str=avro_schema_str)

        # Configure producer with Avro serializer
        producer_config = {
            'bootstrap.servers': self.kafka.bootstrap_servers(),
            'client.id': 'ducktape-test-producer-schema-registry',
        }

        self.logger.info("Creating producer with config: %s", producer_config)
        producer = Producer(producer_config)

        # Track delivery results
        delivered_messages = []
        failed_messages = []

        def delivery_callback(err, msg):
            """Delivery report callback"""
            if err is not None:
                self.logger.error("Message delivery failed: %s", err)
                failed_messages.append(err)
            else:
                delivered_messages.append(msg)

        # Produce messages
        self.logger.info("Producing messages for %.1f seconds to topic %s", test_duration, topic_name)
        start_time = time.time()
        messages_sent = 0

        while time.time() - start_time < test_duration:
            message_value = {'name': f"User{messages_sent}", 'age': messages_sent}
            try:
                # Produce message with value serialized by Avro serializer
                producer.produce(
                    topic=topic_name,
                    key=str(time.time()),
                    value=avro_serializer(message_value, SerializationContext(topic_name, MessageField.VALUE)),
                    callback=delivery_callback
                )
                messages_sent += 1
                if messages_sent % 100 == 0:
                    producer.poll(0)
            except BufferError:
                producer.poll(0.001)
                continue

        actual_duration = time.time() - start_time
        producer.flush(timeout=30)

        # Calculate throughput
        send_throughput = messages_sent / actual_duration
        delivery_throughput = len(delivered_messages) / actual_duration

        # Verify results
        self.logger.info("Time-based production results:")
        self.logger.info("  Duration: %.2f seconds", actual_duration)
        self.logger.info("  Messages sent: %d", messages_sent)
        self.logger.info("  Messages delivered: %d", len(delivered_messages))
        self.logger.info("  Messages failed: %d", len(failed_messages))
        self.logger.info("  Send throughput: %.2f msg/s", send_throughput)
        self.logger.info("  Delivery throughput: %.2f msg/s", delivery_throughput)

        assert messages_sent > 0, "No messages were sent during test duration"
        assert len(delivered_messages) > 0, "No messages were delivered"
        assert send_throughput > 10, f"Send throughput too low: {send_throughput:.2f} msg/s"

        self.logger.info("Successfully completed time-based production test with schema registry")
