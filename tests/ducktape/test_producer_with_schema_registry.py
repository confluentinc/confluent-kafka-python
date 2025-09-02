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
from tests.ducktape.benchmark_metrics import MetricsCollector, MetricsBounds, validate_metrics, print_metrics_report
from tests.integration.schema_registry.data.proto import PublicTestProto_pb2
from confluent_kafka import Producer


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
            raise ConnectionError("Cannot connect to Kafka at localhost:9092. "
                            "Please ensure Kafka is running.")

        self.logger.info("Successfully connected to Kafka")

    def calculate_and_verify_results(self, metrics_summary, bounds, serialization_type):
        """Calculate throughput and verify results using comprehensive metrics"""
        is_valid, violations = validate_metrics(metrics_summary, bounds)

        # Print comprehensive metrics report
        self.logger.info("%s serialization test with comprehensive metrics completed:", serialization_type)
        print_metrics_report(metrics_summary, is_valid, violations)

        # Enhanced assertions using metrics
        assert metrics_summary['messages_sent'] > 0, "No messages were sent during test duration"
        assert metrics_summary['messages_delivered'] > 0, "No messages were delivered"
        assert metrics_summary['send_throughput_msg_per_sec'] > 10, \
            f"Send throughput too low: {metrics_summary['send_throughput_msg_per_sec']:.2f} msg/s " \
            f"(expected > 10 msg/s)"

        # Validate against performance bounds
        if not is_valid:
            self.logger.warning("Performance bounds validation failed for %s: %s",
                                serialization_type, "; ".join(violations))

        self.logger.info("Successfully completed %s test with comprehensive metrics", serialization_type)

    def produce_messages_with_serialization(self, producer, topic_name, serializer, string_serializer, test_duration, message_value_func, serialization_type):
        """Produce messages using the given serializer with comprehensive metrics collection"""
        # Initialize metrics collection and bounds
        metrics = MetricsCollector()
        bounds = MetricsBounds()

        # Track send times for latency calculation
        send_times = {}

        def delivery_callback(err, msg):
            """Enhanced delivery report callback with metrics tracking"""
            if err is not None:
                self.logger.error("Message delivery failed: %s", err)
                metrics.record_failed(topic=msg.topic() if msg else topic_name,
                                      partition=msg.partition() if msg else 0)
            else:
                # Calculate actual latency if we have send time
                msg_key = msg.key().decode('utf-8', errors='replace') if msg.key() else 'unknown'
                if msg_key in send_times:
                    latency_ms = (time.time() - send_times[msg_key]) * 1000
                    del send_times[msg_key]  # Clean up
                else:
                    latency_ms = 0.0  # Default latency if timing info not available

                metrics.record_delivered(latency_ms, topic=msg.topic(), partition=msg.partition())

        # Start metrics collection
        metrics.start()

        self.logger.info("Producing messages with %s serialization and metrics for %.1f seconds to topic %s",
                         serialization_type, test_duration, topic_name)
        start_time = time.time()
        messages_sent = 0

        while time.time() - start_time < test_duration:
            message_value = message_value_func(messages_sent)
            message_key = str(uuid4())

            try:
                # Calculate message size for metrics
                serialized_key = string_serializer(message_key)
                serialized_value = serializer(message_value, SerializationContext(topic_name, MessageField.VALUE))
                message_size = len(serialized_key) + len(serialized_value)

                # Record message being sent with metrics
                metrics.record_sent(message_size, topic=topic_name, partition=0)

                # Track send time for latency calculation
                send_times[message_key] = time.time()

                producer.produce(
                    topic=topic_name,
                    key=serialized_key,
                    value=serialized_value,
                    callback=delivery_callback
                )
                messages_sent += 1

                # Poll frequently to trigger delivery callbacks and record poll operations
                if messages_sent % 100 == 0:
                    producer.poll(0)
                    metrics.record_poll()

            except BufferError:
                # Record buffer full events and poll
                metrics.record_buffer_full()
                producer.poll(0.001)
                continue

        # Flush to ensure all messages are sent
        self.logger.info("Flushing producer...")
        producer.flush(timeout=30)

        # Finalize metrics collection
        metrics.finalize()

        # Get comprehensive metrics summary and validate
        metrics_summary = metrics.get_summary()
        self.calculate_and_verify_results(metrics_summary, bounds, serialization_type)

    def test_basic_produce_with_avro_serialization(self):
        """Test producing messages with Avro serialization using Schema Registry"""
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
        self.produce_messages_with_serialization(
            producer,
            topic_name,
            avro_serializer,
            string_serializer,
            test_duration,
            lambda messages_sent: {'name': f"User{messages_sent}", 'age': messages_sent},
            "Avro"
        )

    def test_basic_produce_with_json_serialization(self):
        """Test producing messages with JSON serialization using Schema Registry"""
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
        self.produce_messages_with_serialization(
            producer,
            topic_name,
            json_serializer,
            string_serializer,
            test_duration,
            lambda messages_sent: {'name': f"User{messages_sent}", 'age': messages_sent},
            "JSON"
        )

    def test_basic_produce_with_protobuf_serialization(self):
        """Test producing messages with Protobuf serialization using Schema Registry"""
        topic_name = "test-topic-protobuf-serialization"
        test_duration = 5.0  # 5 seconds

        # Create topic
        self.kafka.create_topic(topic_name, partitions=1, replication_factor=1)
        topic_ready = self.kafka.wait_for_topic(topic_name, max_wait_time=30)
        assert topic_ready, f"Topic {topic_name} was not created within timeout"

        # Create serializers
        string_serializer = StringSerializer('utf8')
        protobuf_serializer = ProtobufSerializer(PublicTestProto_pb2.TestMessage, self.schema_registry_client)

        # Configure producer
        producer_config = {
            'bootstrap.servers': self.kafka.bootstrap_servers(),
            'client.id': 'ducktape-test-producer-protobuf-serialization',
        }

        self.logger.info("Creating producer with config: %s", producer_config)
        producer = Producer(producer_config)

        # Produce messages
        self.produce_messages_with_serialization(
            producer,
            topic_name,
            protobuf_serializer,
            string_serializer,
            test_duration,
            lambda _: PublicTestProto_pb2.TestMessage(
                test_string=f"example string",
                test_bool=True,
                test_bytes=b'example bytes',
                test_double=1.0,
                test_float=12.0,
                test_fixed32=1,
                test_fixed64=1,
            ),
            "Protobuf"
        )
