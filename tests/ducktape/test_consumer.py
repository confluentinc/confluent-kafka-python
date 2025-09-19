"""
Ducktape test for Confluent Kafka Python Consumer
Assumes Kafka is already running on localhost:9092
"""
import time
import uuid
from ducktape.tests.test import Test
from ducktape.mark import matrix

from tests.ducktape.services.kafka import KafkaClient
from tests.ducktape.consumer_benchmark_metrics import (ConsumerMetricsCollector, ConsumerMetricsBounds,
                                                       validate_consumer_metrics, print_consumer_metrics_report)
from tests.ducktape.consumer_strategy import SyncConsumerStrategy, AsyncConsumerStrategy
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.schema_registry._sync.json_schema import JSONSerializer
from confluent_kafka.schema_registry._sync.protobuf import ProtobufSerializer
from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField
from tests.integration.schema_registry.data.proto import PublicTestProto_pb2
import json


class SimpleConsumerTest(Test):
    """Test basic consumer functionality with external Kafka"""

    def __init__(self, test_context):
        super(SimpleConsumerTest, self).__init__(test_context=test_context)

        # Set up Kafka client (assumes external Kafka running)
        self.kafka = KafkaClient(test_context, bootstrap_servers="localhost:9092")

    def setup(self):
        """Set up test environment"""
        self.logger.info("Verifying connection to external Kafka at localhost:9092")

        if not self.kafka.verify_connection():
            raise Exception("Cannot connect to Kafka at localhost:9092. "
                            "Please ensure Kafka is running.")

        self.logger.info("Successfully connected to Kafka")

    def create_consumer(self, consumer_type, batch_size=10):
        """Create appropriate consumer strategy based on type"""
        group_id = f"test-group-{uuid.uuid4()}"  # Unique group ID for each test

        if consumer_type == "sync":
            return SyncConsumerStrategy(
                self.kafka.bootstrap_servers(),
                group_id,
                self.logger,
                batch_size
            )
        else:  # async
            return AsyncConsumerStrategy(
                self.kafka.bootstrap_servers(),
                group_id,
                self.logger,
                batch_size
            )

    def produce_test_messages(self, topic_name, num_messages):
        """Produce messages to topic for consumer tests"""
        producer = Producer({'bootstrap.servers': self.kafka.bootstrap_servers()})

        self.logger.info(f"Producing {num_messages} test messages to {topic_name}")

        for i in range(num_messages):
            producer.produce(
                topic=topic_name,
                value=f"Test message {i}",
                key=f"key-{i}"
            )

            # Flush more frequently to prevent buffer overflow with large message count
            if i % 50 == 0:
                producer.poll(0)
                if i % 1000 == 0:
                    producer.flush(timeout=1)  # Periodic flush

        producer.flush(timeout=60)  # Final flush with longer timeout
        self.logger.info(f"Successfully produced {num_messages} messages")

    def produce_test_serialized_messages(self, topic_name, num_messages, serialization_type):
        """Produce messages with Schema Registry serialization for consumer tests"""
        # Create serializers for message production
        key_serializer, value_serializer = self.create_serializers(serialization_type)

        # Create producer
        producer_config = {
            'bootstrap.servers': self.kafka.bootstrap_servers(),
            'client.id': 'ducktape-test-producer-sr'
        }
        producer = Producer(producer_config)

        self.logger.info(f"Producing {num_messages} messages with {serialization_type} serialization to {topic_name}")

        produced_count = 0

        def delivery_callback(err, msg):
            nonlocal produced_count
            if err is None:
                produced_count += 1
            else:
                self.logger.error(f"Message delivery failed: {err}")

        # Produce messages
        for i in range(num_messages):
            # Create message based on type
            if serialization_type == 'avro':
                message_value = {'name': f'User{i}', 'age': i % 100}
            elif serialization_type == 'json':
                message_value = {'name': f'User{i}', 'age': i % 100}
            elif serialization_type == 'protobuf':
                message_value = PublicTestProto_pb2.TestMessage(test_string=f'User{i}', test_int32=i % 100)

            # Serialize and produce
            try:
                serialized_key = key_serializer(f'key{i}')
                serialized_value = value_serializer(
                    message_value,
                    SerializationContext(topic_name, MessageField.VALUE)
                )

                producer.produce(
                    topic=topic_name,
                    key=serialized_key,
                    value=serialized_value,
                    on_delivery=delivery_callback
                )

                # Poll for delivery reports periodically
                if i % 1000 == 0:
                    producer.poll(0)

            except Exception as e:
                self.logger.error(f"Failed to produce message {i}: {e}")

        # Final flush
        producer.flush(timeout=30)
        self.logger.info(f"Successfully produced {produced_count} messages with {serialization_type} serialization")
        assert produced_count > 0, "No messages were produced"
        return produced_count

    @matrix(consumer_type=["sync", "async"], batch_size=[1, 5, 20])
    def test_basic_consume(self, consumer_type, batch_size):
        """Test basic message consumption with comprehensive metrics and bounds validation"""

        topic_name = f"test-{consumer_type}-consumer-topic"
        test_duration = 5.0  # 5 seconds
        # TODO: clean up this magic number
        num_messages = 1500000  # 1.5M messages for sustained 5-second consumption at ~300K msg/s

        # Create topic
        self.kafka.create_topic(topic_name, partitions=1, replication_factor=1)

        # Wait for topic to be available
        topic_ready = self.kafka.wait_for_topic(topic_name, max_wait_time=30)
        assert topic_ready, (f"Topic {topic_name} was not created within timeout. "
                             f"Available topics: {self.kafka.list_topics()}")

        # Produce test messages
        self.produce_test_messages(topic_name, num_messages)

        # Initialize metrics collection and bounds
        metrics = ConsumerMetricsCollector(operation_type="consume")
        bounds = ConsumerMetricsBounds()

        # Create appropriate consumer strategy
        strategy = self.create_consumer(consumer_type, batch_size)

        # Assign metrics collector to strategy
        strategy.metrics = metrics

        self.logger.info(f"Testing {consumer_type} consumer for {test_duration} seconds")

        # Start metrics collection
        metrics.start()

        # Container for consumed messages
        consumed_messages = []

        # Run the test
        start_time = time.time()
        messages_consumed = strategy.consume_messages(
            topic_name, test_duration, start_time, consumed_messages, timeout=0.1
        )

        # Finalize metrics collection
        metrics.finalize()

        # Get comprehensive metrics summary
        metrics_summary = metrics.get_summary()
        is_valid, violations = validate_consumer_metrics(metrics_summary, bounds)

        # Print comprehensive metrics report
        print_consumer_metrics_report(metrics_summary, is_valid, violations, consumer_type, batch_size)

        # Get AIOConsumer built-in metrics for comparison (async only)
        final_metrics = strategy.get_final_metrics()

        if final_metrics:
            self.logger.info("=== AIOConsumer Built-in Metrics ===")
            for key, value in final_metrics.items():
                self.logger.info(f"{key}: {value}")

        # Enhanced assertions using metrics
        assert messages_consumed > 0, "No messages were consumed"
        assert len(consumed_messages) > 0, "No messages were collected"
        assert metrics_summary['messages_consumed'] > 0, "No messages were consumed (metrics)"
        assert metrics_summary['consumption_rate_msg_per_sec'] > 0, \
            f"Consumption rate too low: {metrics_summary['consumption_rate_msg_per_sec']:.2f} msg/s"

        # Validate against performance bounds
        if not is_valid:
            self.logger.warning("Performance bounds validation failed: %s", "; ".join(violations))

        self.logger.info("Successfully completed basic consumption test with comprehensive metrics")

    @matrix(consumer_type=["sync", "async"])
    def test_basic_poll(self, consumer_type):
        """Test basic message polling (single message) with comprehensive metrics and bounds validation"""

        topic_name = f"test-{consumer_type}-poll-topic"
        test_duration = 5.0  # 5 seconds
        num_messages = 1500000  # 1.5M messages for sustained 5-second consumption

        # Create topic
        self.kafka.create_topic(topic_name, partitions=1, replication_factor=1)

        # Wait for topic to be available with retry logic
        topic_ready = self.kafka.wait_for_topic(topic_name, max_wait_time=30)
        assert topic_ready, (f"Topic {topic_name} was not created within timeout. "
                             f"Available topics: {self.kafka.list_topics()}")

        # Produce test messages
        self.produce_test_messages(topic_name, num_messages)

        # Initialize metrics collection and bounds
        metrics = ConsumerMetricsCollector(operation_type="poll")
        bounds = ConsumerMetricsBounds()

        # Create appropriate consumer strategy
        strategy = self.create_consumer(consumer_type)

        # Assign metrics collector to strategy
        strategy.metrics = metrics

        self.logger.info(f"Testing {consumer_type} consumer polling (single messages) for {test_duration} seconds")

        # Start metrics collection
        metrics.start()

        # Container for consumed messages
        consumed_messages = []

        # Run the test using poll_messages instead of consume_messages
        start_time = time.time()
        messages_consumed = strategy.poll_messages(
            topic_name, test_duration, start_time, consumed_messages, timeout=0.1
        )

        # Finalize metrics collection
        metrics.finalize()

        # Get comprehensive metrics summary
        metrics_summary = metrics.get_summary()
        is_valid, violations = validate_consumer_metrics(metrics_summary, bounds)

        # Print comprehensive metrics report
        print_consumer_metrics_report(metrics_summary, is_valid, violations, consumer_type, 1)

        # Enhanced assertions using metrics
        assert messages_consumed > 0, "No messages were consumed"
        assert len(consumed_messages) > 0, "No messages were collected"
        assert metrics_summary['messages_consumed'] > 0, "No messages were consumed (metrics)"
        assert metrics_summary['consumption_rate_msg_per_sec'] > 0, \
            f"Consumption rate too low: {metrics_summary['consumption_rate_msg_per_sec']:.2f} msg/s"

        # Validate against performance bounds
        if not is_valid:
            self.logger.warning("Performance bounds validation failed: %s", "; ".join(violations))

        self.logger.info("Successfully completed basic poll test with comprehensive metrics")

    def create_serializers(self, serialization_type):
        """Create Schema Registry serializers for message production"""
        sr_client = SchemaRegistryClient({
            'url': 'http://localhost:8081',
            'basic.auth.user.info': 'ASUHV2PEDSTIW3LF:cfltSQ9mRLOItofBcTEzk6Ml/86VAqb9gjy2YYoeRDZZgML/LZ/ift9QBOyuyAyw'
        })

        key_serializer = StringSerializer('utf8')

        if serialization_type == 'avro':
            avro_schema = {
                "type": "record",
                "name": "User",
                "fields": [
                    {"name": "name", "type": "string"},
                    {"name": "age", "type": "int"}
                ]
            }
            value_serializer = AvroSerializer(
                schema_registry_client=sr_client,
                schema_str=json.dumps(avro_schema)
            )
        elif serialization_type == 'json':
            json_schema = {
                "type": "object",
                "properties": {
                    "name": {"type": "string"},
                    "age": {"type": "integer"}
                },
                "required": ["name", "age"]
            }
            value_serializer = JSONSerializer(json.dumps(json_schema), sr_client)
        elif serialization_type == 'protobuf':
            value_serializer = ProtobufSerializer(PublicTestProto_pb2.TestMessage, sr_client)

        return key_serializer, value_serializer

    @matrix(consumer_type=["sync", "async"], deserialization_type=["avro", "json", "protobuf"])
    def test_basic_consume_with_schema_registry(self, consumer_type, deserialization_type):
        """
        Test consumer with Schema Registry deserialization for comprehensive performance analysis.

        Note: in this test, we are consuming messages with the same schema,
        a realistic high-throughput scenario.
        We cache the schema in the Schema Registry client, so only the first message
        makes HTTP calls to the Schema Registry server.
        Performance impact compared to test_basic_consume should come from per-message
        deserialization overhead.
        """
        topic_name = f"test-{consumer_type}-sr-{deserialization_type}-topic"
        test_duration = 5.0  # 5 seconds
        batch_size = 20
        num_messages = 500000  # Sufficient messages for sustained 5-second consumption at SR throughput

        # Create topic
        self.kafka.create_topic(topic_name, partitions=1, replication_factor=1)

        # Wait for topic to be available
        topic_ready = self.kafka.wait_for_topic(topic_name, max_wait_time=30)
        assert topic_ready, (f"Topic {topic_name} was not created within timeout. "
                             f"Available topics: {self.kafka.list_topics()}")

        # Produce messages with Schema Registry serialization
        self.produce_test_serialized_messages(topic_name, num_messages, deserialization_type)

        # Initialize consumer metrics collection and bounds
        metrics = ConsumerMetricsCollector(operation_type="consume_with_schema_registry")
        bounds = ConsumerMetricsBounds()

        # Create appropriate consumer strategy with batch_size
        strategy = self.create_consumer(consumer_type, batch_size)
        strategy.metrics = metrics

        self.logger.info(f"Testing {consumer_type} consumer with {deserialization_type} "
                         f"deserialization for {test_duration} seconds")

        metrics.start()
        consumed_messages = []
        start_time = time.time()
        messages_consumed = strategy.consume_messages(
            topic_name, test_duration, start_time, consumed_messages,
            timeout=0.1, deserialization_type=deserialization_type
        )
        metrics.finalize()
        metrics_summary = metrics.get_summary()
        is_valid, violations = validate_consumer_metrics(metrics_summary, bounds)
        print_consumer_metrics_report(metrics_summary, is_valid, violations,
                                      f"{consumer_type}_sr_{deserialization_type}", batch_size)

        # Enhanced assertions using metrics
        assert messages_consumed > 0, "No messages were consumed"
        assert len(consumed_messages) > 0, "No messages were collected"
        assert metrics_summary['messages_consumed'] > 0, "No messages were consumed (metrics)"

        # Validate deserialized message structure
        if consumed_messages:
            sample_message = consumed_messages[0]
            if deserialization_type in ['avro', 'json']:
                assert isinstance(sample_message.value(), dict), \
                    f"Expected dict for {deserialization_type}, got {type(sample_message.value())}"
                assert 'name' in sample_message.value(), "Missing 'name' field in deserialized message"
                assert 'age' in sample_message.value(), "Missing 'age' field in deserialized message"
            elif deserialization_type == 'protobuf':
                assert hasattr(sample_message.value(), 'test_string'), \
                    "Missing 'test_string' attribute in protobuf message"
                assert hasattr(sample_message.value(), 'test_int32'), \
                    "Missing 'test_int32' attribute in protobuf message"

        # Validate against performance bounds
        if not is_valid:
            self.logger.warning("Performance bounds validation failed: %s", "; ".join(violations))

    def teardown(self):
        """Clean up test environment"""
        self.logger.info("Test completed - external Kafka service remains running")
