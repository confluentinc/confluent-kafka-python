"""
Ducktape test for Confluent Kafka Python Consumer
Assumes Kafka is already running on localhost:9092
"""
import time
import uuid
from ducktape.tests.test import Test
from ducktape.mark import matrix, parametrize

from tests.ducktape.services.kafka import KafkaClient
from tests.ducktape.consumer_benchmark_metrics import ConsumerMetricsCollector, ConsumerMetricsBounds, validate_consumer_metrics, print_consumer_metrics_report
from tests.ducktape.consumer_strategy import SyncConsumerStrategy, AsyncConsumerStrategy
from tests.ducktape.producer_strategy import SyncProducerStrategy  # For producing test messages
from confluent_kafka import Producer  # For pre-producing test messages


class SimpleConsumerTest(Test):
    """Test basic consumer functionality with external Kafka"""

    def __init__(self, test_context):
        super(SimpleConsumerTest, self).__init__(test_context=test_context)

        # Set up Kafka client (assumes external Kafka running)
        self.kafka = KafkaClient(test_context, bootstrap_servers="localhost:9092")

    def setUp(self):
        """Set up test environment"""
        self.logger.info("Verifying connection to external Kafka at localhost:9092")

        if not self.kafka.verify_connection():
            raise Exception("Cannot connect to Kafka at localhost:9092. "
                            "Please ensure Kafka is running.")

        self.logger.info("Successfully connected to Kafka")

    def createConsumer(self, consumer_type):
        """Create appropriate consumer strategy based on type"""
        group_id = f"test-group-{uuid.uuid4()}"  # Unique group ID for each test

        if consumer_type == "sync":
            return SyncConsumerStrategy(
                self.kafka.bootstrap_servers(),
                group_id,
                self.logger
            )
        else:  # async
            return AsyncConsumerStrategy(
                self.kafka.bootstrap_servers(),
                group_id,
                self.logger
            )

    def produce_test_messages(self, topic_name, num_messages=10000):
        """Produce messages to topic for consumer tests"""
        producer_strategy = SyncProducerStrategy(self.kafka.bootstrap_servers(), self.logger)
        producer = producer_strategy.create_producer()

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

    @matrix(consumer_type=["sync", "async"])
    def test_basic_consume(self, consumer_type):
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
        metrics = ConsumerMetricsCollector()
        bounds = ConsumerMetricsBounds()

        # Create appropriate consumer strategy
        strategy = self.createConsumer(consumer_type)

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
        self.logger.info(f"=== {consumer_type.upper()} CONSUMER METRICS REPORT ===")
        print_consumer_metrics_report(metrics_summary, is_valid, violations)

        # Get AIOConsumer built-in metrics for comparison (async only)
        final_metrics = strategy.get_final_metrics()

        if final_metrics:
            self.logger.info(f"=== AIOConsumer Built-in Metrics ===")
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

        # Pre-produce messages for consumption
        producer = Producer({'bootstrap.servers': self.kafka.bootstrap_servers()})
        self.logger.info(f"Pre-producing {num_messages} messages...")

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

        # Initialize metrics collection and bounds
        metrics = ConsumerMetricsCollector()
        bounds = ConsumerMetricsBounds()

        # Create appropriate consumer strategy
        strategy = self.createConsumer(consumer_type)

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
        self.logger.info(f"=== {consumer_type.upper()} CONSUMER POLL METRICS REPORT ===")
        print_consumer_metrics_report(metrics_summary, is_valid, violations)

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

    def tearDown(self):
        """Clean up test environment"""
        self.logger.info("Test completed - external Kafka service remains running")
