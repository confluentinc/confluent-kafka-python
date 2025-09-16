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
from confluent_kafka.aio import AIOConsumer
import asyncio


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

    @matrix(consumer_type=["async"])
    def test_consumer_enters_group_rebalance(self, consumer_type):
        """Test basic rebalancing when a second consumer joins the group"""

        async def async_rebalance_test():
            topic_name = f"test-rebalance-enter-{uuid.uuid4()}"

            # Setup: Create topic and produce messages
            self.kafka.create_topic(topic_name, partitions=2, replication_factor=1)
            topic_ready = self.kafka.wait_for_topic(topic_name, max_wait_time=30)
            assert topic_ready, f"Topic {topic_name} was not created"
            self.produce_test_messages(topic_name, num_messages=10)

            # Create consumers with shared group ID
            group_id = f"rebalance-test-group-{uuid.uuid4()}"
            consumer1 = AsyncConsumerStrategy(
                self.kafka.bootstrap_servers(), group_id, self.logger, batch_size=10
            ).create_consumer()
            consumer2 = AsyncConsumerStrategy(
                self.kafka.bootstrap_servers(), group_id, self.logger, batch_size=10
            ).create_consumer()

            # Track rebalance events
            rebalance_events = []
            async def track_rebalance(consumer, partitions):
                rebalance_events.append(len(partitions))
                await consumer.assign(partitions)

            # Consumer 1 joins first - should get both partitions
            await consumer1.subscribe([topic_name], on_assign=track_rebalance)
            msg1 = await consumer1.poll(timeout=10.0)
            assert msg1 is not None, "Consumer 1 should receive a message"
            await asyncio.sleep(1)  # Let assignment complete

            assignment1 = await consumer1.assignment()
            assert len(assignment1) == 2, f"Consumer 1 should have 2 partitions, got {len(assignment1)}"
            assert len(rebalance_events) == 1, f"Should have 1 rebalance event, got {len(rebalance_events)}"

            # Consumer 2 joins - should trigger rebalance
            await consumer2.subscribe([topic_name], on_assign=track_rebalance)

            # Poll both consumers until rebalance completes
            for attempt in range(15):  # Max 15 seconds
                await consumer1.poll(timeout=1.0)
                await consumer2.poll(timeout=1.0)

                assignment1_current = await consumer1.assignment()
                assignment2_current = await consumer2.assignment()

                # Rebalance complete when both have partitions
                if len(assignment1_current) > 0 and len(assignment2_current) > 0:
                    break
                await asyncio.sleep(1.0)

            # Verify final state
            assignment1_final = await consumer1.assignment()
            assignment2_final = await consumer2.assignment()

            assert len(assignment1_final) == 1, f"Consumer 1 should have 1 partition, got {len(assignment1_final)}"
            assert len(assignment2_final) == 1, f"Consumer 2 should have 1 partition, got {len(assignment2_final)}"
            assert len(rebalance_events) >= 2, f"Should have at least 2 rebalance events, got {len(rebalance_events)}"

            # Verify consumers can still consume messages
            msg1_after = await consumer1.poll(timeout=5.0)
            msg2_after = await consumer2.poll(timeout=5.0)
            messages_received = sum([1 for msg in [msg1_after, msg2_after] if msg is not None])
            assert messages_received > 0, "At least one consumer should receive messages after rebalance"

            # Clean up
            await consumer1.close()
            await consumer2.close()

        # Run the async test
        asyncio.run(async_rebalance_test())

    def teardown(self):
        """Clean up test environment"""
        self.logger.info("Test completed - external Kafka service remains running")
