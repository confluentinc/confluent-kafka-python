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
import pytest


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

    # =========== Performance tests ===========

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

    # =========== Functional tests ===========

    def test_async_consumer_joins_and_leaves_rebalance(self):
        """Test rebalancing when consumer joins and then leaves the group"""

        async def async_rebalance_test():
            topic_name = f"test-rebalance-{uuid.uuid4()}"
            group_id = f"rebalance-group-{uuid.uuid4()}"

            # Setup
            self._setup_topic_with_messages(topic_name, partitions=2, messages=10)

            # Create consumers
            consumer1 = self._create_consumer(group_id)
            consumer2 = self._create_consumer(group_id)

            # Track rebalance events
            rebalance_events = []
            async def track_rebalance(consumer, partitions):
                rebalance_events.append(len(partitions))
                await consumer.assign(partitions)

            try:
                # Phase 1: Consumer1 joins (should get all partitions)
                await consumer1.subscribe([topic_name], on_assign=track_rebalance)
                await self._wait_for_assignment(consumer1, expected_partitions=2)
                assert len(rebalance_events) == 1

                # Phase 2: Consumer2 joins (should split partitions)
                await consumer2.subscribe([topic_name], on_assign=track_rebalance)
                await self._wait_for_balanced_assignment([consumer1, consumer2], total_partitions=2)
                assert len(rebalance_events) >= 2

                # Phase 3: Consumer2 leaves (consumer1 should get all partitions back)
                await consumer2.close()
                await self._wait_for_assignment(consumer1, expected_partitions=2)
                assert len(rebalance_events) >= 3

                # Verify functionality
                self.produce_test_messages(topic_name, num_messages=1)
                msg = await consumer1.poll(timeout=5.0)
                assert msg is not None, "Consumer should receive fresh message"

            finally:
                await consumer1.close()

        asyncio.run(async_rebalance_test())

    def _setup_topic_with_messages(self, topic_name, partitions=2, messages=10):
        """Helper: Create topic and produce test messages"""
        self.kafka.create_topic(topic_name, partitions=partitions, replication_factor=1)
        assert self.kafka.wait_for_topic(topic_name, max_wait_time=30)
        self.produce_test_messages(topic_name, num_messages=messages)

    def _create_consumer(self, group_id):
        """Helper: Create AIOConsumer with standard config"""
        return AsyncConsumerStrategy(
            self.kafka.bootstrap_servers(), group_id, self.logger, batch_size=10
        ).create_consumer()

    async def _wait_for_assignment(self, consumer, expected_partitions, max_wait=15):
        """Helper: Wait for consumer to get expected partition count"""
        for _ in range(max_wait):
            await consumer.poll(timeout=1.0)
            assignment = await consumer.assignment()
            if len(assignment) == expected_partitions:
                return
            await asyncio.sleep(1.0)

        assignment = await consumer.assignment()
        assert len(assignment) == expected_partitions, \
            f"Expected {expected_partitions} partitions, got {len(assignment)}"

    async def _wait_for_balanced_assignment(self, consumers, total_partitions, max_wait=15):
        """Helper: Wait for consumers to split partitions evenly"""
        for _ in range(max_wait):
            for consumer in consumers:
                await consumer.poll(timeout=1.0)

            assignments = [await c.assignment() for c in consumers]
            assigned_count = sum(len(a) for a in assignments)

            if assigned_count == total_partitions and all(len(a) > 0 for a in assignments):
                return
            await asyncio.sleep(1.0)

        assignments = [await c.assignment() for c in consumers]
        assigned_count = sum(len(a) for a in assignments)
        assert assigned_count == total_partitions, \
            f"Expected {total_partitions} total partitions, got {assigned_count}"

    def test_async_topic_partition_changes_rebalance(self):
        """Test rebalancing when partitions are added to existing topic"""

        async def async_topic_change_test():
            topic_name = f"test-topic-changes-{uuid.uuid4()}"

            # Setup: Create topic with 2 partitions initially
            self.kafka.create_topic(topic_name, partitions=2, replication_factor=1)
            topic_ready = self.kafka.wait_for_topic(topic_name, max_wait_time=30)
            assert topic_ready, f"Topic {topic_name} was not created"
            self.produce_test_messages(topic_name, num_messages=10)

            # Create consumers
            group_id = f"topic-changes-group-{uuid.uuid4()}"
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

            # Both consumers join - should get 1 partition each (2 total)
            await consumer1.subscribe([topic_name], on_assign=track_rebalance)
            await consumer2.subscribe([topic_name], on_assign=track_rebalance)

            # Wait for initial rebalance
            for _ in range(10):
                await consumer1.poll(timeout=1.0)
                await consumer2.poll(timeout=1.0)

                assignment1 = await consumer1.assignment()
                assignment2 = await consumer2.assignment()

                if len(assignment1) > 0 and len(assignment2) > 0:
                    break
                await asyncio.sleep(1.0)

            # Verify initial state: 2 partitions total, 1 each
            assignment1_initial = await consumer1.assignment()
            assignment2_initial = await consumer2.assignment()
            total_partitions_initial = len(assignment1_initial) + len(assignment2_initial)

            assert total_partitions_initial == 2, f"Should have 2 total partitions initially, got {total_partitions_initial}"
            assert len(rebalance_events) >= 2, f"Should have at least 2 rebalance events, got {len(rebalance_events)}"

            # Add partitions to existing topic (2 -> 4 partitions)
            self.kafka.add_partitions(topic_name, new_partition_count=4)

            # Produce messages to new partitions to trigger metadata refresh
            self.produce_test_messages(topic_name, num_messages=5)

            # Force rebalance by creating a new consumer that joins the group
            # This will trigger metadata refresh and rebalancing for all consumers
            consumer3 = AsyncConsumerStrategy(
                self.kafka.bootstrap_servers(), group_id, self.logger, batch_size=10
            ).create_consumer()
            await consumer3.subscribe([topic_name], on_assign=track_rebalance)

            # Poll all consumers until they detect new partitions and rebalance
            for _ in range(15):  # Max 15 seconds for partition discovery
                await consumer1.poll(timeout=1.0)
                await consumer2.poll(timeout=1.0)
                await consumer3.poll(timeout=1.0)

                assignment1_current = await consumer1.assignment()
                assignment2_current = await consumer2.assignment()
                assignment3_current = await consumer3.assignment()
                total_partitions_current = len(assignment1_current) + len(assignment2_current) + len(assignment3_current)

                # Rebalance complete when total partitions = 4 (distributed among 3 consumers)
                if total_partitions_current == 4:
                    break
                await asyncio.sleep(1.0)

            # Verify final state: 4 partitions total distributed among 3 consumers
            assignment1_final = await consumer1.assignment()
            assignment2_final = await consumer2.assignment()
            assignment3_final = await consumer3.assignment()
            total_partitions_final = len(assignment1_final) + len(assignment2_final) + len(assignment3_final)

            assert total_partitions_final == 4, f"Should have 4 total partitions after adding, got {total_partitions_final}"
            # With 3 consumers and 4 partitions, distribution should be roughly 1-2 partitions per consumer
            assert len(assignment1_final) >= 1, f"Consumer 1 should have at least 1 partition, got {len(assignment1_final)}"
            assert len(assignment2_final) >= 1, f"Consumer 2 should have at least 1 partition, got {len(assignment2_final)}"
            assert len(assignment3_final) >= 1, f"Consumer 3 should have at least 1 partition, got {len(assignment3_final)}"
            assert len(rebalance_events) >= 5, f"Should have at least 5 rebalance events after partition addition and consumer3 join, got {len(rebalance_events)}"

            # Verify consumers can still consume from all partitions
            msg1 = await consumer1.poll(timeout=5.0)
            msg2 = await consumer2.poll(timeout=5.0)
            msg3 = await consumer3.poll(timeout=5.0)
            messages_received = sum([1 for msg in [msg1, msg2, msg3] if msg is not None])
            assert messages_received > 0, "Consumers should receive messages from new partitions"

            # Clean up
            await consumer1.close()
            await consumer2.close()
            await consumer3.close()

        # Run the async test
        asyncio.run(async_topic_change_test())

    # TODO: verify if the current behavior is correct/intended
    def test_async_callback_exception_behavior(self):
        """Test current behavior: callback exceptions crash the consumer operation"""

        async def async_callback_test():
            topic_name = f"test-callback-exception-{uuid.uuid4()}"
            group_id = f"callback-exception-group-{uuid.uuid4()}"

            # Setup
            self._setup_topic_with_messages(topic_name, partitions=2, messages=10)
            consumer = self._create_consumer(group_id)

            # Track callback calls and create failing callback
            callback_calls = []
            async def failing_callback(consumer_obj, partitions):
                callback_calls.append("called")
                raise ValueError("Simulated callback failure")

            try:
                # Subscribe with failing callback
                await consumer.subscribe([topic_name], on_assign=failing_callback)

                # Current behavior: callback exception should propagate and crash poll()
                with pytest.raises(ValueError, match="Simulated callback failure"):
                    await consumer.poll(timeout=10.0)

                # Verify callback was called before the crash
                assert len(callback_calls) == 1, "Callback should have been called before crash"

            finally:
                # Consumer may be in an unusable state after the exception
                try:
                    await consumer.close()
                except:
                    pass  # Ignore cleanup errors after crash

        asyncio.run(async_callback_test())

    def teardown(self):
        """Clean up test environment"""
        self.logger.info("Test completed - external Kafka service remains running")
