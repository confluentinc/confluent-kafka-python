"""
Ducktape test for Confluent Kafka Python Consumer
Assumes Kafka is already running on localhost:9092
"""
import time
import uuid
from ducktape.tests.test import Test
from ducktape.mark import matrix

from tests.ducktape.services.kafka import KafkaClient
from tests.ducktape.consumer_benchmark_metrics import (
    ConsumerMetricsCollector, ConsumerMetricsBounds,
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

    def create_consumer_strategy(self, consumer_type, group_id=None, batch_size=10):
        """Create appropriate consumer strategy based on type"""
        if not group_id:
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

    def create_consumer(self, consumer_type, group_id=None, batch_size=10):
        return self.create_consumer_strategy(consumer_type, group_id, batch_size).create_consumer()

    def produce_test_messages(self, topic_name, num_messages, serialization_type=None):
        """Produce messages to topic for consumer tests with optional Schema Registry serialization"""
        # Create producer configuration
        producer_config = {
            'bootstrap.servers': self.kafka.bootstrap_servers(),
            'client.id': f'ducktape-test-producer'
        }
        producer = Producer(producer_config)

        # Setup serializers if using Schema Registry
        if serialization_type:
            key_serializer, value_serializer = self.create_serializers(serialization_type)
            self.logger.info(f"Producing {num_messages} messages with {serialization_type} serialization to {topic_name}")
        else:
            self.logger.info(f"Producing {num_messages} messages to {topic_name}")

        # Produce messages
        for i in range(num_messages):
            try:
                # Create message content based on serialization type
                if serialization_type == 'protobuf':
                    message_value = PublicTestProto_pb2.TestMessage(
                        test_string=f'User{i}',
                        test_bool=i % 2 == 0,
                        test_bytes=f'bytes{i}'.encode('utf-8'),
                        test_double=float(i),
                        test_float=float(i),
                        test_fixed32=i,
                        test_fixed64=i,
                        test_int32=i,
                        test_int64=i,
                        test_sfixed32=i,
                        test_sfixed64=i,
                        test_sint32=i,
                        test_sint64=i,
                        test_uint32=i,
                        test_uint64=i
                    )
                elif serialization_type:  # Avro or JSON
                    # Match the Protobuf schema structure for Avro/JSON
                    # For JSON, convert bytes to base64 string
                    if serialization_type == 'json':
                        test_bytes = f'bytes{i}'  # JSON uses string for bytes
                    else:
                        test_bytes = f'bytes{i}'.encode('utf-8')  # Avro uses actual bytes

                    message_value = {
                        'test_string': f'User{i}',
                        'test_bool': i % 2 == 0,
                        'test_bytes': test_bytes,
                        'test_double': float(i),
                        'test_float': float(i),
                        'test_fixed32': i,
                        'test_fixed64': i,
                        'test_int32': i,
                        'test_int64': i,
                        'test_sfixed32': i,
                        'test_sfixed64': i,
                        'test_sint32': i,
                        'test_sint64': i,
                        'test_uint32': i,
                        'test_uint64': i
                    }
                else:
                    # Plain messages - no complex structure needed
                    message_value = None  # Will be handled in serialization section

                # Serialize key and value if using Schema Registry
                if serialization_type:
                    serialized_key = key_serializer(f'key{i}')
                    serialized_value = value_serializer(
                        message_value,
                        SerializationContext(topic_name, MessageField.VALUE)
                    )
                else:
                    serialized_key = f"key-{i}"
                    serialized_value = f"User{i}"  # Simple string for plain messages

                producer.produce(
                    topic=topic_name,
                    key=serialized_key,
                    value=serialized_value,
                )

                # Flush more frequently to prevent buffer overflow with large message count
                if i % 50 == 0:
                    producer.poll(0)
                    if i % 1000 == 0:
                        producer.flush(timeout=1)  # Periodic flush

            except Exception as e:
                self.logger.error(f"Failed to produce message {i}: {e}")

        # Final flush
        producer.flush(timeout=60)
        self.logger.info(f"Successfully produced {num_messages} plain text messages")

    # =========== Performance tests ===========

    @matrix(consumer_type=["sync", "async"], batch_size=[1, 5, 20])
    def test_basic_consume(self, consumer_type, batch_size):
        """Test batch consumption with comprehensive metrics and bounds validation"""
        self._run_consumer_performance_test(
            consumer_type=consumer_type,
            operation_type="consume",
            batch_size=batch_size,
        )

    @matrix(consumer_type=["sync", "async"])
    def test_basic_poll(self, consumer_type):
        """Test single message polling with comprehensive metrics and bounds validation"""
        self._run_consumer_performance_test(
            consumer_type=consumer_type,
            operation_type="poll",
        )

    @matrix(consumer_type=["sync", "async"], serialization_type=["avro", "json", "protobuf"])
    def test_basic_consume_with_schema_registry(self, consumer_type, serialization_type):
        """
        Test batch consumption with Schema Registry deserialization with comprehensive metrics and bounds validation.

        Note: in this test, we are consuming messages with the same schema,
        a realistic high-throughput scenario.
        We cache the schema in the Schema Registry client, so only the first message
        makes HTTP calls to the Schema Registry server.
        Performance impact compared to test_basic_consume should come from per-message
        deserialization overhead.
        """

        self._run_consumer_performance_test(
            consumer_type=consumer_type,
            operation_type="consume",
            batch_size=20,
            serialization_type=serialization_type,
            num_messages_to_produce=500000,
        )

    @matrix(consumer_type=["sync", "async"], serialization_type=["avro", "json", "protobuf"])
    def test_basic_poll_with_schema_registry(self, consumer_type, serialization_type):
        """
        Test single message polling with Schema Registry deserialization with comprehensive metrics and bounds validation.

        Note: in this test, we are consuming messages with the same schema,
        a realistic high-throughput scenario.
        We cache the schema in the Schema Registry client, so only the first message
        makes HTTP calls to the Schema Registry server.
        Performance impact compared to test_basic_consume should come from per-message
        deserialization overhead.
        """

        self._run_consumer_performance_test(
            consumer_type=consumer_type,
            operation_type="poll",
            serialization_type=serialization_type,
            num_messages_to_produce=500000,
        )

    # =========== Functional tests ===========

    def test_async_consumer_joins_and_leaves_rebalance(self):
        """Test rebalancing when consumer joins and then leaves the group"""

        async def async_rebalance_test():
            topic_name = f"test-rebalance-{uuid.uuid4()}"
            group_id = f"rebalance-group-{uuid.uuid4()}"  # Shared group ID

            # Setup
            self._setup_topic_with_messages(topic_name, partitions=2, messages=10)

            # Create consumers with shared group ID
            consumer1 = self.create_consumer("async", group_id)
            consumer2 = self.create_consumer("async", group_id)

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

    def test_async_topic_partition_changes_rebalance(self):
        """Test rebalancing when partitions are added to existing topic"""

        async def async_topic_change_test():
            topic_name = f"test-topic-changes-{uuid.uuid4()}"
            group_id = f"topic-changes-group-{uuid.uuid4()}"  # Shared group ID

            # Setup: Create topic with 2 partitions initially
            self.kafka.create_topic(topic_name, partitions=2, replication_factor=1)
            topic_ready = self.kafka.wait_for_topic(topic_name, max_wait_time=30)
            assert topic_ready, f"Topic {topic_name} was not created"
            self.produce_test_messages(topic_name, num_messages=10)

            # Create consumers with shared group ID
            consumer1 = self.create_consumer("async", group_id)
            consumer2 = self.create_consumer("async", group_id)

            # Track rebalance events
            rebalance_events = []

            async def track_rebalance(consumer, partitions):
                rebalance_events.append(len(partitions))
                await consumer.assign(partitions)

            # Both consumers join - should get 1 partition each (2 total)
            await consumer1.subscribe([topic_name], on_assign=track_rebalance)
            await consumer2.subscribe([topic_name], on_assign=track_rebalance)

            # Wait for initial rebalance
            for attempt in range(10):
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

            assert total_partitions_initial == 2, \
                f"Should have 2 total partitions initially, got {total_partitions_initial}"
            assert len(rebalance_events) >= 2, \
                f"Should have at least 2 rebalance events, got {len(rebalance_events)}"

            # Add partitions to existing topic (2 -> 4 partitions)
            self.kafka.add_partitions(topic_name, new_partition_count=4)

            # Produce messages to new partitions to trigger metadata refresh
            self.produce_test_messages(topic_name, num_messages=5)

            # Force rebalance by creating a new consumer that joins the group
            # This will trigger metadata refresh and rebalancing for all consumers
            consumer3 = self.create_consumer("async", group_id)
            await consumer3.subscribe([topic_name], on_assign=track_rebalance)

            # Poll all consumers until they detect new partitions and rebalance
            consumers = [consumer1, consumer2, consumer3]
            for _ in range(30):
                # Poll all consumers concurrently
                await asyncio.gather(*[c.poll(timeout=1.0) for c in consumers])

                # Check total partitions across all consumers
                assignments = await asyncio.gather(*[c.assignment() for c in consumers])
                total_partitions_current = sum(len(assignment) for assignment in assignments)

                # Rebalance complete when total partitions = 4 (distributed among 3 consumers)
                if total_partitions_current == 4:
                    break
                await asyncio.sleep(0.5)

            # Verify final state: 4 partitions total distributed among 3 consumers
            assignment1_final = await consumer1.assignment()
            assignment2_final = await consumer2.assignment()
            assignment3_final = await consumer3.assignment()
            total_partitions_final = (len(assignment1_final) +
                                      len(assignment2_final) +
                                      len(assignment3_final))

            assert total_partitions_final == 4, \
                f"Should have 4 total partitions after adding, got {total_partitions_final}"
            # With 3 consumers and 4 partitions, distribution should be roughly 1-2 partitions per consumer
            assert len(assignment1_final) >= 1, \
                f"Consumer 1 should have at least 1 partition, got {len(assignment1_final)}"
            assert len(assignment2_final) >= 1, \
                f"Consumer 2 should have at least 1 partition, got {len(assignment2_final)}"
            assert len(assignment3_final) >= 1, \
                f"Consumer 3 should have at least 1 partition, got {len(assignment3_final)}"
            assert len(rebalance_events) >= 5, \
                ("Should have at least 5 rebalance events after partition addition and consumer3 join, "
                 f"got {len(rebalance_events)}")

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

        asyncio.run(async_topic_change_test())

    def test_async_callback_exception_behavior(self):
        """Test current behavior: callback exceptions propagate and fail the consumer"""

        async def async_callback_test():
            topic_name = f"test-callback-exception-{uuid.uuid4()}"
            group_id = f"callback-exception-group-{uuid.uuid4()}"
            # Setup
            self._setup_topic_with_messages(topic_name, partitions=2, messages=10)
            consumer = self.create_consumer("async", group_id)

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
                except Exception:
                    pass  # Ignore cleanup errors after crash

        asyncio.run(async_callback_test())

    def teardown(self):
        """Clean up test environment"""
        self.logger.info("Test completed - external Kafka service remains running")

    # =========== Private Helper Methods ===========

    def _run_consumer_performance_test(self, consumer_type, operation_type,
                                       batch_size=None,
                                       serialization_type=None,
                                       num_messages_to_produce=1500000):
        """
        Shared helper for consumer performance tests

        Args:
            consumer_type: "sync" or "async"
            operation_type: "consume" or "poll"
            batch_size: Number of messages per batch (default None). Only required for consume operation
            serialization_type: Schema Registry serialization type ("avro", "json", "protobuf") or None for plain text
        """
        topic_name = f"performance-test-{consumer_type}-{operation_type}-{serialization_type or 'plain'}-topic"
        test_duration = 5.0  # 5 seconds

        # Create topic
        self.kafka.create_topic(topic_name, partitions=1, replication_factor=1)

        # Wait for topic to be available
        topic_ready = self.kafka.wait_for_topic(topic_name, max_wait_time=30)
        assert topic_ready, (f"Topic {topic_name} was not created within timeout. "
                             f"Available topics: {self.kafka.list_topics()}")

        # Produce test messages
        self.produce_test_messages(topic_name, num_messages_to_produce, serialization_type)

        # Initialize metrics collection and bounds
        metrics = ConsumerMetricsCollector(operation_type=operation_type, serialization_type=serialization_type)
        bounds = ConsumerMetricsBounds()

        # Create appropriate consumer strategy
        strategy = self.create_consumer_strategy(consumer_type, batch_size=batch_size)

        # Assign metrics collector to strategy
        strategy.metrics = metrics

        self.logger.info(f"Testing {consumer_type} consumer {operation_type}, with serialization type {serialization_type}, for {test_duration} seconds")

        # Start metrics collection
        metrics.start()

        # Container for consumed messages
        consumed_messages = []

        # Run the test
        start_time = time.time()
        if operation_type == "consume":
            messages_consumed = strategy.consume_messages(
                        topic_name, test_duration, start_time, consumed_messages,
                        timeout=0.1, serialization_type=serialization_type
                )
        else:  # poll
            messages_consumed = strategy.poll_messages(
                        topic_name, test_duration, start_time, consumed_messages,
                        timeout=0.1, serialization_type=serialization_type
        )

        # Finalize metrics collection
        metrics.finalize()

        # Get comprehensive metrics summary
        metrics_summary = metrics.get_summary()
        is_valid, violations = validate_consumer_metrics(metrics_summary, bounds)

        # Print comprehensive metrics report
        print_consumer_metrics_report(metrics_summary, is_valid, violations, consumer_type, batch_size, serialization_type)

        # Enhanced assertions using metrics
        assert messages_consumed > 0, "No messages were consumed"
        assert len(consumed_messages) > 0, "No messages were collected"
        assert metrics_summary['messages_consumed'] > 0, "No messages were consumed (metrics)"
        assert metrics_summary['consumption_rate_msg_per_sec'] > 0, \
            f"Consumption rate too low: {metrics_summary['consumption_rate_msg_per_sec']:.2f} msg/s"

        # Validate against performance bounds
        if not is_valid:
            self.logger.warning("Performance bounds validation failed: %s", "; ".join(violations))

        self.logger.info(f"Successfully completed basic {operation_type} test with comprehensive metrics")

        # Return consumed messages for additional validation (e.g., Schema Registry deserialization checks)
        return consumed_messages

    def _setup_topic_with_messages(self, topic_name, partitions=2, messages=10):
        """Helper: Create topic and produce test messages"""
        self.kafka.create_topic(topic_name, partitions=partitions, replication_factor=1)
        assert self.kafka.wait_for_topic(topic_name, max_wait_time=30)
        self.produce_test_messages(topic_name, num_messages=messages)

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

    def create_serializers(self, serialization_type):
        """Create Schema Registry serializers for message production"""
        sr_client = SchemaRegistryClient({'url': 'http://localhost:8081', 'basic.auth.user.info': 'ASUHV2PEDSTIW3LF:cfltSQ9mRLOItofBcTEzk6Ml/86VAqb9gjy2YYoeRDZZgML/LZ/ift9QBOyuyAyw'})

        key_serializer = StringSerializer('utf8')

        if serialization_type == 'avro':
            # Match the Protobuf TestMessage structure
            avro_schema = {
                "type": "record",
                "name": "TestMessage",
                "fields": [
                    {"name": "test_string", "type": "string"},
                    {"name": "test_bool", "type": "boolean"},
                    {"name": "test_bytes", "type": "bytes"},
                    {"name": "test_double", "type": "double"},
                    {"name": "test_float", "type": "float"},
                    {"name": "test_fixed32", "type": "int"},
                    {"name": "test_fixed64", "type": "long"},
                    {"name": "test_int32", "type": "int"},
                    {"name": "test_int64", "type": "long"},
                    {"name": "test_sfixed32", "type": "int"},
                    {"name": "test_sfixed64", "type": "long"},
                    {"name": "test_sint32", "type": "int"},
                    {"name": "test_sint64", "type": "long"},
                    {"name": "test_uint32", "type": "int"},
                    {"name": "test_uint64", "type": "long"}
                ]
            }
            value_serializer = AvroSerializer(
                schema_registry_client=sr_client,
                schema_str=json.dumps(avro_schema)
            )
        elif serialization_type == 'json':
            # Match the Protobuf TestMessage structure
            json_schema = {
                "type": "object",
                "properties": {
                    "test_string": {"type": "string"},
                    "test_bool": {"type": "boolean"},
                    "test_bytes": {"type": "string"},
                    "test_double": {"type": "number"},
                    "test_float": {"type": "number"},
                    "test_fixed32": {"type": "integer"},
                    "test_fixed64": {"type": "integer"},
                    "test_int32": {"type": "integer"},
                    "test_int64": {"type": "integer"},
                    "test_sfixed32": {"type": "integer"},
                    "test_sfixed64": {"type": "integer"},
                    "test_sint32": {"type": "integer"},
                    "test_sint64": {"type": "integer"},
                    "test_uint32": {"type": "integer"},
                    "test_uint64": {"type": "integer"}
                },
                "required": ["test_string", "test_bool", "test_bytes", "test_double", "test_float",
                           "test_fixed32", "test_fixed64", "test_int32", "test_int64", "test_sfixed32",
                           "test_sfixed64", "test_sint32", "test_sint64", "test_uint32", "test_uint64"]
            }
            value_serializer = JSONSerializer(json.dumps(json_schema), sr_client)
        elif serialization_type == 'protobuf':
            value_serializer = ProtobufSerializer(PublicTestProto_pb2.TestMessage, sr_client)

        return key_serializer, value_serializer

    def teardown(self):
        """Clean up test environment"""
        self.logger.info("Test completed - external Kafka service remains running")
