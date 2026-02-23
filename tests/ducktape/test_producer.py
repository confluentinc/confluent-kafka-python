"""
Ducktape test for Confluent Kafka Python Producer
Assumes Kafka is already running on localhost:9092
"""

import time

from ducktape.mark import matrix
from ducktape.tests.test import Test

from tests.ducktape.producer_benchmark_metrics import (
    MetricsBounds,
    MetricsCollector,
    print_metrics_report,
    validate_metrics,
)
from tests.ducktape.producer_strategy import AsyncProducerStrategy, SyncProducerStrategy
from tests.ducktape.services.kafka import KafkaClient


class SimpleProducerTest(Test):
    """Test basic producer functionality with external Kafka"""

    def __init__(self, test_context):
        super(SimpleProducerTest, self).__init__(test_context=test_context)

        # Set up Kafka client (assumes external Kafka running)
        self.kafka = KafkaClient(test_context, bootstrap_servers="localhost:9092")

    def setUp(self):
        """Set up test environment"""
        self.logger.info("Verifying connection to external Kafka at localhost:9092")

        if not self.kafka.verify_connection():
            raise Exception("Cannot connect to Kafka at localhost:9092. " "Please ensure Kafka is running.")

        self.logger.info("Successfully connected to Kafka")

    def create_producer(self, producer_type, config_overrides=None):
        """Create appropriate producer strategy based on type"""
        if producer_type == "sync":
            strategy = SyncProducerStrategy(self.kafka.bootstrap_servers(), self.logger)
        else:  # async
            strategy = AsyncProducerStrategy(self.kafka.bootstrap_servers(), self.logger)

        # Store config overrides for later use in create_producer
        strategy.config_overrides = config_overrides
        return strategy

    @matrix(producer_type=["sync", "async"])
    def test_basic_produce(self, producer_type):
        """Test basic message production with comprehensive metrics and bounds validation"""

        topic_name = f"test-{producer_type}-topic"
        test_duration = 5.0  # 5 seconds

        # Create topic
        self.kafka.create_topic(topic_name, partitions=1, replication_factor=1)

        # Wait for topic to be available with retry logic
        topic_ready = self.kafka.wait_for_topic(topic_name, max_wait_time=30)
        assert topic_ready, (
            f"Topic {topic_name} was not created within timeout. " f"Available topics: {self.kafka.list_topics()}"
        )

        # Initialize metrics collection and bounds
        metrics = MetricsCollector()
        bounds = MetricsBounds()

        # Create appropriate producer strategy
        strategy = self.create_producer(producer_type)

        # Assign metrics collector to strategy
        strategy.metrics = metrics

        self.logger.info(f"Testing {producer_type} producer for {test_duration} seconds")

        # Start metrics collection
        metrics.start()

        # Message formatter
        def message_formatter(msg_num):
            return f"Test message {msg_num}", f"key-{msg_num}"

        # Run the test
        start_time = time.time()
        messages_sent = strategy.produce_messages(
            topic_name,
            test_duration,
            start_time,
            message_formatter,
        )

        # Finalize metrics collection
        metrics.finalize()

        # Get comprehensive metrics summary
        metrics_summary = metrics.get_summary()
        is_valid, violations = validate_metrics(metrics_summary, bounds)

        # Print comprehensive metrics report
        self.logger.info(f"=== {producer_type.upper()} PRODUCER METRICS REPORT ===")
        print_metrics_report(metrics_summary, is_valid, violations)

        # Enhanced assertions using metrics
        assert messages_sent > 0, "No messages were sent"
        assert metrics_summary["messages_delivered"] > 0, "No messages were delivered"
        assert (
            metrics_summary["send_throughput_msg_per_sec"] > 10
        ), f"Send throughput too low: {metrics_summary['send_throughput_msg_per_sec']:.2f} msg/s"

        # Validate against performance bounds
        if not is_valid:
            self.logger.error("Performance bounds validation failed: %s", "; ".join(violations))
            assert False, f"Performance bounds validation failed: {'; '.join(violations)}"

        self.logger.info("Successfully completed basic production test with comprehensive metrics")

    @matrix(producer_type=["sync", "async"])
    def test_basic_produce_with_transaction(self, producer_type):
        """Test basic transactional message production with comprehensive metrics and bounds validation"""

        topic_name = f"test-{producer_type}-topic-with-transaction"
        test_duration = 5.0  # 5 seconds

        # Create topic
        self.kafka.create_topic(topic_name, partitions=1, replication_factor=1)

        # Wait for topic to be available with retry logic
        topic_ready = self.kafka.wait_for_topic(topic_name, max_wait_time=30)
        assert topic_ready, (
            f"Topic {topic_name} was not created within timeout. " f"Available topics: {self.kafka.list_topics()}"
        )

        # Initialize metrics collection and bounds
        metrics = MetricsCollector()
        import os

        config_path = os.path.join(os.path.dirname(__file__), "transaction_benchmark_bounds.json")
        bounds = MetricsBounds.from_config_file(config_path)

        # Create appropriate producer strategy
        strategy = self.create_producer(producer_type)

        # Assign metrics collector to strategy
        strategy.metrics = metrics

        self.logger.info(f"Testing {producer_type} producer for {test_duration} seconds")

        # Start metrics collection
        metrics.start()

        # Message formatter
        def message_formatter(msg_num):
            return f"Test message {msg_num}", f"key-{msg_num}"

        # Run the test
        start_time = time.time()
        messages_sent = strategy.produce_messages(
            topic_name,
            test_duration,
            start_time,
            message_formatter,
            use_transaction=True,
        )

        # Finalize metrics collection
        metrics.finalize()

        # Get comprehensive metrics summary
        metrics_summary = metrics.get_summary()
        is_valid, violations = validate_metrics(metrics_summary, bounds)

        # Print comprehensive metrics report
        self.logger.info(f"=== {producer_type.upper()} PRODUCER WITH TRANSACTION METRICS REPORT ===")
        print_metrics_report(metrics_summary, is_valid, violations)

        # Enhanced assertions using metrics
        assert messages_sent > 0, "No messages were sent"
        assert metrics_summary["messages_delivered"] > 0, "No messages were delivered"
        assert (
            metrics_summary["send_throughput_msg_per_sec"] > 10
        ), f"Send throughput too low: {metrics_summary['send_throughput_msg_per_sec']:.2f} msg/s"

        # Validate against performance bounds
        if not is_valid:
            self.logger.error("Performance bounds validation failed: %s", "; ".join(violations))
            assert False, f"Performance bounds validation failed: {'; '.join(violations)}"

        self.logger.info("Successfully completed basic production test with comprehensive metrics with transaction")

    @matrix(producer_type=["sync", "async"], test_duration=[2, 5, 10])
    def test_produce_multiple_batches(self, producer_type, test_duration):
        """Test batch throughput with comprehensive metrics and bounds validation"""

        topic_name = f"{producer_type}-batch-test-topic-{test_duration}s"

        # Create topic
        self.kafka.create_topic(topic_name, partitions=2, replication_factor=1)

        # Wait for topic to be available with retry logic
        topic_ready = self.kafka.wait_for_topic(topic_name, max_wait_time=30)
        assert topic_ready, f"Topic {topic_name} was not created within timeout"

        # Initialize metrics collection and bounds
        metrics = MetricsCollector()
        bounds = MetricsBounds()
        # Adjust bounds for different test durations
        if test_duration <= 2:
            bounds.min_throughput_msg_per_sec = 50.0  # Lower threshold for short tests

        # Create appropriate producer strategy
        strategy = self.create_producer(producer_type)

        # Assign metrics collector to strategy
        strategy.metrics = metrics

        self.logger.info(f"Testing {producer_type} producer with batches for {test_duration} seconds")

        # Start metrics collection
        metrics.start()

        # Message formatter for batch test
        def message_formatter(msg_num):
            return f"Batch message {msg_num}", f"batch-key-{msg_num}"

        # Run the test
        start_time = time.time()
        messages_sent = strategy.produce_messages(
            topic_name,
            test_duration,
            start_time,
            message_formatter,
        )

        # Finalize metrics collection
        metrics.finalize()

        # Get comprehensive metrics summary
        metrics_summary = metrics.get_summary()
        is_valid, violations = validate_metrics(metrics_summary, bounds)

        # Get AIOProducer built-in metrics for comparison (async only)
        final_metrics = strategy.get_final_metrics()

        # Print comprehensive metrics report
        self.logger.info(f"=== {producer_type.upper()} BATCH TEST ({test_duration}s) METRICS REPORT ===")
        print_metrics_report(metrics_summary, is_valid, violations)

        if final_metrics:
            # Get the actual metrics dictionary
            producer_metrics_summary = final_metrics.get_summary()
            if producer_metrics_summary:
                self.logger.info("=== Producer Built-in Metrics ===")
                self.logger.info(f"Runtime: {producer_metrics_summary['duration_seconds']:.2f}s")
                self.logger.info(f"Success Rate: {producer_metrics_summary['success_rate']:.3f}")
                self.logger.info(f"Throughput: {producer_metrics_summary['send_throughput_msg_per_sec']:.1f} msg/sec")
                self.logger.info(f"Latency: Avg={producer_metrics_summary['avg_latency_ms']:.1f}ms")

        # Enhanced assertions using metrics
        assert messages_sent > 0, "No messages were sent"
        assert metrics_summary["messages_delivered"] > 0, "No messages were delivered"
        assert (
            metrics_summary["send_throughput_msg_per_sec"] > 10
        ), f"Send throughput too low: {metrics_summary['send_throughput_msg_per_sec']:.2f} msg/s"

        # Validate against performance bounds
        if not is_valid:
            self.logger.error(
                "Performance bounds validation failed for %ds test: %s",
                test_duration,
                "; ".join(violations),
            )
            assert False, f"Performance bounds validation failed for {test_duration}s test: {'; '.join(violations)}"

        self.logger.info(
            "Successfully completed %ds batch production test with comprehensive metrics",
            test_duration,
        )

    @matrix(producer_type=["sync", "async"], compression_type=["none", "gzip", "snappy"])
    def test_produce_with_compression(self, producer_type, compression_type):
        """Test compression throughput with comprehensive metrics and bounds validation"""

        topic_name = f"{producer_type}-compression-test-{compression_type}"
        test_duration = 5.0  # 5 seconds

        # Create topic
        self.kafka.create_topic(topic_name, partitions=1, replication_factor=1)

        # Wait for topic to be available with retry logic
        topic_ready = self.kafka.wait_for_topic(topic_name, max_wait_time=30)
        assert topic_ready, f"Topic {topic_name} was not created within timeout"

        # Initialize metrics collection and bounds
        metrics = MetricsCollector()
        bounds = MetricsBounds()
        # Adjust bounds for compression tests (may be slower with large messages)
        bounds.min_throughput_msg_per_sec = 5.0  # Lower threshold for large messages
        bounds.max_p95_latency_ms = 5000.0  # Allow higher latency for compression

        # Create appropriate producer strategy with compression config
        compression_config = {}
        if compression_type != "none":
            compression_config["compression.type"] = compression_type

        # Configure polling intervals based on compression type and producer type
        if producer_type == "async":
            polling_config = {
                "gzip": 10,  # Poll every 10 messages for gzip (frequent)
                "snappy": 50,  # Poll every 50 messages for snappy (moderate)
                "none": 100,  # Poll every 100 messages for none (standard)
            }
        else:  # sync
            # Sync producers need more frequent polling to prevent buffer overflow as throughput is very high
            polling_config = {
                "gzip": 5,  # Poll every 5 messages for gzip (most frequent)
                "snappy": 25,  # Poll every 25 messages for snappy (moderate)
                "none": 50,  # Poll every 50 messages for none (standard)
            }
        poll_interval = polling_config.get(compression_type, 50 if producer_type == "sync" else 100)

        strategy = self.create_producer(producer_type, compression_config)
        strategy.poll_interval = poll_interval

        # Assign metrics collector to strategy
        strategy.metrics = metrics

        self.logger.info(
            f"Testing {producer_type} producer with {compression_type} compression " f"for {test_duration} seconds"
        )
        self.logger.info(f"Using polling interval: {poll_interval} messages per poll")

        # Start metrics collection
        metrics.start()

        # Create larger messages to test compression effectiveness
        large_message = "x" * 1000  # 1KB message

        # Message formatter for compression test
        def message_formatter(msg_num):
            return f"{large_message}-{msg_num}", f"comp-key-{msg_num}"

        # Run the test
        start_time = time.time()
        messages_sent = strategy.produce_messages(
            topic_name,
            test_duration,
            start_time,
            message_formatter,
        )

        # Finalize metrics collection
        metrics.finalize()

        # Get comprehensive metrics summary
        metrics_summary = metrics.get_summary()
        is_valid, violations = validate_metrics(metrics_summary, bounds)

        # Get AIOProducer built-in metrics for comparison (async only)
        final_metrics = strategy.get_final_metrics()

        # Print comprehensive metrics report
        self.logger.info(f"=== {producer_type.upper()} COMPRESSION TEST ({compression_type}) METRICS REPORT ===")
        print_metrics_report(metrics_summary, is_valid, violations)

        if final_metrics:
            # Get the actual metrics dictionary
            producer_metrics_summary = final_metrics.get_summary()
            if producer_metrics_summary:
                self.logger.info("=== Producer Built-in Metrics ===")
                self.logger.info(f"Runtime: {producer_metrics_summary['duration_seconds']:.2f}s")
                self.logger.info(f"Success Rate: {producer_metrics_summary['success_rate']:.3f}")
                self.logger.info(f"Throughput: {producer_metrics_summary['send_throughput_msg_per_sec']:.1f} msg/sec")
                self.logger.info(f"Latency: Avg={producer_metrics_summary['avg_latency_ms']:.1f}ms")

        # Enhanced assertions using metrics
        assert messages_sent > 0, "No messages were sent"
        assert metrics_summary["messages_delivered"] > 0, "No messages were delivered"
        assert metrics_summary["send_throughput_msg_per_sec"] > 5, (
            f"Send throughput too low for {compression_type}: "
            f"{metrics_summary['send_throughput_msg_per_sec']:.2f} msg/s"
        )

        # Validate against performance bounds
        if not is_valid:
            self.logger.error(
                "Performance bounds validation failed for %s compression: %s",
                compression_type,
                "; ".join(violations),
            )
            assert False, (
                f"Performance bounds validation failed for {compression_type} compression: " f"{'; '.join(violations)}"
            )

        self.logger.info(
            "Successfully completed %s compression test with comprehensive metrics",
            compression_type,
        )

    @matrix(producer_type=["sync", "async"], serialization_type=["avro", "json", "protobuf"])
    def test_basic_produce_with_schema_registry(self, producer_type, serialization_type):
        """
        Test producer with Schema Registry serialization.

        Note: in this test, we are producing messages with the same schema,
        a realistic high-throughput production scenario.
        As we have cache for schemas, only the first message will make HTTP calls to Schema Registry server.
        Performance impact comes from serialization overhead, not network calls.
        """

        topic_name = f"performance-test-produce-{serialization_type or 'plain'}-topic"
        test_duration = 5.0

        # Create topic and wait until ready
        self.kafka.create_topic(topic_name, partitions=1, replication_factor=1)
        topic_ready = self.kafka.wait_for_topic(topic_name, max_wait_time=30)
        assert topic_ready, f"Topic {topic_name} was not created within timeout"

        # Initialize metrics
        metrics = MetricsCollector()
        bounds = MetricsBounds()

        # Create appropriate producer strategy
        strategy = self.create_producer(producer_type)
        strategy.metrics = metrics

        # Start metrics collection
        metrics.start()

        # Message formatter - realistic pattern where messages use same schema
        def message_formatter(i):
            try:
                # Create message content based on serialization type
                if serialization_type == "protobuf":
                    from tests.integration.schema_registry.data.proto import (
                        PublicTestProto_pb2,
                    )

                    message_value = PublicTestProto_pb2.TestMessage(
                        test_string=f"User{i}",
                        test_bool=i % 2 == 0,
                        test_bytes=f"bytes{i}".encode("utf-8"),
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
                        test_uint64=i,
                    )
                elif serialization_type:  # Avro or JSON
                    # Match the Protobuf schema structure for Avro/JSON
                    # For JSON, convert bytes to base64 string
                    if serialization_type == "json":
                        test_bytes = f"bytes{i}"  # JSON uses string for bytes
                    else:
                        test_bytes = f"bytes{i}".encode("utf-8")  # Avro uses actual bytes

                    message_value = {
                        "test_string": f"User{i}",
                        "test_bool": i % 2 == 0,
                        "test_bytes": test_bytes,
                        "test_double": float(i),
                        "test_float": float(i),
                        "test_fixed32": i,
                        "test_fixed64": i,
                        "test_int32": i,
                        "test_int64": i,
                        "test_sfixed32": i,
                        "test_sfixed64": i,
                        "test_sint32": i,
                        "test_sint64": i,
                        "test_uint32": i,
                        "test_uint64": i,
                    }
                else:
                    # Plain messages - no complex structure needed
                    message_value = f"Test message {i}"  # Simple string message

                return (message_value, f"key-{i}")
            except Exception as e:
                self.logger.error(f"Error creating message {i}: {e}")
                return (f"Test message {i}", f"key-{i}")

        start_time = time.time()
        messages_sent = strategy.produce_messages(
            topic_name,
            test_duration,
            start_time,
            message_formatter,
            serialization_type,
        )

        # Finalize and validate metrics
        metrics.finalize()
        metrics_summary = metrics.get_summary()
        is_valid, violations = validate_metrics(metrics_summary, bounds)

        self.logger.info(f"=== {producer_type.upper()} {serialization_type.upper()} SR METRICS REPORT ===")
        print_metrics_report(metrics_summary, is_valid, violations)

        assert messages_sent > 0, "No messages were sent"
        assert metrics_summary["messages_delivered"] > 0, "No messages were delivered"
        assert (
            metrics_summary["send_throughput_msg_per_sec"] > 10
        ), f"Send throughput too low: {metrics_summary['send_throughput_msg_per_sec']:.2f} msg/s"

        if not is_valid:
            self.logger.error("Performance bounds validation failed: %s", "; ".join(violations))
            assert False, f"Performance bounds validation failed: {'; '.join(violations)}"

        self.logger.info("Successfully completed SR production test with comprehensive metrics")

    def tearDown(self):
        """Clean up test environment"""
        self.logger.info("Test completed - external Kafka service remains running")
