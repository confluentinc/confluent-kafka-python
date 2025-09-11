"""
Ducktape test for Confluent Kafka Python Producer
Assumes Kafka is already running on localhost:9092
"""
import time
from ducktape.tests.test import Test
from ducktape.mark import matrix, parametrize

from tests.ducktape.services.kafka import KafkaClient
from tests.ducktape.benchmark_metrics import MetricsCollector, MetricsBounds, validate_metrics, print_metrics_report
from tests.ducktape.producer_strategy import SyncProducerStrategy, AsyncProducerStrategy


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
            raise Exception("Cannot connect to Kafka at localhost:9092. "
                            "Please ensure Kafka is running.")

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
        assert topic_ready, (f"Topic {topic_name} was not created within timeout. "
                             f"Available topics: {self.kafka.list_topics()}")

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

        # Containers for results
        delivered_messages = []
        failed_messages = []

        # Run the test
        start_time = time.time()
        messages_sent = strategy.produce_messages(
            topic_name, test_duration, start_time, message_formatter,
            delivered_messages, failed_messages
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
        assert len(delivered_messages) > 0, "No messages were delivered"
        assert metrics_summary['messages_delivered'] > 0, "No messages were delivered (metrics)"
        assert metrics_summary['send_throughput_msg_per_sec'] > 10, \
            f"Send throughput too low: {metrics_summary['send_throughput_msg_per_sec']:.2f} msg/s"

        # Validate against performance bounds
        if not is_valid:
            self.logger.warning("Performance bounds validation failed: %s", "; ".join(violations))

        self.logger.info("Successfully completed basic production test with comprehensive metrics")

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

        # Containers for results
        delivered_messages = []
        failed_messages = []

        # Run the test
        start_time = time.time()
        messages_sent = strategy.produce_messages(
            topic_name, test_duration, start_time, message_formatter,
            delivered_messages, failed_messages
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
                self.logger.info(f"=== Producer Built-in Metrics ===")
                self.logger.info(f"Runtime: {producer_metrics_summary['duration_seconds']:.2f}s")
                self.logger.info(f"Success Rate: {producer_metrics_summary['success_rate']:.3f}")
                self.logger.info(f"Throughput: {producer_metrics_summary['send_throughput_msg_per_sec']:.1f} msg/sec")
                self.logger.info(f"Latency: Avg={producer_metrics_summary['avg_latency_ms']:.1f}ms")

        # Enhanced assertions using metrics
        assert messages_sent > 0, "No messages were sent"
        assert len(delivered_messages) > 0, "No messages were delivered"
        assert metrics_summary['messages_delivered'] > 0, "No messages were delivered (metrics)"
        assert metrics_summary['send_throughput_msg_per_sec'] > 10, \
            f"Send throughput too low: {metrics_summary['send_throughput_msg_per_sec']:.2f} msg/s"

        # Validate against performance bounds
        if not is_valid:
            self.logger.warning("Performance bounds validation failed for %ds test: %s",
                                test_duration, "; ".join(violations))

        self.logger.info("Successfully completed %ds batch production test with comprehensive metrics", test_duration)

    @matrix(producer_type=["sync", "async"], compression_type=['none', 'gzip', 'snappy'])
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
        if compression_type != 'none':
            compression_config['compression.type'] = compression_type
        
        # Configure polling intervals based on compression type and producer type
        if producer_type == 'async':
            polling_config = {
                'gzip': 10,     # Poll every 10 messages for gzip (frequent)
                'snappy': 50,   # Poll every 50 messages for snappy (moderate)
                'none': 100     # Poll every 100 messages for none (standard)
            }
        else:  # sync
            # Sync producers need more frequent polling to prevent buffer overflow as throughput is very high
            polling_config = {
                'gzip': 5,      # Poll every 5 messages for gzip (most frequent)
                'snappy': 25,   # Poll every 25 messages for snappy (moderate)
                'none': 50      # Poll every 50 messages for none (standard)
            }
        poll_interval = polling_config.get(compression_type, 50 if producer_type == 'sync' else 100)
        
        strategy = self.create_producer(producer_type, compression_config)
        strategy.poll_interval = poll_interval

        # Assign metrics collector to strategy
        strategy.metrics = metrics

        self.logger.info(f"Testing {producer_type} producer with {compression_type} compression for {test_duration} seconds")
        self.logger.info(f"Using polling interval: {poll_interval} messages per poll")

        # Start metrics collection
        metrics.start()

        # Create larger messages to test compression effectiveness
        large_message = "x" * 1000  # 1KB message

        # Message formatter for compression test
        def message_formatter(msg_num):
            return f"{large_message}-{msg_num}", f"comp-key-{msg_num}"

        # Containers for results
        delivered_messages = []
        failed_messages = []

        # Run the test
        start_time = time.time()
        messages_sent = strategy.produce_messages(
            topic_name, test_duration, start_time, message_formatter,
            delivered_messages, failed_messages
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
                self.logger.info(f"=== Producer Built-in Metrics ===")
                self.logger.info(f"Runtime: {producer_metrics_summary['duration_seconds']:.2f}s")
                self.logger.info(f"Success Rate: {producer_metrics_summary['success_rate']:.3f}")
                self.logger.info(f"Throughput: {producer_metrics_summary['send_throughput_msg_per_sec']:.1f} msg/sec")
                self.logger.info(f"Latency: Avg={producer_metrics_summary['avg_latency_ms']:.1f}ms")

        # Enhanced assertions using metrics
        assert messages_sent > 0, "No messages were sent"
        assert len(delivered_messages) > 0, "No messages were delivered"
        assert metrics_summary['messages_delivered'] > 0, "No messages were delivered (metrics)"
        assert metrics_summary['send_throughput_msg_per_sec'] > 5, \
            f"Send throughput too low for {compression_type}: " \
            f"{metrics_summary['send_throughput_msg_per_sec']:.2f} msg/s"

        # Validate against performance bounds
        if not is_valid:
            self.logger.warning("Performance bounds validation failed for %s compression: %s",
                                compression_type, "; ".join(violations))

        self.logger.info("Successfully completed %s compression test with comprehensive metrics", compression_type)

    def tearDown(self):
        """Clean up test environment"""
        self.logger.info("Test completed - external Kafka service remains running")
