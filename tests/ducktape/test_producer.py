"""
Ducktape test for Confluent Kafka Python Producer
Assumes Kafka is already running on localhost:9092
"""
import time
from ducktape.tests.test import Test
from ducktape.mark import matrix, parametrize

from tests.ducktape.services.kafka import KafkaClient
from tests.ducktape.benchmark_metrics import MetricsCollector, MetricsBounds, validate_metrics, print_metrics_report
from confluent_kafka import Producer


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

    def test_basic_produce(self):
        """Test basic message production with comprehensive metrics and bounds validation"""

        topic_name = "test-topic"
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

        # Configure producer
        producer_config = {
            'bootstrap.servers': self.kafka.bootstrap_servers(),
            'client.id': 'ducktape-test-producer'
        }

        self.logger.info("Creating producer with config: %s", producer_config)
        producer = Producer(producer_config)

        # Enhanced delivery callback with metrics tracking
        send_times = {}  # Track send times for latency calculation

        def delivery_callback(err, msg):
            """Delivery report callback with metrics tracking"""
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

        # Time-based message production with metrics
        self.logger.info("Producing messages with metrics for %.1f seconds to topic %s", test_duration, topic_name)
        start_time = time.time()
        messages_sent = 0

        while time.time() - start_time < test_duration:
            message_value = f"Test message {messages_sent}"
            message_key = f"key-{messages_sent}"

            try:
                # Record message being sent with metrics
                message_size = len(message_value.encode('utf-8')) + len(message_key.encode('utf-8'))
                metrics.record_sent(message_size, topic=topic_name, partition=0)

                # Track send time for latency calculation
                send_times[message_key] = time.time()

                producer.produce(
                    topic=topic_name,
                    value=message_value,
                    key=message_key,
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

        # Get comprehensive metrics summary
        metrics_summary = metrics.get_summary()
        is_valid, violations = validate_metrics(metrics_summary, bounds)

        # Print comprehensive metrics report
        self.logger.info("Basic production test with metrics completed:")
        print_metrics_report(metrics_summary, is_valid, violations)

        # Enhanced assertions using metrics
        assert messages_sent > 0, "No messages were sent during test duration"
        assert metrics_summary['messages_delivered'] > 0, "No messages were delivered"
        assert metrics_summary['send_throughput_msg_per_sec'] > 10, \
            f"Send throughput too low: {metrics_summary['send_throughput_msg_per_sec']:.2f} msg/s " \
            f"(expected > 10 msg/s)"

        # Validate against performance bounds
        if not is_valid:
            self.logger.error("Performance bounds validation failed: %s", "; ".join(violations))
            assert False, f"Performance bounds validation failed: {'; '.join(violations)}"

        self.logger.info("Successfully completed basic production test with comprehensive metrics")

    @parametrize(test_duration=2)
    @parametrize(test_duration=5)
    @parametrize(test_duration=10)
    def test_produce_multiple_batches(self, test_duration):
        """Test batch throughput with comprehensive metrics and bounds validation"""

        topic_name = f"batch-test-topic-{test_duration}s"

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
            bounds.min_throughput_msg_per_sec = 500.0  # Lower threshold for short tests

        # Configure producer with batch settings
        producer_config = {
            'bootstrap.servers': self.kafka.bootstrap_servers(),
            'client.id': f'batch-test-producer-{test_duration}s',
            'batch.size': 1000,  # Small batch size for testing
            'linger.ms': 100     # Small linger time for testing
        }

        producer = Producer(producer_config)

        # Enhanced delivery callback with metrics
        send_times = {}

        def delivery_callback(err, msg):
            if err is None:
                # Calculate latency
                msg_key = msg.key().decode('utf-8', errors='replace') if msg.key() else 'unknown'
                if msg_key in send_times:
                    latency_ms = (time.time() - send_times[msg_key]) * 1000
                    del send_times[msg_key]
                else:
                    latency_ms = 0.0  # Default for batch processing

                metrics.record_delivered(latency_ms, topic=msg.topic(), partition=msg.partition())
            else:
                self.logger.error("Delivery failed: %s", err)
                metrics.record_failed(topic=msg.topic() if msg else topic_name,
                                      partition=msg.partition() if msg else 0)

        # Start metrics collection
        metrics.start()

        # Time-based batch message production with metrics
        self.logger.info("Producing batches with metrics for %d seconds", test_duration)
        start_time = time.time()
        messages_sent = 0

        while time.time() - start_time < test_duration:
            try:
                message_value = f"Batch message {messages_sent}"
                message_key = f"batch-key-{messages_sent % 10}"  # Use modulo for key distribution
                partition = messages_sent % 2  # Distribute across partitions

                # Record metrics
                message_size = len(message_value.encode('utf-8')) + len(message_key.encode('utf-8'))
                metrics.record_sent(message_size, topic=topic_name, partition=partition)
                send_times[message_key] = time.time()

                producer.produce(
                    topic=topic_name,
                    value=message_value,
                    key=message_key,
                    callback=delivery_callback
                )
                messages_sent += 1

                # Poll occasionally to trigger callbacks and record polls
                if messages_sent % 100 == 0:
                    producer.poll(0)
                    metrics.record_poll()

            except BufferError:
                # Record buffer full events
                metrics.record_buffer_full()
                producer.poll(0.001)
                continue

        # Final flush
        producer.flush(timeout=30)

        # Finalize metrics
        metrics.finalize()

        # Get comprehensive metrics summary
        metrics_summary = metrics.get_summary()
        is_valid, violations = validate_metrics(metrics_summary, bounds)

        # Print comprehensive metrics report
        self.logger.info("Batch production test (%ds) with metrics completed:", test_duration)
        print_metrics_report(metrics_summary, is_valid, violations)

        # Enhanced assertions using metrics
        assert messages_sent > 0, "No messages were sent during test duration"
        assert metrics_summary['messages_delivered'] > 0, "No messages were delivered"
        assert metrics_summary['send_throughput_msg_per_sec'] > 10, \
            f"Send throughput too low: {metrics_summary['send_throughput_msg_per_sec']:.2f} msg/s"

        # Validate against performance bounds
        if not is_valid:
            self.logger.error("Performance bounds validation failed for %ds test: %s",
                              test_duration, "; ".join(violations))
            assert False, f"Performance bounds validation failed for {test_duration}s test: {'; '.join(violations)}"

        self.logger.info("Successfully completed %ds batch production test with comprehensive metrics", test_duration)

    @matrix(compression_type=['none', 'gzip', 'snappy'])
    def test_produce_with_compression(self, compression_type):
        """Test compression throughput with comprehensive metrics and bounds validation"""

        topic_name = f"compression-test-{compression_type}"
        test_duration = 5.0  # 5 seconds

        # Create topic
        self.kafka.create_topic(topic_name, partitions=1, replication_factor=1)

        # Wait for topic to be available with retry logic
        topic_ready = self.kafka.wait_for_topic(topic_name, max_wait_time=30)
        assert topic_ready, f"Topic {topic_name} was not created within timeout"

        # Initialize metrics collection and bounds
        metrics = MetricsCollector()
        bounds = MetricsBounds()
        # Adjust bounds for compression tests (may be slower)
        bounds.min_throughput_msg_per_sec = 5.0  # Lower threshold for large messages
        bounds.max_p95_latency_ms = 5000.0  # Allow higher latency for compression

        # Configure producer with compression
        producer_config = {
            'bootstrap.servers': self.kafka.bootstrap_servers(),
            'client.id': f'compression-test-{compression_type}',
            'compression.type': compression_type
        }

        producer = Producer(producer_config)

        # Create larger messages to test compression effectiveness
        large_message = "x" * 1000  # 1KB message
        send_times = {}

        def delivery_callback(err, msg):
            if err is None:
                # Calculate latency
                msg_key = msg.key().decode('utf-8', errors='replace') if msg.key() else 'unknown'
                if msg_key in send_times:
                    latency_ms = (time.time() - send_times[msg_key]) * 1000
                    del send_times[msg_key]
                else:
                    latency_ms = 0.0  # Default for compression processing

                metrics.record_delivered(latency_ms, topic=msg.topic(), partition=msg.partition())
            else:
                metrics.record_failed(topic=msg.topic() if msg else topic_name,
                                      partition=msg.partition() if msg else 0)

        # Start metrics collection
        metrics.start()

        # Time-based message production with compression and metrics
        self.logger.info("Producing messages with %s compression and metrics for %.1f seconds",
                         compression_type, test_duration)
        start_time = time.time()
        messages_sent = 0

        while time.time() - start_time < test_duration:
            try:
                message_value = f"{large_message}-{messages_sent}"
                message_key = f"comp-key-{messages_sent}"

                # Record metrics
                message_size = len(message_value.encode('utf-8')) + len(message_key.encode('utf-8'))
                metrics.record_sent(message_size, topic=topic_name, partition=0)
                send_times[message_key] = time.time()

                producer.produce(
                    topic=topic_name,
                    value=message_value,
                    key=message_key,
                    callback=delivery_callback
                )
                messages_sent += 1

                # Poll frequently to prevent buffer overflow and record polls
                if messages_sent % 10 == 0:
                    producer.poll(0)
                    metrics.record_poll()

            except BufferError:
                # Record buffer full events
                metrics.record_buffer_full()
                producer.poll(0.001)
                continue

        producer.flush(timeout=30)

        # Finalize metrics
        metrics.finalize()

        # Get comprehensive metrics summary
        metrics_summary = metrics.get_summary()
        is_valid, violations = validate_metrics(metrics_summary, bounds)

        # Print comprehensive metrics report
        self.logger.info("Compression test (%s) with metrics completed:", compression_type)
        print_metrics_report(metrics_summary, is_valid, violations)

        # Enhanced assertions using metrics
        assert messages_sent > 0, "No messages were sent during test duration"
        assert metrics_summary['messages_delivered'] > 0, "No messages were delivered"
        assert metrics_summary['send_throughput_msg_per_sec'] > 5, \
            f"Send throughput too low for {compression_type}: " \
            f"{metrics_summary['send_throughput_msg_per_sec']:.2f} msg/s"

        # Validate against performance bounds
        if not is_valid:
            self.logger.error("Performance bounds validation failed for %s compression: %s",
                              compression_type, "; ".join(violations))
            assert False, f"Performance bounds validation failed for {compression_type} compression: {'; '.join(violations)}"

        self.logger.info("Successfully completed %s compression test with comprehensive metrics", compression_type)

    def tearDown(self):
        """Clean up test environment"""
        self.logger.info("Test completed - external Kafka service remains running")
