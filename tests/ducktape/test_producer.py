"""
Ducktape test for Confluent Kafka Python Producer
Assumes Kafka is already running on localhost:9092
"""
import time
from ducktape.tests.test import Test
from ducktape.mark import matrix, parametrize

from tests.ducktape.services.kafka import KafkaClient

try:
    from confluent_kafka import Producer, KafkaError
except ImportError:
    # Handle case where confluent_kafka is not installed
    Producer = None
    KafkaError = None


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
        """Test basic message production throughput over 5 seconds (time-based)"""
        if Producer is None:
            self.logger.error("confluent_kafka not available, skipping test")
            return

        topic_name = "test-topic"
        test_duration = 5.0  # 5 seconds

        # Create topic
        self.kafka.create_topic(topic_name, partitions=1, replication_factor=1)

        # Wait for topic to be available with retry logic
        topic_ready = self.kafka.wait_for_topic(topic_name, max_wait_time=30)
        assert topic_ready, (f"Topic {topic_name} was not created within timeout. "
                             f"Available topics: {self.kafka.list_topics()}")

        # Configure producer
        producer_config = {
            'bootstrap.servers': self.kafka.bootstrap_servers(),
            'client.id': 'ducktape-test-producer'
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

        # Time-based message production
        self.logger.info("Producing messages for %.1f seconds to topic %s", test_duration, topic_name)
        start_time = time.time()
        messages_sent = 0

        while time.time() - start_time < test_duration:
            message_value = f"Test message {messages_sent}"
            message_key = f"key-{messages_sent}"

            try:
                producer.produce(
                    topic=topic_name,
                    value=message_value,
                    key=message_key,
                    callback=delivery_callback
                )
                messages_sent += 1

                # Poll frequently to trigger delivery callbacks
                if messages_sent % 100 == 0:
                    producer.poll(0)
            except BufferError:
                # If buffer is full, poll and wait briefly
                producer.poll(0.001)
                continue

        actual_duration = time.time() - start_time

        # Flush to ensure all messages are sent
        self.logger.info("Flushing producer...")
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

        # Basic performance assertions
        assert messages_sent > 0, "No messages were sent during test duration"
        assert len(delivered_messages) > 0, "No messages were delivered"
        assert send_throughput > 10, f"Send throughput too low: {send_throughput:.2f} msg/s (expected > 10 msg/s)"

        self.logger.info("Successfully completed time-based basic production test")

    @parametrize(test_duration=2)
    @parametrize(test_duration=5)
    @parametrize(test_duration=10)
    def test_produce_multiple_batches(self, test_duration):
        """Test batch throughput over different time durations (time-based)"""
        if Producer is None:
            self.logger.error("confluent_kafka not available, skipping test")
            return

        topic_name = f"batch-test-topic-{test_duration}s"

        # Create topic
        self.kafka.create_topic(topic_name, partitions=2, replication_factor=1)

        # Wait for topic to be available with retry logic
        topic_ready = self.kafka.wait_for_topic(topic_name, max_wait_time=30)
        assert topic_ready, f"Topic {topic_name} was not created within timeout"

        # Configure producer with batch settings
        producer_config = {
            'bootstrap.servers': self.kafka.bootstrap_servers(),
            'client.id': f'batch-test-producer-{test_duration}s',
            'batch.size': 1000,  # Small batch size for testing
            'linger.ms': 100     # Small linger time for testing
        }

        producer = Producer(producer_config)

        delivered_count = [0]  # Use list to modify from callback

        def delivery_callback(err, msg):
            if err is None:
                delivered_count[0] += 1
            else:
                self.logger.error("Delivery failed: %s", err)

        # Time-based batch message production
        self.logger.info("Producing batches for %d seconds", test_duration)
        start_time = time.time()
        messages_sent = 0

        while time.time() - start_time < test_duration:
            try:
                producer.produce(
                    topic=topic_name,
                    value=f"Batch message {messages_sent}",
                    key=f"batch-key-{messages_sent % 10}",  # Use modulo for key distribution
                    callback=delivery_callback
                )
                messages_sent += 1

                # Poll occasionally to trigger callbacks
                if messages_sent % 100 == 0:
                    producer.poll(0)
            except BufferError:
                # If buffer is full, poll and wait briefly
                producer.poll(0.001)
                continue

        actual_duration = time.time() - start_time

        # Final flush
        producer.flush(timeout=30)

        # Calculate throughput
        send_throughput = messages_sent / actual_duration
        delivery_throughput = delivered_count[0] / actual_duration

        # Verify results
        self.logger.info("Batch production results (%ds):", test_duration)
        self.logger.info("  Duration: %.2f seconds", actual_duration)
        self.logger.info("  Messages sent: %d", messages_sent)
        self.logger.info("  Messages delivered: %d", delivered_count[0])
        self.logger.info("  Send throughput: %.2f msg/s", send_throughput)
        self.logger.info("  Delivery throughput: %.2f msg/s", delivery_throughput)

        # Performance assertions
        assert messages_sent > 0, "No messages were sent during test duration"
        assert delivered_count[0] > 0, "No messages were delivered"
        assert send_throughput > 10, f"Send throughput too low: {send_throughput:.2f} msg/s"

        self.logger.info("Successfully completed %ds batch production test", test_duration)

    @matrix(compression_type=['none', 'gzip', 'snappy'])
    def test_produce_with_compression(self, compression_type):
        """Test compression throughput over 5 seconds (time-based)"""
        if Producer is None:
            self.logger.error("confluent_kafka not available, skipping test")
            return

        topic_name = f"compression-test-{compression_type}"
        test_duration = 5.0  # 5 seconds

        # Create topic
        self.kafka.create_topic(topic_name, partitions=1, replication_factor=1)

        # Wait for topic to be available with retry logic
        topic_ready = self.kafka.wait_for_topic(topic_name, max_wait_time=30)
        assert topic_ready, f"Topic {topic_name} was not created within timeout"

        # Configure producer with compression
        producer_config = {
            'bootstrap.servers': self.kafka.bootstrap_servers(),
            'client.id': f'compression-test-{compression_type}',
            'compression.type': compression_type
        }

        producer = Producer(producer_config)

        # Create larger messages to test compression effectiveness
        large_message = "x" * 1000  # 1KB message
        delivered_count = [0]

        def delivery_callback(err, msg):
            if err is None:
                delivered_count[0] += 1

        # Time-based message production with compression
        self.logger.info("Producing messages with %s compression for %.1f seconds", compression_type, test_duration)
        start_time = time.time()
        messages_sent = 0

        while time.time() - start_time < test_duration:
            try:
                producer.produce(
                    topic=topic_name,
                    value=f"{large_message}-{messages_sent}",
                    key=f"comp-key-{messages_sent}",
                    callback=delivery_callback
                )
                messages_sent += 1

                # Poll frequently to prevent buffer overflow
                if messages_sent % 10 == 0:
                    producer.poll(0)
            except BufferError:
                # If buffer is full, poll and wait briefly
                producer.poll(0.001)
                continue

        actual_duration = time.time() - start_time

        producer.flush(timeout=30)

        # Calculate throughput
        send_throughput = messages_sent / actual_duration
        delivery_throughput = delivered_count[0] / actual_duration
        message_size_kb = len(f"{large_message}-{messages_sent}") / 1024
        throughput_mb_s = (delivery_throughput * message_size_kb) / 1024

        # Verify results
        self.logger.info("Compression production results (%s):", compression_type)
        self.logger.info("  Duration: %.2f seconds", actual_duration)
        self.logger.info("  Messages sent: %d", messages_sent)
        self.logger.info("  Messages delivered: %d", delivered_count[0])
        self.logger.info("  Message size: %.1f KB", message_size_kb)
        self.logger.info("  Send throughput: %.2f msg/s", send_throughput)
        self.logger.info("  Delivery throughput: %.2f msg/s", delivery_throughput)
        self.logger.info("  Data throughput: %.2f MB/s", throughput_mb_s)

        # Performance assertions
        assert messages_sent > 0, "No messages were sent during test duration"
        assert delivered_count[0] > 0, "No messages were delivered"
        assert send_throughput > 5, f"Send throughput too low for {compression_type}: {send_throughput:.2f} msg/s"

        self.logger.info("Successfully completed %s compression test", compression_type)

    def tearDown(self):
        """Clean up test environment"""
        self.logger.info("Test completed - external Kafka service remains running")
