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
        """Test basic message production to a topic"""
        if Producer is None:
            self.logger.error("confluent_kafka not available, skipping test")
            return
            
        topic_name = "test-topic"
        num_messages = 10
        
        # Create topic
        self.kafka.create_topic(topic_name, partitions=1, replication_factor=1)
        
        # Wait for topic to be created
        time.sleep(2)
        
        # Verify topic exists
        topics = self.kafka.list_topics()
        assert topic_name in topics, f"Topic {topic_name} was not created. Available topics: {topics}"
        
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
                self.logger.debug("Message delivered to %s [%d] at offset %d",
                                msg.topic(), msg.partition(), msg.offset())
                delivered_messages.append(msg)
        
        # Produce messages
        self.logger.info("Producing %d messages to topic %s", num_messages, topic_name)
        for i in range(num_messages):
            message_value = f"Test message {i}"
            message_key = f"key-{i}"
            
            producer.produce(
                topic=topic_name,
                value=message_value,
                key=message_key,
                callback=delivery_callback
            )
            
            # Poll to trigger delivery callbacks
            producer.poll(0)
            
        # Flush to ensure all messages are sent
        self.logger.info("Flushing producer...")
        producer.flush(timeout=30)
        
        # Verify all messages were delivered
        self.logger.info("Delivered: %d, Failed: %d", len(delivered_messages), len(failed_messages))
        
        assert len(failed_messages) == 0, f"Some messages failed to deliver: {failed_messages}"
        assert len(delivered_messages) == num_messages, \
            f"Expected {num_messages} delivered messages, got {len(delivered_messages)}"
            
        self.logger.info("Successfully produced %d messages to topic %s", 
                        len(delivered_messages), topic_name)
                        
    @parametrize(num_messages=5)
    @parametrize(num_messages=10)
    @parametrize(num_messages=50)
    def test_produce_multiple_batches(self, num_messages):
        """Test producing different numbers of messages"""
        if Producer is None:
            self.logger.error("confluent_kafka not available, skipping test")
            return
            
        topic_name = f"batch-test-topic-{num_messages}"
        
        # Create topic
        self.kafka.create_topic(topic_name, partitions=2, replication_factor=1)
        time.sleep(2)
        
        # Configure producer
        producer_config = {
            'bootstrap.servers': self.kafka.bootstrap_servers(),
            'client.id': f'batch-test-producer-{num_messages}',
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
                
        # Produce messages
        self.logger.info("Producing %d messages in batches", num_messages)
        for i in range(num_messages):
            producer.produce(
                topic=topic_name,
                value=f"Batch message {i}",
                key=f"batch-key-{i % 10}",  # Use modulo for key distribution
                callback=delivery_callback
            )
            
            # Poll occasionally
            if i % 10 == 0:
                producer.poll(0)
                
        # Final flush
        producer.flush(timeout=30)
        
        # Verify results
        assert delivered_count[0] == num_messages, \
            f"Expected {num_messages} delivered, got {delivered_count[0]}"
            
        self.logger.info("Successfully produced %d messages in batches", delivered_count[0])
        
    @matrix(compression_type=['none', 'gzip', 'snappy'])
    def test_produce_with_compression(self, compression_type):
        """Test producing messages with different compression types"""
        if Producer is None:
            self.logger.error("confluent_kafka not available, skipping test")
            return
            
        topic_name = f"compression-test-{compression_type}"
        
        # Create topic
        self.kafka.create_topic(topic_name, partitions=1, replication_factor=1)
        time.sleep(2)
        
        # Configure producer with compression
        producer_config = {
            'bootstrap.servers': self.kafka.bootstrap_servers(),
            'client.id': f'compression-test-{compression_type}',
            'compression.type': compression_type
        }
        
        producer = Producer(producer_config)
        
        # Create larger messages to test compression
        large_message = "x" * 1000  # 1KB message
        delivered_count = [0]
        
        def delivery_callback(err, msg):
            if err is None:
                delivered_count[0] += 1
                
        # Produce messages
        self.logger.info("Producing messages with %s compression", compression_type)
        for i in range(20):
            producer.produce(
                topic=topic_name,
                value=f"{large_message}-{i}",
                key=f"comp-key-{i}",
                callback=delivery_callback
            )
            producer.poll(0)
            
        producer.flush(timeout=30)
        
        assert delivered_count[0] == 20, \
            f"Expected 20 delivered messages, got {delivered_count[0]}"
            
        self.logger.info("Successfully produced 20 messages with %s compression", compression_type)
        
    def tearDown(self):
        """Clean up test environment"""
        self.logger.info("Test completed - external Kafka service remains running")
