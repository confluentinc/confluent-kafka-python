"""
Kafka client wrapper for Ducktape tests
Assumes Kafka is already running on localhost:9092
"""
import time
from ducktape.services.service import Service


class KafkaClient(Service):
    """Kafka client wrapper - assumes external Kafka is running"""

    DEFAULT_TIMEOUT = 10

    def __init__(self, context, bootstrap_servers="localhost:9092"):
        # Use num_nodes=0 since we're not managing any nodes
        super(KafkaClient, self).__init__(context, num_nodes=0)
        self.bootstrap_servers_str = bootstrap_servers

    def start_node(self, node):
        """No-op since we assume Kafka is already running"""
        self.logger.info("Assuming Kafka is already running on %s", self.bootstrap_servers_str)

    def stop_node(self, node):
        """No-op since we don't manage the Kafka service"""
        self.logger.info("Not stopping external Kafka service")

    def clean_node(self, node):
        """No-op since we don't manage any files"""
        self.logger.info("No cleanup needed for external Kafka service")

    def bootstrap_servers(self):
        """Get bootstrap servers string for clients"""
        return self.bootstrap_servers_str

    def verify_connection(self):
        """Verify that Kafka is accessible"""
        try:
            from confluent_kafka.admin import AdminClient
            admin_client = AdminClient({'bootstrap.servers': self.bootstrap_servers_str})

            # Try to get cluster metadata to verify connection
            metadata = admin_client.list_topics(timeout=self.DEFAULT_TIMEOUT)
            self.logger.info("Successfully connected to Kafka. Available topics: %s",
                             list(metadata.topics.keys()))
            return True
        except Exception as e:
            self.logger.error("Failed to connect to Kafka: %s", e)
            return False

    def create_topic(self, topic, partitions=1, replication_factor=1):
        """Create a topic using Kafka admin client"""
        try:
            from confluent_kafka.admin import AdminClient, NewTopic

            admin_client = AdminClient({'bootstrap.servers': self.bootstrap_servers_str})

            topic_config = NewTopic(
                topic=topic,
                num_partitions=partitions,
                replication_factor=replication_factor
            )

            self.logger.info("Creating topic: %s (partitions=%d, replication=%d)",
                             topic, partitions, replication_factor)

            # Create topic
            fs = admin_client.create_topics([topic_config])

            # Wait for topic creation to complete
            for topic_name, f in fs.items():
                try:
                    f.result(timeout=30)  # Wait up to 30 seconds
                    self.logger.info("Topic %s created successfully", topic_name)
                except Exception as e:
                    if "already exists" in str(e).lower():
                        self.logger.info("Topic %s already exists", topic_name)
                    else:
                        self.logger.warning("Failed to create topic %s: %s", topic_name, e)

        except ImportError:
            self.logger.error("confluent_kafka not available for topic creation")
        except Exception as e:
            self.logger.error("Failed to create topic %s: %s", topic, e)

    def list_topics(self):
        """List all topics using admin client"""
        try:
            from confluent_kafka.admin import AdminClient

            admin_client = AdminClient({'bootstrap.servers': self.bootstrap_servers_str})
            metadata = admin_client.list_topics(timeout=self.DEFAULT_TIMEOUT)

            topics = list(metadata.topics.keys())
            self.logger.debug("Available topics: %s", topics)
            return topics

        except ImportError:
            self.logger.error("confluent_kafka not available for listing topics")
            return []
        except Exception as e:
            self.logger.error("Failed to list topics: %s", e)
            return []

    def wait_for_topic(self, topic_name, max_wait_time=30, initial_wait=0.1):
        """
        Wait for topic to be created with exponential backoff retry logic.
        """
        wait_time = initial_wait
        total_wait = 0

        self.logger.info("Waiting for topic '%s' to be available...", topic_name)

        while total_wait < max_wait_time:
            topics = self.list_topics()
            if topic_name in topics:
                self.logger.info("Topic '%s' is ready after %.2fs", topic_name, total_wait)
                return True

            self.logger.debug("Topic '%s' not found, waiting %.2fs (total: %.2fs)",
                              topic_name, wait_time, total_wait)
            time.sleep(wait_time)
            total_wait += wait_time

            # Exponential backoff with max cap of 2 seconds
            wait_time = min(wait_time * 2, 2.0)

        self.logger.error("Timeout waiting for topic '%s' after %ds", topic_name, max_wait_time)
        return False

    def add_partitions(self, topic_name, new_partition_count):
        """Add partitions to an existing topic"""
        try:
            from confluent_kafka.admin import AdminClient, NewPartitions

            admin_client = AdminClient({'bootstrap.servers': self.bootstrap_servers_str})
            metadata = admin_client.list_topics(timeout=self.DEFAULT_TIMEOUT)

            if topic_name not in metadata.topics:
                raise ValueError(f"Topic {topic_name} does not exist")

            current_partitions = len(metadata.topics[topic_name].partitions)
            if new_partition_count <= current_partitions:
                return  # No change needed

            # Add partitions
            new_partitions = NewPartitions(topic=topic_name, new_total_count=new_partition_count)
            fs = admin_client.create_partitions([new_partitions])

            # Wait for completion
            for topic, f in fs.items():
                f.result(timeout=30)

        except Exception as e:
            self.logger.error("Failed to add partitions to topic %s: %s", topic_name, e)
            raise
