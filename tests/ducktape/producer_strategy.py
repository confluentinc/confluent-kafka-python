"""
Producer strategies for testing sync and async Kafka producers.

This module contains strategy classes that encapsulate the different producer
implementations (sync vs async) with consistent interfaces for testing.
"""


class ProducerStrategy:
    """Base class for producer strategies"""
    def __init__(self, bootstrap_servers, logger):
        self.bootstrap_servers = bootstrap_servers
        self.logger = logger
        self.metrics = None

    def create_producer(self):
        raise NotImplementedError()

    def produce_messages(
        self,
        topic_name,
        test_duration,
        start_time,
        message_formatter,
        delivered_container,
        failed_container=None,
    ):
        raise NotImplementedError()


class AsyncProducerStrategy(ProducerStrategy):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._producer_instance = None

    def create_producer(self, config_overrides=None):
        from confluent_kafka.aio import AIOProducer
        # Enable logging for AIOProducer
        import logging
        logging.basicConfig(level=logging.INFO)

        config = {
            'bootstrap.servers': self.bootstrap_servers,
            # Optimized configuration for low-latency, high-throughput (same as async)
            'queue.buffering.max.messages': 1000000,  # 1M messages (sufficient)
            'queue.buffering.max.kbytes': 1048576,    # 1GB (default)
            'batch.size': 65536,                      # 64KB batches
            'batch.num.messages': 50000,              # 50K messages per batch
            'message.max.bytes': 2097152,             # 2MB max message size
            'linger.ms': 1,                           # Low latency
            'compression.type': 'lz4',                # Fast compression
            'acks': 1,                                # Leader only
            'retries': 3,                             # Retry failed sends
            'delivery.timeout.ms': 30000,             # 30s
            'max.in.flight.requests.per.connection': 5
        }

        # Apply any test-specific overrides
        if config_overrides:
            config.update(config_overrides)

        # Get producer configuration from strategy attributes
        max_workers = getattr(self, 'max_workers', 4)
        batch_size = getattr(self, 'batch_size', 1000)  # Optimal batch size for low latency

        # Use updated defaults with configurable parameters
        self._producer_instance = AIOProducer(config, max_workers=max_workers)

        # Log the configuration for validation
        self.logger.info("=== ASYNC PRODUCER CONFIGURATION ===")
        for key, value in config.items():
            self.logger.info(f"{key}: {value}")
        self.logger.info(f"max_workers: {max_workers}")
        self.logger.info(f"batch_size: {batch_size}")
        self.logger.info("=" * 41)

        return self._producer_instance
