"""
Producer strategies for testing sync and async Kafka producers.

This module contains strategy classes that encapsulate the different producer
implementations (sync vs async) with consistent interfaces for testing.
"""
import time
import asyncio
import uuid
from confluent_kafka import Producer
import json
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry._sync.json_schema import JSONSerializer
from confluent_kafka.schema_registry._sync.protobuf import ProtobufSerializer
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.schema_registry._async.json_schema import AsyncJSONSerializer
from confluent_kafka.schema_registry._async.protobuf import AsyncProtobufSerializer
from confluent_kafka.schema_registry._async.avro import AsyncAvroSerializer
from confluent_kafka.schema_registry.schema_registry_client import AsyncSchemaRegistryClient
from confluent_kafka.serialization import MessageField, SerializationContext, StringSerializer
from tests.integration.schema_registry.data.proto import PublicTestProto_pb2
import os


class ProducerStrategy:
    """Base class for producer strategies"""

    def __init__(self, bootstrap_servers, logger):
        self.bootstrap_servers = bootstrap_servers
        self.logger = logger
        self.metrics = None

    def create_producer(self):
        raise NotImplementedError()

    def produce_messages(self, topic_name, test_duration, start_time, message_formatter,
                         delivered_container, failed_container=None, serialization_type=None):
        raise NotImplementedError()

    def _get_base_config(self):
        """Get shared Kafka producer configuration optimized for low-latency, high-throughput"""
        return {
            'bootstrap.servers': self.bootstrap_servers,
            'queue.buffering.max.messages': 1000000,  # 1M messages (sufficient)
            'queue.buffering.max.kbytes': 1048576,    # 1GB (default)
            'batch.size': 65536,                      # 64KB batches (increased for better efficiency)
            'batch.num.messages': 50000,              # 50K messages per batch (up from 10K default)
            'message.max.bytes': 2097152,             # 2MB max message size (up from ~1MB default)
            'linger.ms': 1,                          # Wait 1ms for batching (low latency)
            'compression.type': 'lz4',               # Fast compression
            'acks': 1,                               # Wait for leader only (faster)
            'retries': 3,                            # Retry failed sends
            'delivery.timeout.ms': 30000,            # 30s delivery timeout
            'max.in.flight.requests.per.connection': 5  # Pipeline requests
        }

    def _log_configuration(self, config, producer_type, extra_params=None):
        """Log producer configuration for validation"""
        separator = "=" * (len(f"{producer_type.upper()} PRODUCER CONFIGURATION") + 6)
        self.logger.info(f"=== {producer_type.upper()} PRODUCER CONFIGURATION ===")
        for key, value in config.items():
            self.logger.info(f"{key}: {value}")

        if extra_params:
            for key, value in extra_params.items():
                self.logger.info(f"{key}: {value}")

        self.logger.info(separator)

    def _print_timing_metrics(self, producer_type, produce_times, poll_times, flush_time):
        """Print code path timing metrics"""
        avg_produce_time = sum(produce_times) / len(produce_times) * 1000 if produce_times else 0
        avg_poll_time = sum(poll_times) / len(poll_times) * 1000 if poll_times else 0
        flush_time_ms = flush_time * 1000

        separator = "=" * (len(f"{producer_type.upper()} PRODUCER CODE PATH TIMING") + 6)
        print(f"\n=== {producer_type.upper()} PRODUCER CODE PATH TIMING ===")
        print(f"Time to call {'AIO' if producer_type == 'ASYNC' else ''}Producer.produce(): {avg_produce_time:.4f}ms")
        print(f"Time to call {'AIO' if producer_type == 'ASYNC' else ''}Producer.poll(): {avg_poll_time:.4f}ms")
        print(f"Time to call {'AIO' if producer_type == 'ASYNC' else ''}Producer.flush(): {flush_time_ms:.4f}ms")
        print(f"Total produce() calls: {len(produce_times)}")
        print(f"Total poll() calls: {len(poll_times)}")
        print(separator)

    def build_serializers(self, serialization_type):
        """Build and return (key_serializer, value_serializer) for the given serialization_type.
        Returns (None, None) if serialization_type is not provided.
        This method should be implemented by concrete strategy classes.
        """
        raise NotImplementedError("Subclasses must implement build_serializers")


class SyncProducerStrategy(ProducerStrategy):
    def build_serializers(self, serialization_type):
        """Build synchronous serializers for Schema Registry"""
        if not serialization_type:
            return None, None

        schema_registry_url = os.getenv(
            'SCHEMA_REGISTRY_URL',
            getattr(self, 'schema_registry_url', 'http://localhost:8081')
        )
        client_config = {
            'url': schema_registry_url,
        }
        sr_client = SchemaRegistryClient(client_config)
        key_serializer = StringSerializer('utf8')

        if serialization_type == 'avro':
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
            json_schema = {
                "type": "object",
                "title": "TestMessage",  # Required for Schema Registry
                "properties": {
                    "test_string": {"type": "string"},
                    "test_bool": {"type": "boolean"},
                    "test_bytes": {"type": "string"},  # JSON represents bytes as string
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
                "required": [
                    "test_string", "test_bool", "test_bytes", "test_double", "test_float",
                    "test_fixed32", "test_fixed64", "test_int32", "test_int64",
                    "test_sfixed32", "test_sfixed64", "test_sint32", "test_sint64",
                    "test_uint32", "test_uint64"
                ]
            }
            value_serializer = JSONSerializer(json.dumps(json_schema), sr_client)
        elif serialization_type == 'protobuf':
            value_serializer = ProtobufSerializer(PublicTestProto_pb2.TestMessage, sr_client)
        else:
            raise ValueError(f"Unsupported serialization_type: {serialization_type}")

        return key_serializer, value_serializer

    def create_producer(self, config_overrides=None):
        config = self._get_base_config()

        # Apply any test-specific overrides
        if config_overrides:
            config.update(config_overrides)

        producer = Producer(config)

        # Log the configuration for validation
        self._log_configuration(config, "SYNC")

        return producer

    def create_transactional_producer(self):
        overrides = {
            'transactional.id': f'sync-tx-producer-{uuid.uuid4()}',
            'acks': 'all',
            'enable.idempotence': True
        }
        return self.create_producer(config_overrides=overrides)

    def produce_messages(self, topic_name, test_duration, start_time, message_formatter,
                         delivered_container, failed_container=None, serialization_type=None,
                         use_transaction=False):
        config_overrides = getattr(self, 'config_overrides', None)
        if use_transaction:
            producer = self.create_transactional_producer()
            producer.init_transactions()
        else:
            producer = self.create_producer(config_overrides)

        # Temporary metrics for timing sections
        messages_sent = 0
        send_times = {}  # Track send times for latency calculation
        produce_times = []
        poll_times = []
        flush_time = 0

        # Initialize serializers via helper (or None if not requested)
        key_serializer, value_serializer = self.build_serializers(serialization_type)

        def delivery_callback(err, msg):
            if err:
                if failed_container is not None:
                    failed_container.append(err)
                if self.metrics:
                    self.metrics.record_failed(
                        topic=msg.topic() if msg else topic_name,
                        partition=msg.partition() if msg else 0
                    )
            else:
                delivered_container.append(msg)
                if self.metrics:
                    # Calculate latency if we have send time
                    msg_key = msg.key().decode('utf-8', errors='replace') if msg.key() else 'unknown'
                    latency_ms = 0.0
                    if msg_key in send_times:
                        latency_ms = (time.time() - send_times[msg_key]) * 1000
                        del send_times[msg_key]

                    self.metrics.record_delivered(latency_ms, topic=msg.topic(), partition=msg.partition())

        while time.time() - start_time < test_duration:
            message_value, message_key = message_formatter(messages_sent)
            try:
                # Begin transaction if using transactions
                if use_transaction:
                    producer.begin_transaction()

                if serialization_type:
                    # Serialize key and value using Schema Registry serializers
                    serialized_key = key_serializer(message_key)
                    serialized_value = value_serializer(
                        message_value, SerializationContext(topic_name, MessageField.VALUE)
                    )
                    message_size = len(serialized_key) + len(serialized_value)
                    if self.metrics:
                        send_times[message_key] = time.time()
                        self.metrics.record_sent(message_size, topic=topic_name, partition=0)
                    produce_start = time.time()
                    producer.produce(
                        topic=topic_name,
                        value=serialized_value,
                        key=serialized_key,
                        on_delivery=delivery_callback
                    )
                else:
                    # Track send time for latency calculation (plain string messages)
                    if self.metrics:
                        send_times[message_key] = time.time()
                        message_size = len(message_value.encode('utf-8')) + len(message_key.encode('utf-8'))
                        self.metrics.record_sent(message_size, topic=topic_name, partition=0)
                    # Produce message
                    produce_start = time.time()
                    producer.produce(
                        topic=topic_name,
                        value=message_value,
                        key=message_key,
                        on_delivery=delivery_callback
                    )
                # Commit transaction if using transactions
                if use_transaction:
                    producer.commit_transaction()

                produce_times.append(time.time() - produce_start)
                messages_sent += 1

                # Use configured polling interval (default to 50 if not set)
                poll_interval = getattr(self, 'poll_interval', 50)

                if messages_sent % poll_interval == 0:
                    poll_start = time.time()
                    producer.poll(0)
                    poll_times.append(time.time() - poll_start)
                    if self.metrics:
                        self.metrics.record_poll()

            except Exception as e:
                if failed_container is not None:
                    failed_container.append(e)
                if self.metrics:
                    self.metrics.record_failed(topic=topic_name, partition=0)
                self.logger.error(f"Failed to produce message {messages_sent}: {e}")

        # Flush producer
        flush_start = time.time()
        producer.flush(timeout=30)
        flush_time = time.time() - flush_start

        # Print timing metrics
        self._print_timing_metrics("SYNC", produce_times, poll_times, flush_time)

        return messages_sent

    def get_final_metrics(self):
        """Return final metrics summary for the sync producer"""
        if self.metrics:
            return self.metrics
        return None


class AsyncProducerStrategy(ProducerStrategy):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._producer_instance = None

    async def build_serializers(self, serialization_type):
        """Build asynchronous serializers for Schema Registry"""
        if not serialization_type:
            return None, None

        schema_registry_url = os.getenv(
            'SCHEMA_REGISTRY_URL',
            getattr(self, 'schema_registry_url', 'http://localhost:8081')
        )
        client_config = {
            'url': schema_registry_url,
        }
        sr_client = AsyncSchemaRegistryClient(client_config)
        key_serializer = StringSerializer('utf8')

        if serialization_type == 'avro':
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
            value_serializer = await AsyncAvroSerializer(
                schema_registry_client=sr_client,
                schema_str=json.dumps(avro_schema)
            )
        elif serialization_type == 'json':
            json_schema = {
                "type": "object",
                "title": "TestMessage",  # Required for Schema Registry
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
                "required": [
                    "test_string", "test_bool", "test_bytes", "test_double", "test_float",
                    "test_fixed32", "test_fixed64", "test_int32", "test_int64",
                    "test_sfixed32", "test_sfixed64", "test_sint32", "test_sint64",
                    "test_uint32", "test_uint64"
                ]
            }
            value_serializer = await AsyncJSONSerializer(json.dumps(json_schema), sr_client)
        elif serialization_type == 'protobuf':
            value_serializer = await AsyncProtobufSerializer(PublicTestProto_pb2.TestMessage, sr_client)
        else:
            raise ValueError(f"Unsupported serialization_type: {serialization_type}")

        return key_serializer, value_serializer

    def create_producer(self, config_overrides=None):
        from confluent_kafka.aio import AIOProducer
        # Enable logging for AIOProducer
        import logging
        logging.basicConfig(level=logging.INFO)

        config = self._get_base_config()

        # Apply any test-specific overrides
        if config_overrides:
            config.update(config_overrides)

        # Get producer configuration from strategy attributes
        max_workers = getattr(self, 'max_workers', 4)
        batch_size = getattr(self, 'batch_size', 1000)  # Optimal batch size for low latency

        # Use updated defaults with configurable parameters
        self._producer_instance = AIOProducer(config, max_workers=max_workers, batch_size=batch_size)

        # Log the configuration for validation
        extra_params = {'max_workers': max_workers, 'batch_size': batch_size}
        self._log_configuration(config, "ASYNC", extra_params)

        return self._producer_instance

    def create_transactional_producer(self):
        overrides = {
            'transactional.id': f'async-tx-producer-{uuid.uuid4()}',
            'acks': 'all',
            'enable.idempotence': True
        }
        return self.create_producer(config_overrides=overrides)

    def produce_messages(self, topic_name, test_duration, start_time, message_formatter,
                         delivered_container, failed_container=None, serialization_type=None,
                         use_transaction=False):

        async def async_produce():
            config_overrides = getattr(self, 'config_overrides', None)

            if use_transaction:
                producer = self.create_transactional_producer()
                await producer.init_transactions()
            else:
                producer = self.create_producer(config_overrides)

            # Temporary metrics for timing sections
            messages_sent = 0
            produce_times = []
            poll_times = []  # Async producer doesn't use polling, but needed for metrics
            flush_time = 0
            pending_futures = []
            send_times = {}

            # Get serializers if using Schema Registry
            if serialization_type:
                key_serializer, value_serializer = await self.build_serializers(serialization_type)

            # Pre-create shared metrics callback to avoid closure creation overhead
            if self.metrics:
                def shared_metrics_callback(err, msg):
                    if not err:
                        # Calculate latency if we have send time
                        msg_key = msg.key().decode('utf-8', errors='replace') if msg.key() else 'unknown'
                        latency_ms = 0.0
                        if msg_key in send_times:
                            latency_ms = (time.time() - send_times[msg_key]) * 1000
                            del send_times[msg_key]
                        self.metrics.record_delivered(latency_ms, topic=msg.topic(), partition=msg.partition())
                    if not err:  # Also append to delivered_messages for assertion
                        delivered_container.append(msg)

            while time.time() - start_time < test_duration:
                message_value, message_key = message_formatter(messages_sent)
                try:
                    # Begin transaction if using transactions
                    if use_transaction:
                        await producer.begin_transaction()

                    # Handle serialization if using Schema Registry
                    if serialization_type:
                        serialized_key = key_serializer(message_key)
                        # Async serializers return coroutines, so we need to await them
                        serialized_value = await value_serializer(
                            message_value, SerializationContext(topic_name, MessageField.VALUE)
                        )
                        message_size = len(serialized_key) + len(serialized_value)
                        produce_value = serialized_value
                        produce_key = serialized_key
                    else:
                        # Plain string encoding for non-SR
                        message_size = len(message_value.encode('utf-8')) + len(message_key.encode('utf-8'))
                        produce_value = message_value
                        produce_key = message_key

                    # Record sent message for metrics and track send time
                    if self.metrics:
                        self.metrics.record_sent(message_size, topic=topic_name, partition=0)
                        send_times[message_key] = time.time()  # Track send time for latency

                    # Produce message
                    produce_start = time.time()
                    delivery_future = await producer.produce(
                        topic=topic_name,
                        value=produce_value,
                        key=produce_key,
                        on_delivery=shared_metrics_callback
                    )

                    # Commit transaction if using transactions
                    if use_transaction:
                        await producer.commit_transaction()

                    produce_times.append(time.time() - produce_start)
                    pending_futures.append((delivery_future, message_key))  # Store delivery future
                    messages_sent += 1

                except Exception as e:
                    if failed_container is not None:
                        failed_container.append(e)
                    if self.metrics:
                        self.metrics.record_failed(topic=topic_name, partition=0)
                    self.logger.error(f"Failed to produce message {messages_sent}: {e}")

            # Flush producer
            flush_start = time.time()
            await producer.flush(timeout=30)
            flush_time = time.time() - flush_start

            # Wait for all pending futures to complete (for delivery confirmation only)
            for delivery_future, message_key in pending_futures:
                try:
                    msg = await delivery_future
                    delivered_container.append(msg)

                    # Record delivery metrics (replaces the old callback approach)
                    if self.metrics:
                        # Calculate latency if we have send time
                        latency_ms = 0.0
                        if message_key in send_times:
                            latency_ms = (time.time() - send_times[message_key]) * 1000
                            del send_times[message_key]

                        self.metrics.record_delivered(latency_ms, topic=msg.topic(), partition=msg.partition())

                except Exception as e:
                    if failed_container is not None:
                        failed_container.append(e)
                    if self.metrics:
                        self.metrics.record_failed(topic=topic_name, partition=0)
                    self.logger.error(f"Failed to deliver message with key {message_key}: {e}")

            # Print timing metrics
            self._print_timing_metrics("ASYNC", produce_times, poll_times, flush_time)

            # Close producer to ensure clean shutdown
            await producer.close()

            return messages_sent

        loop = asyncio.get_event_loop()
        return loop.run_until_complete(async_produce())

    def get_final_metrics(self):
        """Return final metrics summary for the async producer"""
        if self.metrics:
            return self.metrics
        return None
