"""
Consumer strategies for testing sync and async Kafka consumers.

This module contains strategy classes that encapsulate the different consumer
implementations (sync vs async) with consistent interfaces for testing.
"""
import time
import asyncio
import json
import os
from confluent_kafka import Consumer
from confluent_kafka.aio import AIOConsumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry._sync.json_schema import JSONDeserializer
from confluent_kafka.schema_registry._sync.protobuf import ProtobufDeserializer
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.schema_registry._async.json_schema import AsyncJSONDeserializer
from confluent_kafka.schema_registry._async.protobuf import AsyncProtobufDeserializer
from confluent_kafka.schema_registry._async.avro import AsyncAvroDeserializer
from confluent_kafka.schema_registry import AsyncSchemaRegistryClient
from confluent_kafka.serialization import StringDeserializer, SerializationContext, MessageField
from tests.integration.schema_registry.data.proto import PublicTestProto_pb2


class ConsumerStrategy:
    """Base class for consumer strategies"""
    def __init__(self, bootstrap_servers, group_id, logger, batch_size=10):
        self.bootstrap_servers = bootstrap_servers
        self.group_id = group_id
        self.logger = logger
        self.batch_size = batch_size
        self.metrics = None

    def create_consumer(self):
        raise NotImplementedError()

    def consume_messages(self, topic_name, test_duration, start_time, consumed_container,
                         timeout=1.0, deserialization_type=None):
        raise NotImplementedError()

    def _get_schema_registry_client(self, is_async=False):
        """Get Schema Registry client with proper configuration"""
        schema_registry_url = os.getenv(
            'SCHEMA_REGISTRY_URL',
            getattr(self, 'schema_registry_url', 'http://localhost:8081')
        )
        client_config = {
            'url': schema_registry_url,
            'basic.auth.user.info': 'ASUHV2PEDSTIW3LF:cfltSQ9mRLOItofBcTEzk6Ml/86VAqb9gjy2YYoeRDZZgML/LZ/ift9QBOyuyAyw'
        }

        if is_async:
            return AsyncSchemaRegistryClient(client_config)
        else:
            return SchemaRegistryClient(client_config)

    def _get_schemas(self, deserialization_type):
        """Get schema definitions for the given deserialization type, for testing purposes"""
        if deserialization_type == 'avro':
            return {
                "type": "record",
                "name": "User",
                "fields": [
                    {"name": "name", "type": "string"},
                    {"name": "age", "type": "int"}
                ]
            }
        elif deserialization_type == 'json':
            return {
                "type": "object",
                "properties": {
                    "name": {"type": "string"},
                    "age": {"type": "integer"}
                },
                "required": ["name", "age"]
            }
        elif deserialization_type == 'protobuf':
            return PublicTestProto_pb2.TestMessage
        else:
            raise ValueError(f"Unsupported deserialization type: {deserialization_type}")

    def build_deserializers(self, deserialization_type, is_async=False):
        """
        Build and return (key_deserializer, value_deserializer) for the given deserialization_type.
        Returns (None, None) if deserialization_type is not provided.
        """
        if not deserialization_type:
            return None, None

        key_deserializer = StringDeserializer('utf8')

        # For async, return None as value_deserializer (will be built in async context)
        if is_async:
            return key_deserializer, None

        # Build sync deserializers
        sr_client = self._get_schema_registry_client(is_async=False)
        schema = self._get_schemas(deserialization_type)

        if deserialization_type == 'avro':
            value_deserializer = AvroDeserializer(
                schema_registry_client=sr_client,
                schema_str=json.dumps(schema)
            )
        elif deserialization_type == 'json':
            value_deserializer = JSONDeserializer(json.dumps(schema))
        elif deserialization_type == 'protobuf':
            value_deserializer = ProtobufDeserializer(schema)

        return key_deserializer, value_deserializer

    async def build_async_deserializers(self, deserialization_type):
        """Build async deserializers - must be called from async context"""
        if not deserialization_type:
            return None, None

        key_deserializer = StringDeserializer('utf8')
        sr_client = self._get_schema_registry_client(is_async=True)
        schema = self._get_schemas(deserialization_type)

        if deserialization_type == 'avro':
            value_deserializer = await AsyncAvroDeserializer(
                schema_registry_client=sr_client,
                schema_str=json.dumps(schema)
            )
        elif deserialization_type == 'json':
            value_deserializer = await AsyncJSONDeserializer(json.dumps(schema))
        elif deserialization_type == 'protobuf':
            value_deserializer = await AsyncProtobufDeserializer(schema)

        return key_deserializer, value_deserializer

    def get_final_metrics(self):
        return None


class SyncConsumerStrategy(ConsumerStrategy):
    def create_consumer(self):
        config = {
            'bootstrap.servers': self.bootstrap_servers,
            'group.id': self.group_id,
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': 'true',
            'auto.commit.interval.ms': '5000'
        }

        consumer = Consumer(config)
        return consumer

    def get_final_metrics(self):
        """Sync consumer has no built-in metrics like AIOConsumer"""
        return None

    def consume_messages(self, topic_name, test_duration, start_time, consumed_container,
                         timeout=1.0, deserialization_type=None):
        # Initialize deserializers if using Schema Registry
        key_deserializer, value_deserializer = self.build_deserializers(deserialization_type, is_async=False)

        consumer = self.create_consumer()
        messages_consumed = 0
        consume_times = []  # Track consume batch latencies

        try:
            consumer.subscribe([topic_name])

            while time.time() - start_time < test_duration:
                consume_start = time.time()
                messages = consumer.consume(num_messages=self.batch_size, timeout=timeout)
                consume_end = time.time()

                consume_latency_ms = (consume_end - consume_start) * 1000
                consume_times.append(consume_latency_ms)

                if self.metrics:
                    self.metrics.record_api_call(consume_latency_ms)
                    self.metrics.record_batch_operation(len(messages) if messages else 0)

                if not messages:
                    # Timeout or no messages available
                    if self.metrics:
                        self.metrics.record_timeout()
                    continue

                # Process all messages in the batch
                batch_consumed = 0
                for msg in messages:
                    if msg.error():
                        # Error occurred
                        if self.metrics:
                            self.metrics.record_error(str(msg.error()))
                        self.logger.error(f"Consumer error: {msg.error()}")
                        continue

                    # Deserialize message if deserializers are provided
                    if key_deserializer or value_deserializer:
                        try:
                            # Deserialize key and value
                            deserialized_key = msg.key()
                            if key_deserializer and msg.key() is not None:
                                deserialized_key = key_deserializer(msg.key())

                            deserialized_value = msg.value()
                            if value_deserializer and msg.value() is not None:
                                deserialized_value = value_deserializer(
                                    msg.value(),
                                    SerializationContext(topic_name, MessageField.VALUE)
                                )

                            # Create minimal wrapper - only override key() and value() methods

                            class DeserializedMessage:
                                def __init__(self, original_msg, key, value):
                                    self._original_msg = original_msg
                                    self._key = key
                                    self._value = value

                                def key(self): return self._key
                                def value(self): return self._value
                                def error(self): return self._original_msg.error()
                                def topic(self): return self._original_msg.topic()
                                def partition(self): return self._original_msg.partition()
                                def offset(self): return self._original_msg.offset()

                            msg = DeserializedMessage(msg, deserialized_key, deserialized_value)

                        except Exception as e:
                            self.logger.error(f"Deserialization error: {e}")
                            if self.metrics:
                                self.metrics.record_error(f"Deserialization error: {e}")
                            continue

                    # Successfully consumed a message
                    consumed_container.append(msg)
                    messages_consumed += 1
                    batch_consumed += 1

                    if self.metrics:
                        # Calculate message size - handle both raw bytes and deserialized objects
                        if key_deserializer or value_deserializer:
                            # We have deserialized objects, need special handling
                            try:
                                if hasattr(msg.value(), '__len__'):
                                    value_size = len(msg.value())
                                elif hasattr(msg.value(), 'ByteSize'):
                                    # Protobuf message
                                    value_size = msg.value().ByteSize()
                                elif hasattr(msg.value(), 'SerializeToString'):
                                    # Protobuf message fallback
                                    value_size = len(msg.value().SerializeToString())
                                else:
                                    value_size = 0
                            except (AttributeError, TypeError):
                                value_size = 0
                        else:
                            # Raw bytes, use simple len() - fast path for base case
                            value_size = len(msg.value()) if msg.value() else 0

                        message_size = value_size + (len(msg.key()) if msg.key() else 0)
                        self.metrics.record_processed_message(
                            message_size=message_size,
                            topic=msg.topic(),
                            partition=msg.partition(),
                            offset=msg.offset(),
                            # Amortize latency across batch
                            operation_latency_ms=consume_latency_ms / max(len(messages), 1)
                        )

        finally:
            consumer.close()

        return messages_consumed

    def poll_messages(self, topic_name, test_duration, start_time, consumed_container, timeout=1.0):
        """Poll messages one by one using consumer.poll() instead of batch consume()"""
        consumer = self.create_consumer()
        messages_consumed = 0
        poll_times = []  # Track individual poll latencies

        try:
            consumer.subscribe([topic_name])

            while time.time() - start_time < test_duration:
                poll_start = time.time()
                msg = consumer.poll(timeout=timeout)
                poll_end = time.time()

                poll_latency_ms = (poll_end - poll_start) * 1000
                poll_times.append(poll_latency_ms)

                if self.metrics:
                    self.metrics.record_api_call(poll_latency_ms)

                if msg is None:
                    # Timeout - no message received
                    if self.metrics:
                        self.metrics.record_timeout()
                    continue

                if msg.error():
                    # Error occurred
                    if self.metrics:
                        self.metrics.record_error(str(msg.error()))
                    self.logger.error(f"Consumer error: {msg.error()}")
                    continue

                # Process the single message
                consumed_container.append(msg)
                messages_consumed += 1

                if self.metrics:
                    self.metrics.record_processed_message(
                        message_size=len(msg.value()) if msg.value() else 0,
                        topic=msg.topic(),
                        partition=msg.partition(),
                        offset=msg.offset(),
                        operation_latency_ms=poll_latency_ms
                    )

                # Progress tracking (removed verbose logging)

        finally:
            consumer.close()

        return messages_consumed


class AsyncConsumerStrategy(ConsumerStrategy):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._consumer_instance = None

    def create_consumer(self):
        config = {
            'bootstrap.servers': self.bootstrap_servers,
            'group.id': self.group_id,
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': 'true',
            'auto.commit.interval.ms': '5000'
        }

        self._consumer_instance = AIOConsumer(config, max_workers=20)
        return self._consumer_instance

    def get_final_metrics(self):
        """Get metrics from the AIOConsumer instance (if available)"""
        if self._consumer_instance and hasattr(self._consumer_instance, 'get_metrics'):
            return self._consumer_instance.get_metrics()
        return None

    def consume_messages(self, topic_name, test_duration, start_time, consumed_container,
                         timeout=1.0, deserialization_type=None):

        async def async_consume():
            # Initialize deserializers if using Schema Registry
            if deserialization_type:
                key_deserializer, value_deserializer = await self.build_async_deserializers(deserialization_type)
            else:
                key_deserializer, value_deserializer = None, None

            consumer = self.create_consumer()
            messages_consumed = 0
            consume_times = []  # Track consume batch latencies

            try:
                await consumer.subscribe([topic_name])

                while time.time() - start_time < test_duration:
                    consume_start = time.time()
                    messages = await consumer.consume(num_messages=self.batch_size, timeout=timeout)
                    consume_end = time.time()

                    consume_latency_ms = (consume_end - consume_start) * 1000
                    consume_times.append(consume_latency_ms)

                    if self.metrics:
                        self.metrics.record_api_call(consume_latency_ms)
                        self.metrics.record_batch_operation(len(messages) if messages else 0)

                    if not messages:
                        # Timeout or no messages available
                        if self.metrics:
                            self.metrics.record_timeout()
                        continue

                    # Process all messages in the batch
                    batch_consumed = 0
                    for msg in messages:
                        if msg.error():
                            # Error occurred
                            if self.metrics:
                                self.metrics.record_error(str(msg.error()))
                            self.logger.error(f"Consumer error: {msg.error()}")
                            continue

                        # Deserialize message if deserializers are provided
                        if key_deserializer or value_deserializer:
                            try:
                                # Deserialize key and value
                                deserialized_key = msg.key()
                                if key_deserializer and msg.key() is not None:
                                    # Note: StringDeserializer is sync, so no await needed
                                    deserialized_key = key_deserializer(msg.key())

                                deserialized_value = msg.value()
                                if value_deserializer and msg.value() is not None:
                                    deserialized_value = await value_deserializer(
                                        msg.value(),
                                        SerializationContext(topic_name, MessageField.VALUE)
                                    )

                                # Create minimal wrapper - only override key() and value() methods

                                class DeserializedMessage:
                                    def __init__(self, original_msg, key, value):
                                        self._original_msg = original_msg
                                        self._key = key
                                        self._value = value

                                    def key(self): return self._key
                                    def value(self): return self._value
                                    def error(self): return self._original_msg.error()
                                    def topic(self): return self._original_msg.topic()
                                    def partition(self): return self._original_msg.partition()
                                    def offset(self): return self._original_msg.offset()

                                msg = DeserializedMessage(msg, deserialized_key, deserialized_value)

                            except Exception as e:
                                self.logger.error(f"Deserialization error: {e}")
                                if self.metrics:
                                    self.metrics.record_error(f"Deserialization error: {e}")
                                continue

                        # Successfully consumed a message
                        consumed_container.append(msg)
                        messages_consumed += 1
                        batch_consumed += 1

                        if self.metrics:
                            # Calculate message size - handle both raw bytes and deserialized objects
                            if key_deserializer or value_deserializer:
                                # We have deserialized objects, need special handling
                                try:
                                    if hasattr(msg.value(), '__len__'):
                                        value_size = len(msg.value())
                                    elif hasattr(msg.value(), 'ByteSize'):
                                        # Protobuf message
                                        value_size = msg.value().ByteSize()
                                    elif hasattr(msg.value(), 'SerializeToString'):
                                        # Protobuf message fallback
                                        value_size = len(msg.value().SerializeToString())
                                    else:
                                        value_size = 0
                                except (AttributeError, TypeError):
                                    value_size = 0
                            else:
                                # Raw bytes, use simple len() - fast path for base case
                                value_size = len(msg.value()) if msg.value() else 0

                            message_size = value_size + (len(msg.key()) if msg.key() else 0)
                            self.metrics.record_processed_message(
                                message_size=message_size,
                                topic=msg.topic(),
                                partition=msg.partition(),
                                offset=msg.offset(),
                                # Amortize latency across batch
                                operation_latency_ms=consume_latency_ms / max(len(messages), 1)
                            )

                    # Progress tracking (removed verbose logging)

            finally:
                await consumer.close()

            return messages_consumed

        loop = asyncio.get_event_loop()
        return loop.run_until_complete(async_consume())

    def poll_messages(self, topic_name, test_duration, start_time, consumed_container, timeout=1.0):
        """Poll messages one by one using consumer.poll() instead of batch consume()"""

        async def async_poll():
            consumer = self.create_consumer()
            messages_consumed = 0
            poll_times = []  # Track individual poll latencies

            try:
                await consumer.subscribe([topic_name])

                while time.time() - start_time < test_duration:
                    poll_start = time.time()
                    msg = await consumer.poll(timeout=timeout)
                    poll_end = time.time()

                    poll_latency_ms = (poll_end - poll_start) * 1000
                    poll_times.append(poll_latency_ms)

                    if self.metrics:
                        self.metrics.record_api_call(poll_latency_ms)

                    if msg is None:
                        # Timeout - no message received
                        if self.metrics:
                            self.metrics.record_timeout()
                        continue

                    if msg.error():
                        # Error occurred
                        if self.metrics:
                            self.metrics.record_error(str(msg.error()))
                        self.logger.error(f"Consumer error: {msg.error()}")
                        continue

                    # Process the single message
                    consumed_container.append(msg)
                    messages_consumed += 1

                    if self.metrics:
                        self.metrics.record_processed_message(
                            message_size=len(msg.value()) if msg.value() else 0,
                            topic=msg.topic(),
                            partition=msg.partition(),
                            offset=msg.offset(),
                            operation_latency_ms=poll_latency_ms
                        )

            finally:
                await consumer.close()

            return messages_consumed

        loop = asyncio.get_event_loop()
        return loop.run_until_complete(async_poll())
