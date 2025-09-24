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


class DeserializedMessage:
    """Wrapper for messages with deserialized key/value"""
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
                         timeout=1.0, serialization_type=None):
        raise NotImplementedError()

    def _get_schema_registry_client(self, is_async=False):
        """Get Schema Registry client with proper configuration"""
        schema_registry_url = os.getenv(
            'SCHEMA_REGISTRY_URL',
            getattr(self, 'schema_registry_url', 'http://localhost:8081')
        )
        client_config = {
            'url': schema_registry_url,
        }

        if is_async:
            return AsyncSchemaRegistryClient(client_config)
        else:
            return SchemaRegistryClient(client_config)

    def _get_schemas(self, deserialization_type):
        """Get schema definitions for the given deserialization type, for testing purposes"""
        if deserialization_type == 'avro':
            # Match the Protobuf TestMessage structure
            return {
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
        elif deserialization_type == 'json':
            # Match the Protobuf TestMessage structure
            return {
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


    def _record_message_metrics(self, msg, latency_ms):
        """Shared metrics recording logic"""
        if self.metrics:
            # For deserialized messages, use original raw bytes for size calculation
            if hasattr(msg, '_original_msg'):
                # DeserializedMessage - use original raw bytes
                original_msg = msg._original_msg
                value_size = len(original_msg.value()) if original_msg.value() else 0
                key_size = len(original_msg.key()) if original_msg.key() else 0
            else:
                # Raw message - use directly
                value_size = len(msg.value()) if msg.value() else 0
                key_size = len(msg.key()) if msg.key() else 0

            message_size = value_size + key_size
            self.metrics.record_processed_message(
                message_size=message_size,
                topic=msg.topic(),
                partition=msg.partition(),
                offset=msg.offset(),
                operation_latency_ms=latency_ms
            )


class SyncConsumerStrategy(ConsumerStrategy):
    def _deserialize_message(self, msg, key_deserializer, value_deserializer, topic_name, message_count):
        """Sync deserialization logic"""
        if not (key_deserializer or value_deserializer):
            return msg

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

            # Log successful deserialization for first few messages
            if message_count < 5:
                self.logger.debug(f"Deserialized message {message_count}: "
                                f"key={deserialized_key}, value={deserialized_value}")

            return DeserializedMessage(msg, deserialized_key, deserialized_value)

        except Exception as e:
            self.logger.error(f"Deserialization error: {e}")
            return msg  # Return original message if deserialization fails


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

    def consume_messages(self, topic_name, test_duration, start_time, consumed_container,
                         timeout=1.0, serialization_type=None):
        # Initialize deserializers if using Schema Registry
        key_deserializer, value_deserializer = self.build_deserializers(serialization_type, is_async=False)

        consumer = self.create_consumer()

        try:
            # Sync consume implementation
            messages_consumed = 0

            consumer.subscribe([topic_name])

            while time.time() - start_time < test_duration:
                consume_start = time.time()
                messages = consumer.consume(num_messages=self.batch_size, timeout=timeout)
                consume_end = time.time()

                consume_latency_ms = (consume_end - consume_start) * 1000

                if self.metrics:
                    self.metrics.record_api_call(consume_latency_ms)
                    self.metrics.record_batch_operation(len(messages) if messages else 0)

                if not messages:
                    if self.metrics:
                        self.metrics.record_timeout()
                    continue

                # Process all messages in the batch
                batch_consumed = 0
                for msg in messages:
                    if msg.error():
                        if self.metrics:
                            self.metrics.record_error(str(msg.error()))
                        self.logger.error(f"Consumer error: {msg.error()}")
                        continue

                    # Deserialize message
                    msg = self._deserialize_message(msg, key_deserializer, value_deserializer, topic_name, messages_consumed + batch_consumed)
                    consumed_container.append(msg)
                    messages_consumed += 1
                    batch_consumed += 1
                    self._record_message_metrics(msg,
                                               consume_latency_ms / max(len(messages), 1))

            consumer.close()
            return messages_consumed
        except Exception:
            consumer.close()
            raise

    def poll_messages(self, topic_name, test_duration, start_time, consumed_container,
                      timeout=1.0, serialization_type=None):
        # Initialize deserializers if using Schema Registry
        key_deserializer, value_deserializer = (
            self.build_deserializers(serialization_type) if serialization_type
            else (None, None)
        )

        consumer = self.create_consumer()

        try:
            # Sync poll implementation
            messages_consumed = 0
            consumer.subscribe([topic_name])

            while time.time() - start_time < test_duration:
                poll_start = time.time()
                msg = consumer.poll(timeout=timeout)
                poll_end = time.time()

                poll_latency_ms = (poll_end - poll_start) * 1000

                if self.metrics:
                    self.metrics.record_api_call(poll_latency_ms)

                if msg is None:
                    if self.metrics:
                        self.metrics.record_timeout()
                    continue

                if msg.error():
                    if self.metrics:
                        self.metrics.record_error(str(msg.error()))
                    self.logger.error(f"Consumer error: {msg.error()}")
                    continue

                # Deserialize message
                msg = self._deserialize_message(msg, key_deserializer, value_deserializer, topic_name, messages_consumed)
                consumed_container.append(msg)
                messages_consumed += 1
                self._record_message_metrics(msg, poll_latency_ms)

            consumer.close()
            return messages_consumed
        except Exception:
            consumer.close()
            raise


class AsyncConsumerStrategy(ConsumerStrategy):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._consumer_instance = None

    async def _deserialize_message(self, msg, key_deserializer, value_deserializer, topic_name, message_count):
        """Async deserialization logic"""
        if not (key_deserializer or value_deserializer):
            return msg

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

            # Log successful deserialization for first few messages
            if message_count < 5:
                self.logger.debug(f"Async deserialized message {message_count}: "
                                f"key={deserialized_key}, value={deserialized_value}")

            return DeserializedMessage(msg, deserialized_key, deserialized_value)

        except Exception as e:
            self.logger.error(f"Async deserialization error: {e}")
            return msg  # Return original message if deserialization fails


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

    def consume_messages(self, topic_name, test_duration, start_time, consumed_container,
                         timeout=1.0, serialization_type=None):
        async def async_consume():
            # Initialize deserializers if using Schema Registry
            key_deserializer, value_deserializer = (
                await self.build_async_deserializers(serialization_type) if serialization_type
                else (None, None)
            )

            consumer = self.create_consumer()

            try:
                # Async consume implementation
                messages_consumed = 0

                await consumer.subscribe([topic_name])

                while time.time() - start_time < test_duration:
                    consume_start = time.time()
                    messages = await consumer.consume(num_messages=self.batch_size, timeout=timeout)
                    consume_end = time.time()

                    consume_latency_ms = (consume_end - consume_start) * 1000

                    if self.metrics:
                        self.metrics.record_api_call(consume_latency_ms)
                        self.metrics.record_batch_operation(len(messages) if messages else 0)

                    if not messages:
                        if self.metrics:
                            self.metrics.record_timeout()
                        continue

                    # Process all messages in the batch
                    batch_consumed = 0
                    for msg in messages:
                        if msg.error():
                            if self.metrics:
                                self.metrics.record_error(str(msg.error()))
                            self.logger.error(f"Consumer error: {msg.error()}")
                            continue

                        # Deserialize message
                        msg = await self._deserialize_message(msg, key_deserializer, value_deserializer, topic_name, messages_consumed + batch_consumed)
                        consumed_container.append(msg)
                        messages_consumed += 1
                        batch_consumed += 1
                        self._record_message_metrics(msg,
                                                   consume_latency_ms / max(len(messages), 1))

                await consumer.close()
                return messages_consumed
            except Exception:
                await consumer.close()
                raise

        loop = asyncio.get_event_loop()
        return loop.run_until_complete(async_consume())

    def poll_messages(self, topic_name, test_duration, start_time, consumed_container,
                      timeout=1.0, serialization_type=None):
        async def async_poll():
            # Initialize deserializers if using Schema Registry
            key_deserializer, value_deserializer = (
                await self.build_async_deserializers(serialization_type) if serialization_type
                else (None, None)
            )

            consumer = self.create_consumer()

            try:
                # Async poll implementation
                messages_consumed = 0
                await consumer.subscribe([topic_name])

                while time.time() - start_time < test_duration:
                    poll_start = time.time()
                    msg = await consumer.poll(timeout=timeout)
                    poll_end = time.time()

                    poll_latency_ms = (poll_end - poll_start) * 1000

                    if self.metrics:
                        self.metrics.record_api_call(poll_latency_ms)

                    if msg is None:
                        if self.metrics:
                            self.metrics.record_timeout()
                        continue

                    if msg.error():
                        if self.metrics:
                            self.metrics.record_error(str(msg.error()))
                        self.logger.error(f"Consumer error: {msg.error()}")
                        continue

                    # Deserialize message
                    msg = await self._deserialize_message(msg, key_deserializer, value_deserializer, topic_name, messages_consumed)
                    consumed_container.append(msg)
                    messages_consumed += 1
                    self._record_message_metrics(msg, poll_latency_ms)

                await consumer.close()
                return messages_consumed
            except Exception:
                await consumer.close()
                raise

        loop = asyncio.get_event_loop()
        return loop.run_until_complete(async_poll())
