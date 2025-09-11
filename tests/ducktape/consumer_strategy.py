"""
Consumer strategies for testing sync and async Kafka consumers.

This module contains strategy classes that encapsulate the different consumer
implementations (sync vs async) with consistent interfaces for testing.
"""
import time
import asyncio
from confluent_kafka import Consumer


class ConsumerStrategy:
    """Base class for consumer strategies"""
    def __init__(self, bootstrap_servers, group_id, logger):
        self.bootstrap_servers = bootstrap_servers
        self.group_id = group_id
        self.logger = logger
        self.metrics = None

    def create_consumer(self):
        raise NotImplementedError()

    def consume_messages(self, topic_name, test_duration, start_time, consumed_container, timeout=1.0):
        raise NotImplementedError()

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

    def consume_messages(self, topic_name, test_duration, start_time, consumed_container, timeout=1.0):
        consumer = self.create_consumer()
        messages_consumed = 0
        consume_times = []  # Track consume batch latencies
        batch_size = 100  # Process up to 100 messages per consume() call

        try:
            consumer.subscribe([topic_name])

            while time.time() - start_time < test_duration:
                consume_start = time.time()
                messages = consumer.consume(num_messages=batch_size, timeout=timeout)
                consume_end = time.time()

                consume_latency_ms = (consume_end - consume_start) * 1000
                consume_times.append(consume_latency_ms)

                if self.metrics:
                    self.metrics.record_poll_attempt(consume_latency_ms)

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

                    # Successfully consumed a message
                    consumed_container.append(msg)
                    messages_consumed += 1
                    batch_consumed += 1

                    if self.metrics:
                        message_size = len(msg.value()) + (len(msg.key()) if msg.key() else 0)
                        self.metrics.record_consumed(
                            message_size=message_size,
                            topic=msg.topic(),
                            partition=msg.partition(),
                            offset=msg.offset(),
                            poll_latency_ms=consume_latency_ms / max(len(messages), 1)  # Amortize latency across batch
                        )

                # Log progress occasionally
                if messages_consumed % 100 == 0:
                    elapsed = time.time() - start_time
                    rate = messages_consumed / elapsed if elapsed > 0 else 0
                    self.logger.debug(f"Sync: Consumed {messages_consumed} messages at {rate:.1f} msg/s")

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
                    self.metrics.record_poll_attempt(poll_latency_ms)

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
                    self.metrics.record_consumed(
                        message_size=len(msg.value()) if msg.value() else 0,
                        topic=msg.topic(),
                        partition=msg.partition(),
                        offset=msg.offset(),
                        poll_latency_ms=poll_latency_ms
                    )

                if messages_consumed % 1000 == 0:
                    elapsed = time.time() - start_time
                    rate = messages_consumed / elapsed if elapsed > 0 else 0
                    self.logger.info(f"Polled {messages_consumed} messages at {rate:.1f} msg/s")

        finally:
            consumer.close()

        return messages_consumed


class AsyncConsumerStrategy(ConsumerStrategy):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._consumer_instance = None

    def create_consumer(self):
        from confluent_kafka.aio import AIOConsumer
        # Enable logging for AIOConsumer
        import logging
        logging.basicConfig(level=logging.INFO)

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

    def consume_messages(self, topic_name, test_duration, start_time, consumed_container, timeout=1.0):

        async def async_consume():
            consumer = self.create_consumer()
            messages_consumed = 0
            consume_times = []  # Track consume batch latencies
            batch_size = 100  # Process up to 100 messages per consume() call

            try:
                await consumer.subscribe([topic_name])

                while time.time() - start_time < test_duration:
                    consume_start = time.time()
                    messages = await consumer.consume(num_messages=batch_size, timeout=timeout)
                    consume_end = time.time()

                    consume_latency_ms = (consume_end - consume_start) * 1000
                    consume_times.append(consume_latency_ms)

                    if self.metrics:
                        self.metrics.record_poll_attempt(consume_latency_ms)

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

                        # Successfully consumed a message
                        consumed_container.append(msg)
                        messages_consumed += 1
                        batch_consumed += 1

                        if self.metrics:
                            message_size = len(msg.value()) + (len(msg.key()) if msg.key() else 0)
                            self.metrics.record_consumed(
                                message_size=message_size,
                                topic=msg.topic(),
                                partition=msg.partition(),
                                offset=msg.offset(),
                                poll_latency_ms=consume_latency_ms / max(len(messages), 1)  # Amortize latency across batch
                            )

                    # Log progress occasionally (per batch instead of per message)
                    if messages_consumed % 1000 == 0:
                        elapsed = time.time() - start_time
                        rate = messages_consumed / elapsed if elapsed > 0 else 0
                        self.logger.debug(f"Async: Consumed {messages_consumed} messages at {rate:.1f} msg/s")

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
                        self.metrics.record_poll_attempt(poll_latency_ms)

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
                        self.metrics.record_consumed(
                            message_size=len(msg.value()) if msg.value() else 0,
                            topic=msg.topic(),
                            partition=msg.partition(),
                            offset=msg.offset(),
                            poll_latency_ms=poll_latency_ms
                        )

                    if messages_consumed % 1000 == 0:
                        elapsed = time.time() - start_time
                        rate = messages_consumed / elapsed if elapsed > 0 else 0
                        self.logger.info(f"Async: Polled {messages_consumed} messages at {rate:.1f} msg/s")

            finally:
                await consumer.close()

            return messages_consumed

        loop = asyncio.get_event_loop()
        return loop.run_until_complete(async_poll())
