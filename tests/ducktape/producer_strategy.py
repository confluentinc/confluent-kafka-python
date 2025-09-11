"""
Producer strategies for testing sync and async Kafka producers.

This module contains strategy classes that encapsulate the different producer
implementations (sync vs async) with consistent interfaces for testing.
"""
import time
import asyncio
from confluent_kafka import Producer


class ProducerStrategy:
    """Base class for producer strategies"""
    def __init__(self, bootstrap_servers, logger):
        self.bootstrap_servers = bootstrap_servers
        self.logger = logger
        self.metrics = None

    def create_producer(self):
        raise NotImplementedError()

    def produce_messages(self, topic_name, test_duration, start_time, message_formatter, delivered_container, failed_container=None):
        raise NotImplementedError()


class SyncProducerStrategy(ProducerStrategy):
    def create_producer(self, config_overrides=None):
        config = {
            'bootstrap.servers': self.bootstrap_servers
        }
        
        # Apply any test-specific overrides
        if config_overrides:
            config.update(config_overrides)
            
        producer = Producer(config)
        
        # Log the configuration for validation
        self.logger.info("=== SYNC PRODUCER CONFIGURATION ===")
        for key, value in config.items():
            self.logger.info(f"{key}: {value}")
        self.logger.info("=" * 40)
        
        return producer
    
    def produce_messages(self, topic_name, test_duration, start_time, message_formatter, delivered_container, failed_container=None):
        config_overrides = getattr(self, 'config_overrides', None)
        producer = self.create_producer(config_overrides)
        messages_sent = 0
        send_times = {}  # Track send times for latency calculation
        
        # Temporary metrics for timing sections
        produce_times = []
        poll_times = []
        flush_time = 0
        
        def delivery_callback(err, msg):
            if err:
                if failed_container is not None:
                    failed_container.append(err)
                if self.metrics:
                    self.metrics.record_failed(topic=msg.topic() if msg else topic_name,
                                             partition=msg.partition() if msg else 0)
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
                # Track send time for latency calculation
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
                produce_times.append(time.time() - produce_start)
                messages_sent += 1
                
                # Poll every 100 messages to prevent buffer overflow
                if messages_sent % 100 == 0:
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
        avg_produce_time = sum(produce_times) / len(produce_times) * 1000 if produce_times else 0
        avg_poll_time = sum(poll_times) / len(poll_times) * 1000 if poll_times else 0
        flush_time_ms = flush_time * 1000
        
        print(f"\n=== SYNC PRODUCER CODE PATH TIMING ===")
        print(f"Time to call Producer.produce(): {avg_produce_time:.4f}ms")
        print(f"Time to call Producer.poll(): {avg_poll_time:.4f}ms")
        print(f"Time to call Producer.flush(): {flush_time_ms:.4f}ms")
        print(f"Total produce() calls: {len(produce_times)}")
        print(f"Total poll() calls: {len(poll_times)}")
        print("=" * 45)
        
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
    
    def create_producer(self, config_overrides=None):
        from confluent_kafka.aio import AIOProducer
        # Enable logging for AIOProducer
        import logging
        logging.basicConfig(level=logging.INFO)
        
        config = {
            'bootstrap.servers': self.bootstrap_servers
        }
        
        # Apply any test-specific overrides
        if config_overrides:
            config.update(config_overrides)
        
        self._producer_instance = AIOProducer(config, max_workers=20)
        
        # Log the configuration for validation
        self.logger.info("=== ASYNC PRODUCER CONFIGURATION ===")
        for key, value in config.items():
            self.logger.info(f"{key}: {value}")
        self.logger.info("=" * 41)
        
        # Set up internal metrics callback if metrics are enabled
        if self.metrics:
            def metrics_callback(latency_ms, topic, partition, success):
                if success:
                    self.metrics.record_delivered(latency_ms, topic=topic, partition=partition)
                else:
                    self.metrics.record_failed(topic=topic, partition=partition)
            self._producer_instance.set_metrics_callback(metrics_callback)
        
        return self._producer_instance
    
    
    def produce_messages(self, topic_name, test_duration, start_time, message_formatter, delivered_container, failed_container=None):
        
        async def async_produce():
            config_overrides = getattr(self, 'config_overrides', None)
            producer = self.create_producer(config_overrides)
            messages_sent = 0
            pending_futures = []
            
            # Temporary metrics for timing sections
            produce_times = []
            poll_times = []
            flush_time = 0
            
            while time.time() - start_time < test_duration:
                message_value, message_key = message_formatter(messages_sent)
                
                try:
                    # Record sent message for metrics
                    if self.metrics:
                        message_size = len(message_value.encode('utf-8')) + len(message_key.encode('utf-8'))
                        self.metrics.record_sent(message_size, topic=topic_name, partition=0)
                    
                    # Produce message
                    produce_start = time.time()
                    delivery_future = await producer.produce(
                        topic=topic_name,
                        value=message_value,
                        key=message_key
                    )
                    produce_times.append(time.time() - produce_start)
                    pending_futures.append((delivery_future, message_key))  # Store delivery future
                    messages_sent += 1

                    # Poll every 100 messages to prevent buffer overflow
                    if messages_sent % 100 == 0:
                        poll_start = time.time()
                        await producer.poll(0)
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
            await producer.flush(timeout=30)
            flush_time = time.time() - flush_start


            # Wait for all pending futures to complete (for delivery confirmation only)
            for delivery_future, message_key in pending_futures:
                try:
                    msg = await delivery_future
                    delivered_container.append(msg)
                except Exception as e:
                    if failed_container is not None:
                        failed_container.append(e)
                    self.logger.error(f"Failed to deliver message with key {message_key}: {e}")  
            
            # Print timing metrics
            avg_produce_time = sum(produce_times) / len(produce_times) * 1000 if produce_times else 0
            avg_poll_time = sum(poll_times) / len(poll_times) * 1000 if poll_times else 0
            flush_time_ms = flush_time * 1000
            
            print(f"\n=== ASYNC PRODUCER CODE PATH TIMING ===")
            print(f"Time to call AIOProducer.produce(): {avg_produce_time:.4f}ms")
            print(f"Time to call AIOProducer.poll(): {avg_poll_time:.4f}ms")
            print(f"Time to call AIOProducer.flush(): {flush_time_ms:.4f}ms")
            print(f"Total produce() calls: {len(produce_times)}")
            print(f"Total poll() calls: {len(poll_times)}")
            print("=" * 46)
            
            return messages_sent
        
        loop = asyncio.get_event_loop()
        return loop.run_until_complete(async_produce())
        
    def get_final_metrics(self):
        """Return final metrics summary for the async producer"""
        if self.metrics:
            return self.metrics
        return None
