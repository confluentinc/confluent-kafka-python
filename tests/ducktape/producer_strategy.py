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

    def get_final_metrics(self):
        return None


class SyncProducerStrategy(ProducerStrategy):
    def create_producer(self):
        producer = Producer({'bootstrap.servers': self.bootstrap_servers})
        return producer
    
    def get_final_metrics(self):
        """Sync producer has no built-in metrics like AIOProducer"""
        return None
    
    def produce_messages(self, topic_name, test_duration, start_time, message_formatter, delivered_container, failed_container=None):
        producer = self.create_producer()
        messages_sent = 0
        send_times = {}  # Track send times for latency calculation
        
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
                
                producer.produce(
                    topic=topic_name,
                    value=message_value,
                    key=message_key,
                    on_delivery=delivery_callback
                )
                messages_sent += 1
                
                # Poll every 100 messages to prevent buffer overflow
                if messages_sent % 100 == 0:
                    producer.poll(0)
                    if self.metrics:
                        self.metrics.record_poll()
                        
            except Exception as e:
                if failed_container is not None:
                    failed_container.append(e)
                if self.metrics:
                    self.metrics.record_failed(topic=topic_name, partition=0)
                self.logger.error(f"Failed to produce message {messages_sent}: {e}")
        
        producer.flush(timeout=30)
        return messages_sent


class AsyncProducerStrategy(ProducerStrategy):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._producer_instance = None
    
    def create_producer(self):
        from confluent_kafka.aio import AIOProducer
        # Enable logging for AIOProducer
        import logging
        logging.basicConfig(level=logging.INFO)
        
        self._producer_instance = AIOProducer({'bootstrap.servers': self.bootstrap_servers}, max_workers=1, auto_poll=True)
        return self._producer_instance
    
    def get_final_metrics(self):
        """Get metrics from the AIOProducer instance"""
        if self._producer_instance:
            return self._producer_instance.get_metrics()
        return None
    
    def produce_messages(self, topic_name, test_duration, start_time, message_formatter, delivered_container, failed_container=None):
        
        async def async_produce():
            producer = self.create_producer()
            messages_sent = 0
            pending_futures = []
            send_times = {}  # Track send times for latency calculation
            
            while time.time() - start_time < test_duration:
                message_value, message_key = message_formatter(messages_sent)
                
                try:
                    # Record sent message for metrics and track send time
                    if self.metrics:
                        message_size = len(message_value.encode('utf-8')) + len(message_key.encode('utf-8'))
                        self.metrics.record_sent(message_size, topic=topic_name, partition=0)
                        send_times[message_key] = time.time()  # Track send time for latency
                    
                    # Get future from produce call
                    future = producer.produce(
                        topic=topic_name,
                        value=message_value,
                        key=message_key
                    )
                    pending_futures.append((future, message_key))  # Store key with future
                    messages_sent += 1
                    
                except Exception as e:
                    if failed_container is not None:
                        failed_container.append(e)
                    if self.metrics:
                        self.metrics.record_failed(topic=topic_name, partition=0)
                    self.logger.error(f"Failed to produce message {messages_sent}: {e}")
            
            # Wait for all futures to complete using gather for better performance
            try:
                futures_only = [future for future, key in pending_futures]
                results = await asyncio.gather(*futures_only, return_exceptions=True)
                
                for i, result in enumerate(results):
                    future, message_key = pending_futures[i]
                    
                    if isinstance(result, Exception):
                        # Handle failed message
                        if failed_container is not None:
                            failed_container.append(result)
                        if self.metrics:
                            self.metrics.record_failed(topic=topic_name, partition=0)
                    else:
                        # Handle successful message
                        delivered_container.append(result)
                        
                        # Calculate real latency like sync producer
                        if self.metrics:
                            topic = result.topic() if hasattr(result, 'topic') else topic_name
                            partition = result.partition() if hasattr(result, 'partition') else 0
                            
                            # Calculate latency from send time to now
                            latency_ms = 0.0
                            if message_key in send_times:
                                latency_ms = (time.time() - send_times[message_key]) * 1000
                                del send_times[message_key]  # Clean up
                            
                            self.metrics.record_delivered(latency_ms, topic=topic, partition=partition)
                            
            except Exception as e:
                # Handle gather-level exception (shouldn't happen with return_exceptions=True)
                if failed_container is not None:
                    failed_container.append(e)
                if self.metrics:
                    self.metrics.record_failed(topic=topic_name, partition=0)
            
            await producer.flush(timeout=30)
            await producer.stop()
            return messages_sent
        
        loop = asyncio.get_event_loop()
        return loop.run_until_complete(async_produce())
