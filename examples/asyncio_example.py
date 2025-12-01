#!/usr/bin/env python
#
# Copyright 2019 Confluent Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import asyncio
import logging
import random
import signal
import sys

from confluent_kafka.experimental.aio import AIOConsumer, AIOProducer

# This example demonstrates comprehensive AsyncIO usage patterns with Kafka:
# - Event loop safe callbacks that don't block the loop
# - Batched async produce with transaction handling
# - Proper async consumer with partition management
# - Graceful shutdown with signal handling
# - Thread pool integration for blocking operations

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
running = True


# AsyncIO Pattern: Event loop safe callbacks
# These callbacks are automatically scheduled onto the event loop by AIOProducer/AIOConsumer
# ensuring they don't block the loop and can safely interact with other async operations
async def error_cb(err):
    logger.error(f'Kafka error: {err}')


async def throttle_cb(event):
    logger.warning(f'Kafka throttle event: {event}')


async def stats_cb(stats_json_str):
    logger.info(f'Kafka stats: {stats_json_str}')


def configure_common(conf):
    bootstrap_servers = sys.argv[1]
    conf.update(
        {
            'bootstrap.servers': bootstrap_servers,
            'logger': logger,
            'debug': 'conf',
            'error_cb': error_cb,
            'throttle_cb': throttle_cb,
            'stats_cb': stats_cb,
            'statistics.interval.ms': 5000,
        }
    )

    return conf


async def run_producer():
    topic = sys.argv[2]
    # AsyncIO Pattern: Non-blocking producer with thread pool
    # max_workers=5 creates a ThreadPoolExecutor for offloading blocking librdkafka calls
    producer = AIOProducer(configure_common({'transactional.id': 'producer1'}), max_workers=5)

    # AsyncIO Pattern: Async transaction lifecycle
    # All transaction operations are awaitable and won't block the event loop
    await producer.init_transactions()
    # TODO: handle exceptions with transactional API
    transaction_active = False
    try:
        while running:
            await producer.begin_transaction()
            transaction_active = True

            # AsyncIO Pattern: Batched async produce with concurrent futures
            # Creates 100 concurrent produce operations, each returning a Future
            # that resolves when the message is delivered or fails
            produce_futures = [
                await producer.produce(topic=topic, key=f'testkey{i}', value=f'testvalue{i}') for i in range(10)
            ]

            logger.info(f"Produced {len(produce_futures)} messages")
            # Force a flush of the local buffer to ensure messages will be in flight before awaiting their delivery
            # TODO: this shouldn't be strictly necessary in the future
            await producer.flush()
            # Wait for all produce operations to complete concurrently
            for msg in await asyncio.gather(*produce_futures):
                logger.info('Produced to: {} [{}] @ {}'.format(msg.topic(), msg.partition(), msg.offset()))

            # AsyncIO Pattern: Non-blocking transaction commit
            await producer.commit_transaction()
            transaction_active = False
            # Use asyncio.sleep() instead of time.sleep() to yield control to event loop
            # Change this to sleep(0) in a real application as this is mimicing doing external work on the event loop
            await asyncio.sleep(1)
    except Exception as e:
        logger.error(e)
    finally:
        # AsyncIO Pattern: Proper async cleanup
        # Always clean up resources asynchronously to avoid blocking the event loop
        if transaction_active:
            await producer.abort_transaction()
        await producer.close()  # Stops background tasks and closes connections
        logger.info('Closed producer')


async def run_consumer():
    topic = sys.argv[2]
    group_id = f'{topic}_{random.randint(1, 1000)}'
    # AsyncIO Pattern: Non-blocking consumer with manual offset management
    # Callbacks will be scheduled on the event loop automatically
    consumer = AIOConsumer(
        configure_common(
            {
                'group.id': group_id,
                'auto.offset.reset': 'latest',
                'enable.auto.commit': 'false',  # Manual commit for precise control
                'enable.auto.offset.store': 'false',  # Manual offset storage
                'partition.assignment.strategy': 'cooperative-sticky',
            }
        )
    )

    # AsyncIO Pattern: Async rebalance callbacks
    # These callbacks can perform async operations safely within the event loop
    async def on_assign(consumer, partitions):
        # Calling incremental_assign is necessary to pause the assigned partitions
        # otherwise it'll be done by the consumer after callback termination.
        await consumer.incremental_assign(partitions)
        await consumer.pause(partitions)  # Demonstrates async partition control
        logger.debug(f'on_assign {partitions}')
        # Resume the partitions as it's just a pause example
        await consumer.resume(partitions)

    async def on_revoke(consumer, partitions):
        logger.debug(
            f'before on_revoke {partitions}',
        )
        try:
            # AsyncIO Pattern: Non-blocking commit during rebalance
            await consumer.commit()  # Ensure offsets are committed before losing partitions
        except Exception as e:
            logger.info(f'Error during commit: {e}')
        logger.debug(f'after on_revoke {partitions}')

    async def on_lost(consumer, partitions):
        logger.debug(f'on_lost {partitions}')

    try:
        await consumer.subscribe(
            [topic],
            on_assign=on_assign,
            on_revoke=on_revoke,
            # Remember to set a on_lost callback
            # if you're committing on revocation
            # as lost partitions cannot be committed
            on_lost=on_lost,
        )
        i = 0
        while running:
            # AsyncIO Pattern: Non-blocking message polling
            # poll() returns a coroutine that yields control back to the event loop
            message = await consumer.poll(1.0)
            if message is None:
                continue

            if i % 100 == 0:
                # AsyncIO Pattern: Async metadata operations
                # Both assignment() and position() are async and won't block the loop
                position = await consumer.position(await consumer.assignment())
                logger.info(f'Current position: {position}')
                await consumer.commit()  # Async commit of stored offsets
                logger.info('Stored offsets were committed')

            err = message.error()
            if err:
                logger.error(f'Error: {err}')
            else:
                logger.info(f'Consumed: {message.value()}')
                # AsyncIO Pattern: Async offset storage
                await consumer.store_offsets(message=message)
                i += 1
    finally:
        # AsyncIO Pattern: Proper async consumer cleanup
        # Always unsubscribe and close asynchronously
        await consumer.unsubscribe()  # Leave consumer group gracefully
        await consumer.close()  # Close connections and stop background tasks
        logger.info('Closed consumer')


# AsyncIO Pattern: Signal handling for graceful shutdown
# Sets a flag that async tasks check to terminate cleanly
def signal_handler(*_):
    global running
    logger.info('Signal received, shutting down...')
    running = False


async def main():
    # AsyncIO Pattern: Signal handling setup
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # AsyncIO Pattern: Concurrent task execution
    # Both producer and consumer run concurrently in the same event loop
    producer_task = asyncio.create_task(run_producer())
    consumer_task = asyncio.create_task(run_consumer())
    # Wait for both tasks to complete (or be cancelled by signal)
    await asyncio.gather(producer_task, consumer_task)


try:
    asyncio.run(main())
except asyncio.exceptions.CancelledError as e:
    logger.warning(f'Asyncio task was cancelled: {e}')

logger.info('End of example')
