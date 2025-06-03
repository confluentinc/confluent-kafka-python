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
import sys
from confluent_kafka.aio import AIOProducer
from confluent_kafka.aio import AIOConsumer
import random
import logging
import signal

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
running = True


async def error_cb(err):
    logger.error(f'Kafka error: {err}')


async def throttle_cb(event):
    logger.warning(f'Kafka throttle event: {event}')


async def stats_cb(stats_json_str):
    logger.info(f'Kafka stats: {stats_json_str}')


def configure_common(conf):
    bootstrap_servers = sys.argv[1]
    conf.update({
        'bootstrap.servers': bootstrap_servers,
        'logger': logger,
        'debug': 'conf',
        'error_cb': error_cb,
        'throttle_cb': throttle_cb,
        'stats_cb': stats_cb,
        'statistics.interval.ms': 5000,
    })

    return conf


async def run_producer():
    topic = sys.argv[2]
    producer = AIOProducer(configure_common(
        {
            'transactional.id': 'producer1'
        }), max_workers=5)

    await producer.init_transactions()
    # TODO: handle exceptions with transactional API
    transaction_active = False
    try:
        while running:
            await producer.begin_transaction()
            transaction_active = True

            produce_futures = [asyncio.create_task(
                producer.produce(topic=topic,
                                 key=f'testkey{i}',
                                 value=f'testvalue{i}'))
                               for i in range(100)]
            results = await asyncio.gather(*produce_futures)

            for msg in results:
                logger.info(
                    'Produced to: {} [{}] @ {}'.format(msg.topic(),
                                                       msg.partition(),
                                                       msg.offset()))

            await producer.commit_transaction()
            transaction_active = False
            await asyncio.sleep(1)
    except Exception as e:
        logger.error(e)
    finally:
        if transaction_active:
            await producer.abort_transaction()
        await producer.stop()
        logger.info('Closed producer')


async def run_consumer():
    topic = sys.argv[2]
    group_id = f'{topic}_{random.randint(1, 1000)}'
    consumer = AIOConsumer(configure_common(
        {
            'group.id': group_id,
            'auto.offset.reset': 'latest',
            'enable.auto.commit': 'false',
            'enable.auto.offset.store': 'false',
            'partition.assignment.strategy': 'cooperative-sticky',
        }))

    async def on_assign(consumer, partitions):
        # Calling incremental_assign is necessary pause the assigned partitions
        # otherwise it'll be done by the consumer after callback termination.
        await consumer.incremental_assign(partitions)
        await consumer.pause(partitions)
        logger.debug(f'on_assign {partitions}')
        # Resume the partitions as it's just a pause example
        await consumer.resume(partitions)

    async def on_revoke(consumer, partitions):
        logger.debug(f'before on_revoke {partitions}', )
        try:
            await consumer.commit()
        except Exception as e:
            logger.info(f'Error during commit: {e}')
        logger.debug(f'after on_revoke {partitions}')

    async def on_lost(consumer, partitions):
        logger.debug(f'on_lost {partitions}')

    try:
        await consumer.subscribe([topic],
                                 on_assign=on_assign,
                                 on_revoke=on_revoke,
                                 # Remember to set a on_lost callback
                                 # if you're committing on revocation
                                 # as lost partitions cannot be committed
                                 on_lost=on_lost)
        i = 0
        while running:
            message = await consumer.poll(1.0)
            if message is None:
                continue

            if i % 100 == 0:
                position = await consumer.position(await consumer.assignment())
                logger.info(f'Current position: {position}')
                await consumer.commit()
                logger.info('Stored offsets were committed')

            err = message.error()
            if err:
                logger.error(f'Error: {err}')
            else:
                logger.info(f'Consumed: {message.value()}')
                await consumer.store_offsets(message=message)
                i += 1
    finally:
        await consumer.unsubscribe()
        await consumer.close()
        logger.info('Closed consumer')


def signal_handler(*_):
    global running
    logger.info('Signal received, shutting down...')
    running = False


async def main():
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    producer_task = asyncio.create_task(run_producer())
    consumer_task = asyncio.create_task(run_consumer())
    await asyncio.gather(producer_task, consumer_task)

try:
    asyncio.run(main())
except asyncio.exceptions.CancelledError as e:
    logger.warning(f'Asyncio task was cancelled: {e}')
    pass
logger.info('End of example')
