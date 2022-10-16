import argparse
import asyncio
import os
import struct
import sys
import weakref

import logging
import json
import pprint

from confluent_kafka import Consumer


assert sys.platform == 'linux', "This example is linux only, cause of eventfd"


class AsyncConsumer:
    def __init__(self, config, logger):
        self.consumer = Consumer(config, logger=logger)

        # Sorry Windows/MacOX try something with socketpair or pipe.
        self.eventfd = os.eventfd(0, os.EFD_CLOEXEC | os.EFD_NONBLOCK)

        # This is channel how librdkafka notifies asyncio.
        self.loop = asyncio.get_running_loop()
        self.loop.add_reader(self.eventfd, self.__eventfd_ready)
        self.consumer.io_event_enable(self.eventfd, struct.pack('@q', 1))

        self.waiters = set()

        # Close eventfd and remove it from reader if
        # self is not referenced anymore.
        self.__close_eventfd = weakref.finalize(
            self,
            AsyncConsumer.close_eventd, self.loop, self.eventfd
        )

    @staticmethod
    def close_eventd(loop, eventfd):
        loop.remove_reader(eventfd)
        os.close(eventfd)

    def close(self):
        self.consumer.close()

    def __eventfd_ready(self):
        os.eventfd_read(self.eventfd)

        for future in self.waiters:
            if not future.done():
                future.set_result(True)

    def subscribe(self, *args, **kwargs):
        self.consumer.subscribe(*args, **kwargs)

    def assign(self, *args, **kwargs):
        self.consumer.assign(*args, **kwargs)

    async def poll(self, timeout=0):
        if timeout > 0:
            try:
                return await asyncio.wait_for(self._poll_no_timeout(), timeout)
            except asyncio.TimeoutError:
                return None
        else:
            return self._poll_no_timeout()

    async def _poll_no_timeout(self):
        while not (msg := await self._single_poll()): pass
        return msg

    async def _single_poll(self):
        if (msg := self.consumer.poll(timeout=0)) is not None:
            return msg

        awaitable = self.loop.create_future()
        self.waiters.add(awaitable)
        try:
            # timeout=2 is there for two reasons:
            # 1) self.consumer.poll needs to be called reguraly for other
            #    activities like: log callbacks.
            # 2) Ensures progress even if something with eventfd
            #    notification goes wrong.
            await asyncio.wait_for(awaitable, timeout=2)
        except asyncio.TimeoutError:
            return None
        finally:
            self.waiters.discard(awaitable)


async def main():
    arguments = parse_args()

    logger = logging.getLogger('consumer')
    logger.setLevel(logging.DEBUG)
    handler = logging.StreamHandler()
    fmt = '%(asctime)-15s %(levelname)-8s %(message)s'
    handler.setFormatter(logging.Formatter(fmt))
    logger.addHandler(handler)

    config = {
            'bootstrap.servers': arguments.bootstrap_servers,
            'group.id': arguments.group_id,
            'session.timeout.ms': 6000,
            'auto.offset.reset': 'earliest'
    }

    if arguments.client_status:
        config['stats_cb'] = stats_cb
        config['statistics.interval.ms'] = arguments.client_status

    consumer = AsyncConsumer(config, logger)
    consumer.subscribe(arguments.topic, on_assign=print_assignment)

    try:
        while True:
            msg = await consumer.poll(timeout=10)
            if msg is None:
                continue

            if msg.error():
                print(KafkaException(msg.error()))
            else:
                print('%% %s [%d] at offset %d with key %s:\n' % (
                    msg.topic(),
                    msg.partition(),
                    msg.offset(),
                    str(msg.key())
                ))
                print(msg.value())
    finally:
        consumer.close()


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-T',
        metavar='interval',
        dest='client_status',
        type=int,
        default=None
    )

    parser.add_argument('bootstrap_servers')
    parser.add_argument('group_id')
    parser.add_argument('topic', nargs='+')

    return parser.parse_args()


def stats_cb(stats_json_str):
    stats_json = json.loads(stats_json_str)
    print('\nKAFKA Stats: {}\n'.format(pprint.pformat(stats_json)))


def print_assignment(consumer, partitions):
    print('Assignment:', partitions)


asyncio.run(main())
