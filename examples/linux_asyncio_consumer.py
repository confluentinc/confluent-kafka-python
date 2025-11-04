#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright 2022 Confluent Inc.
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


# This example shows the integration of confluent_kafka.Consumer with asyncio.
#
# This file contains a class called 'AsyncConsumer' which offers the following
# methods from 'confluent_kafka.Consumer': assign, subscribe, poll
# With the advantage that the poll method is defined as 'async def poll()'.
# Allowing it to be used like this: msg = await consumer.poll()
#
# Under the hood it uses 'confluent_kafka.Consumer.io_event_enable' to avoid
# - usage of threads
# - busy loops around confluent_kafka.Consumer.poll(timeout=0)
# 'io_event_enable' makes the Consumer write to a filedescriptor in case a new
# message is ready. Hence to wait for new messages, we simply let asyncio wait
# on that filedescriptor.


# FIXME: This example uses
# * eventfd as the filedescriptor - linux only
# * asyncio.add_reader - Which is (up to now) not supported by the
#   Windows ProactorEventLoop. See the following page for more details:
#   https://docs.python.org/3.11/library/asyncio-platforms.html
# Under Windows/macOS, try socketpair or pipes as an alternative.

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
from confluent_kafka import KafkaException

assert sys.platform == 'linux', "This example is linux only, cause of eventfd"


class AsyncConsumer:
    def __init__(self, config, logger):
        """Construct a Consumer usable within asyncio.

        :param config: A configuration dict for this Consumer
        :param logger: A python logger instance.
        """

        self.consumer = Consumer(config, logger=logger)

        self.eventfd = os.eventfd(0, os.EFD_CLOEXEC | os.EFD_NONBLOCK)

        # This is the channel how the consumer notifies asyncio.
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
        """Internal helper method. Not part of the public API."""
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
        """Consumes a single message, calls callbacks and returns events.

        It is defined a 'async def' and returns an awaitable object a
        caller needs to deal with to get the result.
        See https://docs.python.org/3/library/asyncio-task.html#awaitables

        Which makes it safe (and mandatory) to call it directly in an asyncio
        coroutine like this: `msg = await consumer.poll()`

        If timeout > 0: Wait at most X seconds for a message.
                        Returns `None` if no message arrives in time.
        If timeout <= 0: Endless wait for a message.
        """
        if timeout > 0:
            try:
                return await asyncio.wait_for(self._poll_no_timeout(), timeout)
            except asyncio.TimeoutError:
                return None
        else:
            return self._poll_no_timeout()

    async def _poll_no_timeout(self):
        while not (msg := await self._single_poll()):
            pass
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
            'auto.offset.reset': 'earliest'
    }

    if arguments.client_stats:
        config['stats_cb'] = stats_cb
        config['statistics.interval.ms'] = arguments.client_stats

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
                    msg.key()
                ))
                print(msg.value())
    finally:
        consumer.close()


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-T',
        metavar='interval',
        dest='client_stats',
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
