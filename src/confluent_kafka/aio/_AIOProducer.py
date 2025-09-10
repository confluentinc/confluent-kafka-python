# Copyright 2025 Confluent Inc.
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
import concurrent.futures
import confluent_kafka
from confluent_kafka import KafkaException as _KafkaException
import functools
import confluent_kafka.aio._common as _common


class AIOProducer:
    def __init__(self, producer_conf, max_workers=1,
                 executor=None, auto_poll=True):
        if executor is not None:
            self.executor = executor
        else:
            self.executor = concurrent.futures.ThreadPoolExecutor(
                max_workers=max_workers)
        loop = asyncio.get_event_loop()
        wrap_common_callbacks = _common.wrap_common_callbacks
        wrap_common_callbacks(loop, producer_conf)

        self._producer = confluent_kafka.Producer(producer_conf)
        self._running = False
        if auto_poll:
            self._running = True
            self._running_loop = asyncio.create_task(self._loop())

    async def stop(self):
        if self._running:
            self._running = False
            await self._running_loop

    async def _loop(self):
        while self._running:
            await self.poll(1.0)

    async def poll(self, *args, **kwargs):
        await self._call(self._producer.poll, *args, **kwargs)

    async def _call(self, blocking_task, *args, **kwargs):
        return (await asyncio.gather(
            asyncio.get_running_loop().run_in_executor(self.executor,
                                                       functools.partial(
                                                           blocking_task,
                                                           *args,
                                                           **kwargs))

        ))[0]

    async def produce(self, *args, **kwargs):
        loop = asyncio.get_event_loop()
        result = loop.create_future()

        def on_delivery(err, msg):
            if err:
                loop.call_soon_threadsafe(result.set_exception,
                                          _KafkaException(err))
            else:
                loop.call_soon_threadsafe(result.set_result, msg)

        kwargs['on_delivery'] = on_delivery
        # Wait for message to be queued, but don't wait for delivery
        await self._call(self._producer.produce, *args, **kwargs)
        return result

    async def init_transactions(self, *args, **kwargs):
        return await self._call(self._producer.init_transactions,
                                *args, **kwargs)

    async def begin_transaction(self, *args, **kwargs):
        return await self._call(self._producer.begin_transaction,
                                *args, **kwargs)

    async def send_offsets_to_transaction(self, *args, **kwargs):
        return await self._call(self._producer.send_offsets_to_transaction,
                                *args, **kwargs)

    async def commit_transaction(self, *args, **kwargs):
        return await self._call(self._producer.commit_transaction,
                                *args, **kwargs)

    async def abort_transaction(self, *args, **kwargs):
        return await self._call(self._producer.abort_transaction,
                                *args, **kwargs)

    async def flush(self, *args, **kwargs):
        return await self._call(self._producer.flush, *args, **kwargs)

    async def purge(self, *args, **kwargs):
        return await self._call(self._producer.purge, *args, **kwargs)

    async def set_sasl_credentials(self, *args, **kwargs):
        return await self._call(self._producer.set_sasl_credentials,
                                *args, **kwargs)
