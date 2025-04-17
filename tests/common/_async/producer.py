from confluent_kafka.cimpl import Producer
import inspect
import asyncio

from confluent_kafka.error import KeySerializationError, ValueSerializationError
from confluent_kafka.serialization import MessageField, SerializationContext

ASYNC_PRODUCER_POLL_INTERVAL: int = 0.2

class AsyncProducer(Producer):
    def __init__(
            self,
            conf: dict,
            loop: asyncio.AbstractEventLoop = None,
            poll_interval: int = ASYNC_PRODUCER_POLL_INTERVAL
        ):
        super().__init__(conf)

        self._loop = loop or asyncio.get_event_loop()
        self._poll_interval = poll_interval

        self._poll_task = None
        self._waiters: int = 0

    async def produce(
            self, topic, value=None, key=None, partition=-1,
            on_delivery=None, timestamp=0, headers=None
    ):
        fut = self._loop.create_future()
        self._waiters += 1
        try:
            if self._poll_task is None or self._poll_task.done():
                self._poll_task = asyncio.create_task(self._poll_dr(self._poll_interval))

            def wrapped_on_delivery(err, msg):
                if on_delivery is not None:
                    if inspect.iscoroutinefunction(on_delivery):
                        asyncio.run_coroutine_threadsafe(
                            on_delivery(err, msg),
                            self._loop
                        )
                    else:
                        self._loop.call_soon_threadsafe(on_delivery, err, msg)

                if err:
                    self._loop.call_soon_threadsafe(fut.set_exception, err)
                else:
                    self._loop.call_soon_threadsafe(fut.set_result, msg)

            super().produce(
                topic, 
                value, 
                key, 
                headers=headers, 
                partition=partition, 
                timestamp=timestamp, 
                on_delivery=wrapped_on_delivery
            )
            return await fut
        finally:
            self._waiters -= 1

    async def _poll_dr(self, interval: int):
        """Poll delivery reports at interval seconds"""
        while self._waiters:
            super().poll(0)
            await asyncio.sleep(interval)


class TestAsyncSerializingProducer(AsyncProducer):
    def __init__(self, conf):
        conf_copy = conf.copy()

        self._key_serializer = conf_copy.pop('key.serializer', None)
        self._value_serializer = conf_copy.pop('value.serializer', None)

        super(TestAsyncSerializingProducer, self).__init__(conf_copy)

    async def produce(self, topic, key=None, value=None, partition=-1,
                on_delivery=None, timestamp=0, headers=None):
        ctx = SerializationContext(topic, MessageField.KEY, headers)
        if self._key_serializer is not None:
            try:
                key = await self._key_serializer(key, ctx)
            except Exception as se:
                raise KeySerializationError(se)
        ctx.field = MessageField.VALUE
        if self._value_serializer is not None:
            try:
                value = await self._value_serializer(value, ctx)
            except Exception as se:
                raise ValueSerializationError(se)

        return await super().produce(topic, value, key,
                                                 headers=headers,
                                                 partition=partition,
                                                 timestamp=timestamp,
                                                 on_delivery=on_delivery)
