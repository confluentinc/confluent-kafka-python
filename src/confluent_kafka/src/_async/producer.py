from confluent_kafka.cimpl import Producer
import asyncio

class AsyncProducer(Producer):
    def __init__(self, conf, loop: asyncio.AbstractEventLoop = None):
        super().__init__(conf)

        self._loop = loop or asyncio.get_event_loop()
        self._poll_task = asyncio.create_task(self._single_poll())

    def close(self):
        # Cancel poll task
        if not self._poll_task.cancelled():
            self._poll_task.cancel()

    async def produce(
            self, topic, key=None, value=None, partition=-1,
            on_delivery=None, timestamp=0, headers=None
    ):
        fut = self._loop.create_future()

        def wrapped_on_delivery(err, msg):
            if on_delivery is not None:
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

    async def _single_poll(self):
        while True:
            # Zero timeout -- return immediately, don't block
            self.poll(0)
            await asyncio.sleep(0.2)
