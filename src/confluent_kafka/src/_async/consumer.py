import asyncio

from confluent_kafka.cimpl import Consumer

ASYNC_CONSUMER_POLL_INTERVAL_SECONDS: int = 0.2
ASYNC_CONSUMER_POLL_INFINITE_TIMEOUT_SECONDS: int = -1

class AsyncConsumer(Consumer):
    def __init__(
            self,
            conf: dict,
            loop: asyncio.AbstractEventLoop = None,
            poll_interval_seconds: int = ASYNC_CONSUMER_POLL_INTERVAL_SECONDS
        ):
        super().__init__(conf)

        self._loop = loop or asyncio.get_event_loop()
        self._poll_interval = poll_interval_seconds

    def __aiter__(self):
        return self

    async def __anext__(self):
        return await self.poll(None)

    async def poll(self, timeout: int = None):
        # -1 is equivalent to disabling timeout
        timeout = None if timeout == ASYNC_CONSUMER_POLL_INFINITE_TIMEOUT_SECONDS else timeout
        async with asyncio.timeout(timeout):
            while True:
                msg = super().poll(0)
                if msg is not None:
                    return msg
                else:
                    await asyncio.sleep(self._poll_interval)
