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

# Companion code to the blog post "Integrating Kafka With Python Asyncio Web Applications"
# https://www.confluent.io/blog/[path-to-blog-post]

import asyncio
from confluent_kafka import Producer
from confluent_kafka import KafkaException
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from time import time
from threading import Thread
import uvicorn


class AIOProducer:
    def __init__(self, configs, loop):
        self._loop = loop or asyncio.get_event_loop()
        self._producer = Producer(configs)
        self._cancelled = False
        self._poll_thread = Thread(target=self._poll_loop)
        self._poll_thread.start()

    def _poll_loop(self):
        while not self._cancelled:
            self._producer.poll(0.1)

    def close(self):
        self._cancelled = True
        self._poll_thread.join()

    def async_produce(self, topic, value):
        result = self._loop.create_future()
        def ack(err, msg):
            if err:
                self._loop.call_soon_threadsafe(result.set_exception, KafkaException(err))
            else:
                self._loop.call_soon_threadsafe(result.set_result, msg)
        self._producer.produce(topic, value, on_delivery=ack)
        return result

    def produce(self, topic, value, ack=None):
        self._producer.produce(topic, value, on_delivery=ack)


app = FastAPI()

class Item(BaseModel):
    name: str

producer = None

@app.on_event("startup")
async def startup_event():
    global producer
    producer = AIOProducer(
        {"bootstrap.servers": "localhost:9092"},
        asyncio.get_event_loop())

@app.on_event("shutdown")
def shutdown_event():
    producer.close()

@app.post("/items1")
async def create_item1(item: Item):
    try:
        result = await producer.async_produce("items", item.name)
        return { "timestamp": result.timestamp() }
    except KafkaException as ex:
        raise HTTPException(status_code=500, detail=ex.args[0].str())

cnt = 0
def ack(err, msg):
    global cnt
    cnt = cnt + 1

@app.post("/items2")
async def create_item2(item: Item):
    try:
        result = producer.produce("items", item.name, ack=ack)
        return { "timestamp": time() }
    except KafkaException as ex:
        raise HTTPException(status_code=500, detail=ex.args[0].str())

@app.post("/items3")
async def create_item3(item: Item):
    try:
        result = producer.produce("items", item.name)
        return { "timestamp": time() }
    except KafkaException as ex:
        raise HTTPException(status_code=500, detail=ex.args[0].str())

@app.post("/items4")
async def create_item4(item: Item):
    return { "timestamp": time() }


if __name__ == '__main__':
    uvicorn.run(app, host='0.0.0.0', port=8000)
