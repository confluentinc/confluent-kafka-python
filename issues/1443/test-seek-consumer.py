#!/usr/bin/env python

import json
import random
import sys

from confluent_kafka import Consumer, TopicPartition
c = Consumer({
    'bootstrap.servers':'localhost:9092',
    'group.id': 'test-seek-group',
    'auto.offset.reset': 'earliest',
    'enable.auto.offset.store': False,
    'enable.auto.commit': True,
})
topic = 'test-seek-topic'

def test_for_run():
    try:
        c.subscribe([topic])
        final_offset = 0
        while True:
            msgs = c.consume(num_messages=10, timeout=5)
            if not msgs:
                print('no data and wait')
                for i in c.assignment():
                    print(i.topic, i.partition, i.offset, c.get_watermark_offsets(i))
                continue
            for msg in msgs:
                t1 = msg.partition()
                o1 = msg.offset()
                print('Received message: {} par {} offset {}'.format(msg.value().decode('utf-8'), t1, o1))
                final_offset = o1
                c.store_offsets(msg)
            c.seek(TopicPartition(topic, partition=0, offset=(final_offset-5)))
            print('Seek to ' + str(final_offset-5))
    finally:
        c.close()

test_for_run()