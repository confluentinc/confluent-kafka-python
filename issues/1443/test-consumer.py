#!/usr/bin/env python

import json
import random
import sys

from confluent_kafka import Consumer, TopicPartition
c = Consumer({
    'bootstrap.servers':'localhost:9092',
    'group.id': 'testcase-group',
    'auto.offset.reset': 'earliest',
    'enable.auto.offset.store': False,
    'enable.auto.commit': False
})
topic = 'testcase-topic'

def test_for_run():
    try:
        c.subscribe([topic])
        total_count = 0
        map_par = {}
        while True:
            msgs = c.consume(num_messages=10, timeout=5)
            if not msgs:
                print('no data and wait')
                for i in c.assignment():
                    print(i.topic, i.partition, i.offset, c.get_watermark_offsets(i))
                continue
            deald = []
            for msg in msgs:
                t1 = msg.partition()
                o1 = msg.offset()
                error = msg.error()
                print('Received message: {} par {} offset {}'.format(msg.value().decode('utf-8'), t1, o1))
                if random.randint(1, 100) == 9:
                    # test for deal failed then retry again
                    print("-------------------------------------------------DEAL FAILED----------------------------------------------------")
                    print('deal failed will retry msg offset {} partition {}'.format(msg.offset(), msg.partition()))
                    for x in msgs:
                        print("msg partition: '" + str(x.partition()) + "' offset: '" + str(x.offset()) + "'")
                    print("-------------------------------------------------DEAL FAILED END----------------------------------------------------")
                    break
                else:
                    total_count += json.loads(msg.value())['t']
                    # test for deal success
                    if t1 in map_par:
                        if map_par[t1] + 1 != o1:
                            print("----------------------------------------------ERROR-----------------------------------------------")
                            if error:
                                print(error)
                            for x in msgs:
                                print("msg partition: '" + str(x.partition()) + "' offset: '" + str(x.offset()) + "'")
                            raise Exception('deal partition {} except last offset {} current offset {}'.format(t1, map_par[t1], o1))
                    map_par[t1] = o1
                    c.store_offsets(msg)
                    c.commit(msg)
                    deald.append(msg)
    finally:
        c.close()

test_for_run()