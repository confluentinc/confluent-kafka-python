#!/usr/bin/env python

import json
import random
import sys
import time

from confluent_kafka import Consumer, TopicPartition
c = Consumer({
    'bootstrap.servers':'localhost:9092',
    'group.id': 'test-topic-consumer-group',
    'auto.offset.reset': 'earliest',
    'enable.auto.offset.store': False,
    'enable.auto.commit': True,
})
topic = 'test-topic-vv'

def test_for_seek():
    try:
        pp = []
        pp.append(TopicPartition(topic, partition=8))
        c.assign(pp)
        while True:
            msgs = c.consume(num_messages=10, timeout=10)
            if not msgs:
                print('no data and wait')
                for i in c.assignment():
                    print(i.topic, i.partition, i.offset, c.get_watermark_offsets(i))
                continue
            for msg in msgs:
                t1 = msg.partition()
                o1 = msg.offset()
                print('Received message: {} par {} offset {}'.format(msg.value().decode('utf-8'), t1, o1))
            break
    finally:
        c.close()

def test_for_run():
    try:
        c.subscribe([topic])
        total_count = 0
        map_par = {}
        while True:
            # print("Assignments")
            # for i in c.assignment():
            #     print(i.topic, i.partition, i.offset, c.get_watermark_offsets(i))
            # print("New Consume started")
            msgs = c.consume(num_messages=10, timeout=5)
            if not msgs:
                # print('no data and wait')
                # for i in c.assignment():
                    # print(i.topic, i.partition, i.offset, c.get_watermark_offsets(i))
                continue
            deald = []
            currTime  = time.time()
            split = str(currTime).split(".")
            print("------------------------------- Reading messages (" + str(currTime) +") " + time.asctime(time.localtime(int(split[0]))) + " milli -> " + split[1] + " -------------------------")
            for msg in msgs:
                t1 = msg.partition()
                o1 = msg.offset()
                error = msg.error()
                print('Received message: {} partition {} offset {}'.format(msg.value().decode('utf-8'), t1, o1))
                if random.randint(1, 100) == 9:
                    # test for deal failed then retry again
                    print('deal failed will retry msg offset {} partition {}'.format(msg.offset(), msg.partition()))
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
                                print("msg partition: " + str(x.partition()) + " offset: " + str(x.offset()))
                            raise Exception('deal partition {} except last offset {} current offset {}'.format(t1, map_par[t1], o1))
                    map_par[t1] = o1
                    c.store_offsets(msg)
                    deald.append(msg)
            group_partition = {}
            print("-------------------- creating group partition -------------------------")
            for msg in msgs:
                partition = msg.partition()
                offset = msg.offset()
                if msg in deald:
                    print("Continue deal record: " + str(partition) + " offset: " + str(offset))
                    continue
                if partition in group_partition:
                    print("Updating group partition record: " + str(partition) + " offset: " + str(offset))
                    group_partition[partition] = min(group_partition[partition], offset)
                else:
                    print("Creating group partition record: " + str(partition) + " offset: " + str(offset))
                    group_partition[partition] = offset
            # seek to deal failed partition offset
            for k, v in group_partition.items():
                print('---------------------------- seeking ------------------------------------')
                c.seek(TopicPartition(topic, partition=k, offset=v))
                print('deal failed will set msg offset {} partition {}'.format(v, k))
    finally:
        c.close()

if sys.argv[1] == 'test_for_seek':
    test_for_seek()
else:
    test_for_run()