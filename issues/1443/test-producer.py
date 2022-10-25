#!/usr/bin/env python

import json
import random
import time

from confluent_kafka import Producer

p = Producer({'bootstrap.servers':'localhost:9092',})
total_count = 0
c = 0
try:
    for i in range(1000):
        num = random.randint(1, 1000000)
        total_count += num
        a = {'t': num, 'time': time.time()}
        p.produce('testcase-topic', json.dumps(a))
        c += 1
        if c %100 == 0:
            p.flush()
finally:
    p.flush()