#!/usr/bin/env python
#
# Copyright 2024 Confluent Inc.
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
#

import logging
import sys

from confluent_kafka import Consumer

if len(sys.argv) != 2:
    sys.stderr.write("Usage: %s <broker>\n" % sys.argv[0])
    sys.exit(1)

broker = sys.argv[1]

# Custom logger
logger = logging.getLogger('Consumer')
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter('%(asctime)-15s %(levelname)-8s %(message)s'))
logger.addHandler(handler)

# Create Consumer with logger
# The logger must be an object with a log(level, msg, *args) method,
# such as logging.Logger or logging.LoggerAdapter.
c = Consumer({'bootstrap.servers': broker,
              'group.id': 'example-logger-group',
              'debug': 'all'},
             logger=logger)

# Alternatively, pass the logger as a key in the config dict.
# When passed as a kwarg, it overwrites the config key.
#
# c = Consumer({'bootstrap.servers': broker,
#               'group.id': 'example-logger-group',
#               'debug': 'all',
#               'logger': logger})

c.subscribe(['test'])

# Log messages are forwarded when poll() is called
for _ in range(10):
    msg = c.poll(timeout=0.5)
    if msg is not None and not msg.error():
        print("Received: %s" % msg.value().decode('utf-8'))

c.close()
