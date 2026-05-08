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

from confluent_kafka import Producer

if len(sys.argv) != 2:
    sys.stderr.write("Usage: %s <broker>\n" % sys.argv[0])
    sys.exit(1)

broker = sys.argv[1]

# Custom logger
logger = logging.getLogger('Producer')
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter('%(asctime)-15s %(levelname)-8s %(message)s'))
logger.addHandler(handler)

# Create Producer with logger
# The logger must be an object with a log(level, msg, *args) method,
# such as logging.Logger or logging.LoggerAdapter.
p = Producer({'bootstrap.servers': broker, 'debug': 'all'}, logger=logger)

# Alternatively, pass the logger as a key in the config dict.
# When passed as a kwarg, it overwrites the config key.
#
# p = Producer({'bootstrap.servers': broker,
#               'debug': 'all',
#               'logger': logger})

# Log messages are forwarded when poll() or flush() is called
p.poll(0)
p.flush()
