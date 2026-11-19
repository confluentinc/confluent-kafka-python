#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright 2026 Confluent Inc.
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

import json
import subprocess
import sys


def test_verifiable_producer():
    """The kafkatest verifiable producer runs as python -m and reports its deliveries as JSON.

    It produces to a librdkafka mock cluster, which replaces the required --broker-list.
    """
    result = subprocess.run(
        [
            sys.executable,
            '-m',
            'confluent_kafka.kafkatest.verifiable_producer',
            '--topic',
            'test',
            '--broker-list',
            'localhost:9092',
            '--max-messages',
            '3',
            '-X',
            'conf_test.mock.num.brokers=1',
        ],
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert result.returncode == 0, result.stderr
    events = [json.loads(line) for line in result.stdout.splitlines()]
    assert sorted(e['value'] for e in events if e['name'] == 'producer_send_success') == ['0', '1', '2']
    assert events[-1]['name'] == 'shutdown_complete'
