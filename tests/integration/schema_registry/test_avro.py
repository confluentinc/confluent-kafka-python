#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
#
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
#

from uuid import uuid4

import pytest


@pytest.mark.timeout(40)
def test_avro(sr_fixture, schema_fixture,
              error_cb_fixture, print_commit_callback_fixture):
    base_conf = sr_fixture.client_conf

    base_conf.update({'error_cb': error_cb_fixture,
                      'api.version.fallback.ms': 0,
                      'broker.version.fallback': '0.11.0.0',
                      'schema.registry.url': sr_fixture.schema_registry})

    consumer_conf = dict(base_conf, **{
        'group.id': 'test.py',
        'session.timeout.ms': 6000,
        'enable.auto.commit': False,
        'on_commit': print_commit_callback_fixture,
        'auto.offset.reset': 'earliest'})

    run_avro_loop(base_conf, consumer_conf, schema_fixture)


def run_avro_loop(producer_conf, consumer_conf, schema_fixture):
    from confluent_kafka import avro

    p = avro.AvroProducer(producer_conf)

    prim_float = schema_fixture('primitive_float')
    prim_string = schema_fixture('primitive_string')
    basic = schema_fixture("basic_schema")

    str_value = 'abc'
    float_value = 32.0

    combinations = [
        dict(key=float_value, key_schema=prim_float),
        dict(value=float_value, value_schema=prim_float),
        dict(key={'name': 'abc'}, key_schema=basic),
        dict(value={'name': 'abc'}, value_schema=basic),
        dict(value={'name': 'abc'}, value_schema=basic, key=float_value, key_schema=prim_float),
        dict(value={'name': 'abc'}, value_schema=basic, key=str_value, key_schema=prim_string),
        dict(value=float_value, value_schema=prim_float, key={'name': 'abc'}, key_schema=basic),
        dict(value=float_value, value_schema=prim_float, key=str_value, key_schema=prim_string),
        dict(value=str_value, value_schema=prim_string, key={'name': 'abc'}, key_schema=basic),
        dict(value=str_value, value_schema=prim_string, key=float_value, key_schema=prim_float),
        # Verify identity check allows Falsy object values(e.g., 0, empty string) to be handled properly (issue #342)
        dict(value='', value_schema=prim_string, key=0.0, key_schema=prim_float),
        dict(value=0.0, value_schema=prim_float, key='', key_schema=prim_string),
    ]

    for i, combo in enumerate(combinations):
        combo['topic'] = str(uuid4())
        combo['headers'] = [('index', str(i))]
        p.produce(**combo)
    p.flush()

    c = avro.AvroConsumer(consumer_conf)
    c.subscribe([(t['topic']) for t in combinations])

    msgcount = 0
    while msgcount < len(combinations):
        msg = c.poll(1)

        if msg is None:
            continue
        if msg.error():
            print(msg.error())
            continue

        tstype, timestamp = msg.timestamp()
        print('%s[%d]@%d: key=%s, value=%s, tstype=%d, timestamp=%s' %
              (msg.topic(), msg.partition(), msg.offset(),
               msg.key(), msg.value(), tstype, timestamp))

        # omit empty Avro fields from payload for comparison
        record_key = msg.key()
        record_value = msg.value()
        index = int(dict(msg.headers())['index'])

        if isinstance(msg.key(), dict):
            record_key = {k: v for k, v in msg.key().items() if v is not None}

        if isinstance(msg.value(), dict):
            record_value = {k: v for k, v in msg.value().items() if v is not None}

        assert combinations[index].get('key') == record_key
        assert combinations[index].get('value') == record_value

        c.commit()
        msgcount += 1

    # Close consumer
    c.close()
