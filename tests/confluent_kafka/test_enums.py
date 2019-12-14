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
# limit
#

from confluent_kafka import KafkaError, \
    OFFSET_BEGINNING, OFFSET_END, OFFSET_INVALID, OFFSET_STORED, \
    TIMESTAMP_CREATE_TIME, TIMESTAMP_LOG_APPEND_TIME, TIMESTAMP_NOT_AVAILABLE


def test_enums():
    """ Make sure librdkafka error enums are reachable directly from the
        KafkaError class without an instantiated object. """
    print(KafkaError._NO_OFFSET)
    print(KafkaError.REBALANCE_IN_PROGRESS)


def test_tstype_enums():
    """ Make sure librdkafka tstype enums are available. """
    assert TIMESTAMP_NOT_AVAILABLE == 0
    assert TIMESTAMP_CREATE_TIME == 1
    assert TIMESTAMP_LOG_APPEND_TIME == 2


def test_offset_consts():
    """ Make sure librdkafka's logical offsets are available. """
    assert OFFSET_BEGINNING == -2
    assert OFFSET_END == -1
    assert OFFSET_STORED == -1000
    assert OFFSET_INVALID == -1001
