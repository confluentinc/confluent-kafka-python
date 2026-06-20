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

"""
Unit tests for ConsumerRecords, the list subclass ShareConsumer.poll() returns.

Pure-Python and broker-free: ConsumerRecords just wraps a list of Message
objects with count()/is_empty()/records() accessors, so these build Messages
directly and exercise the container.
"""

import confluent_kafka
from confluent_kafka import ConsumerRecords, Message


def _make_message(topic='t', partition=0, offset=0, key=None, value=None):
    """Build a Message the way the C layer hands it to poll() (positional
    constructor, see tests/test_message.py)."""
    return Message(topic, partition, offset, key, value, None, None, (0, 0), -1.0, -1)


def test_is_list_subclass():
    assert issubclass(ConsumerRecords, list)
    assert isinstance(ConsumerRecords(), list)


def test_behaves_as_list():
    msgs = [_make_message(offset=i) for i in range(3)]
    cr = ConsumerRecords(msgs)
    assert len(cr) == 3
    assert cr[0] is msgs[0]
    assert list(iter(cr)) == msgs
    assert cr[1:] == msgs[1:]
    assert cr == msgs


def test_is_empty_true_when_empty():
    assert ConsumerRecords().is_empty() is True


def test_is_empty_false_when_populated():
    assert ConsumerRecords([_make_message()]).is_empty() is False


def test_count_matches_len():
    assert ConsumerRecords().count() == 0
    cr = ConsumerRecords([_make_message(offset=i) for i in range(5)])
    assert cr.count() == len(cr) == 5


def test_records_returns_plain_list():
    cr = ConsumerRecords([_make_message()])
    assert type(cr.records()) is list


def test_records_same_elements_in_order():
    msgs = [_make_message(offset=i) for i in range(4)]
    assert ConsumerRecords(msgs).records() == msgs


def test_records_returns_independent_copy():
    cr = ConsumerRecords([_make_message(offset=i) for i in range(3)])
    out = cr.records()
    assert out is not cr
    out.append(_make_message(offset=99))
    assert cr.count() == 3  # mutating the copy leaves the batch alone


def test_records_empty():
    assert ConsumerRecords().records() == []


def test_exported_from_package():
    assert "ConsumerRecords" in confluent_kafka.__all__
    assert confluent_kafka.ConsumerRecords is ConsumerRecords
