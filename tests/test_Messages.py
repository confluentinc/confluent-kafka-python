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
Unit tests for Messages, the container ShareConsumer.poll() returns.

Pure-Python and broker-free -- they build Messages directly, no broker.
"""

import confluent_kafka
from confluent_kafka import Message, Messages


def _make_message(topic='t', partition=0, offset=0, key=None, value=None):
    """Build a Message the way the C layer hands it to poll() (positional
    constructor, see tests/test_message.py)."""
    return Message(topic, partition, offset, key, value, None, None, (0, 0), -1.0, -1)


def test_not_a_list_subclass():
    # Composition, not inheritance: it must not be (or pass as) a list.
    assert not isinstance(Messages(), list)
    assert not issubclass(Messages, list)


def test_no_list_mutation_api():
    # A polled batch is read-only; the list mutators shouldn't leak in.
    batch = Messages([_make_message()])
    for attr in ('append', 'extend', 'insert', 'pop', 'clear', 'sort', '__setitem__'):
        assert not hasattr(batch, attr)


def test_sequence_protocol():
    msgs = [_make_message(offset=i) for i in range(3)]
    batch = Messages(msgs)
    assert len(batch) == 3
    assert batch[0] is msgs[0]
    assert list(batch) == msgs
    assert list(iter(batch)) == msgs


def test_slice_returns_messages():
    msgs = [_make_message(offset=i) for i in range(4)]
    tail = Messages(msgs)[1:]
    assert isinstance(tail, Messages)  # slicing keeps the type
    assert list(tail) == msgs[1:]


def test_is_empty_true_when_empty():
    assert Messages().is_empty() is True


def test_is_empty_false_when_populated():
    assert Messages([_make_message()]).is_empty() is False


def test_count_matches_len():
    assert Messages().count() == 0
    batch = Messages([_make_message(offset=i) for i in range(5)])
    assert batch.count() == len(batch) == 5


def test_records_returns_plain_list():
    batch = Messages([_make_message()])
    assert type(batch.records()) is list


def test_records_same_elements_in_order():
    msgs = [_make_message(offset=i) for i in range(4)]
    assert Messages(msgs).records() == msgs


def test_records_returns_independent_copy():
    batch = Messages([_make_message(offset=i) for i in range(3)])
    out = batch.records()
    out.append(_make_message(offset=99))
    assert batch.count() == 3  # mutating the copy leaves the batch alone


def test_records_empty():
    assert Messages().records() == []


def test_from_list_builds_a_batch():
    # Internal zero-copy constructor used by the C poll path.
    src = [_make_message(offset=i) for i in range(3)]
    batch = Messages._from_list(src)
    assert isinstance(batch, Messages)
    assert batch.count() == 3
    assert list(batch) == src


def test_repr():
    assert repr(Messages()) == "Messages([])"


def test_exported_from_package():
    assert "Messages" in confluent_kafka.__all__
    assert confluent_kafka.Messages is Messages
