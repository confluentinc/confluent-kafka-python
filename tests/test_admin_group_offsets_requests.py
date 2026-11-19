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
Input checks of the C list_consumer_group_offsets() and
alter_consumer_group_offsets().

AdminClient validates the request before passing it to the C methods, so most
of these tests call the C methods directly. Its checks accept subclasses of
list and TopicPartition, though, and the C methods take only those exact
types, so the subclass tests go through AdminClient. The C methods used to
ignore the result of reading the topic_partitions attribute and of converting
the partitions: a missing or invalid one crashed them or left an exception set
while they carried on. Each case runs in a subprocess so that a crash fails
only that case.
"""

import subprocess
import sys

import pytest

_SETUP = """
import concurrent.futures

from confluent_kafka import ConsumerGroupTopicPartitions, TopicPartition
from confluent_kafka.admin import AdminClient

request = ConsumerGroupTopicPartitions("group", [TopicPartition("topic", 0, 5)])


class Unclassifiable:
    group_id = "group"
    topic_partitions = None

    @property
    def __class__(self):
        raise RuntimeError("no class")
"""

_CALL = """
admin = AdminClient({"socket.timeout.ms": 10})
try:
    getattr(super(AdminClient, admin), method)([request], concurrent.futures.Future())
except Exception as e:
    print(f"{type(e).__name__}: {e}")
else:
    print("accepted")
"""

CASES = {
    'valid': ("pass", "accepted", "accepted"),
    'no-topic_partitions': (
        "del request.topic_partitions",
        "TypeError: Required attribute .topic_partitions missing",
        "TypeError: Required attribute .topic_partitions missing",
    ),
    'tuple-topic_partitions': (
        "request.topic_partitions = tuple(request.topic_partitions)",
        "TypeError: Expected .topic_partitions to be list type, not tuple",
        "TypeError: Expected .topic_partitions to be list type, not tuple",
    ),
    'str-partition': (
        "request.topic_partitions = ['topic']",
        "TypeError: expected cimpl.TopicPartition",
        "TypeError: expected cimpl.TopicPartition",
    ),
    'none-topic_partitions': (
        "request.topic_partitions = None",
        "accepted",
        "ValueError: topic_partitions is mandatory for alter consumer offset operation",
    ),
    'isinstance-error': ("request = Unclassifiable()", "RuntimeError: no class", "RuntimeError: no class"),
}


_ADMIN_CLIENT_SETUP = """
from confluent_kafka import OFFSET_INVALID, ConsumerGroupTopicPartitions, TopicPartition
from confluent_kafka.admin import AdminClient


class PartitionList(list):
    pass


class Partition(TopicPartition):
    pass


# AdminClient takes the partitions without an offset to list offsets, and with
# one to alter them.
offset = OFFSET_INVALID if method == 'list_consumer_group_offsets' else 5
"""

_ADMIN_CLIENT_CALL = """
admin = AdminClient({"socket.timeout.ms": 10})
try:
    getattr(admin, method)([ConsumerGroupTopicPartitions("group", partitions)])
except Exception as e:
    print(f"{type(e).__name__}: {e}")
else:
    print("accepted")
"""

# AdminClient's checks accept these subclasses; the C methods reject them.
SUBCLASS_CASES = {
    'list-subclass': (
        "partitions = PartitionList([TopicPartition('topic', 0, offset)])",
        "TypeError: Expected .topic_partitions to be list type, not PartitionList",
    ),
    'topic-partition-subclass': (
        "partitions = [Partition('topic', 0, offset)]",
        "TypeError: expected cimpl.TopicPartition",
    ),
}


def _run(code):
    """Run code in a new interpreter and return what it printed."""
    result = subprocess.run([sys.executable, '-c', code], capture_output=True, text=True, timeout=60)
    assert result.returncode == 0, f'returncode={result.returncode}\nstderr:\n{result.stderr}'
    return result.stdout.strip()


@pytest.mark.parametrize('case', sorted(CASES))
@pytest.mark.parametrize('method', ['list_consumer_group_offsets', 'alter_consumer_group_offsets'])
def test_group_offsets_request_checks(method, case):
    statement, expected_list, expected_alter = CASES[case]
    expected = expected_list if method == 'list_consumer_group_offsets' else expected_alter
    assert _run(_SETUP + f"method = {method!r}\n" + statement + "\n" + _CALL) == expected


@pytest.mark.parametrize('case', sorted(SUBCLASS_CASES))
@pytest.mark.parametrize('method', ['list_consumer_group_offsets', 'alter_consumer_group_offsets'])
def test_group_offsets_request_subclasses_through_admin_client(method, case):
    """
    A list or TopicPartition subclass passes AdminClient's checks, and the C
    methods raise TypeError for it. They used to crash the process, or for a
    TopicPartition subclass given to list_consumer_group_offsets(), raise a
    SystemError.
    """
    statement, expected = SUBCLASS_CASES[case]
    code = f"method = {method!r}\n" + _ADMIN_CLIENT_SETUP + statement + "\n" + _ADMIN_CLIENT_CALL
    assert _run(code) == expected
