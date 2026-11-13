#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright 2022 Confluent Inc.
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

import socket
import struct

from confluent_kafka import TopicPartition

_NOT_COORDINATOR = 16


def commit_and_check(consumer, topic, metadata):
    if metadata is None:
        consumer.commit(offsets=[TopicPartition(topic, 0, 1)], asynchronous=False)
    else:
        consumer.commit(offsets=[TopicPartition(topic, 0, 1, metadata)], asynchronous=False)

    offsets = consumer.committed([TopicPartition(topic, 0)], timeout=100)
    assert len(offsets) == 1
    assert offsets[0].metadata == metadata


def test_consumer_topicpartition_metadata(kafka_cluster):
    topic = kafka_cluster.create_topic_and_wait_propogation("test_topicpartition")
    consumer_conf = {'group.id': 'test_topicpartition'}

    c = kafka_cluster.consumer(consumer_conf)

    # Commit without any metadata.
    metadata = None
    commit_and_check(c, topic, metadata)

    # Commit with only ASCII metadata.
    metadata = 'hello world'
    commit_and_check(c, topic, metadata)

    # Commit with Unicode characters in metadata.
    metadata = 'नमस्ते दुनिया'
    commit_and_check(c, topic, metadata)

    # Commit with empty string as metadata.
    metadata = ''
    commit_and_check(c, topic, metadata)

    # Commit with invalid metadata (with null byte in the middle).
    metadata = 'xyz\x00abc'
    try:
        commit_and_check(c, topic, metadata)
        # We should never reach this point, since the prior statement should throw.
        assert False
    except ValueError as ve:
        assert 'embedded null character' in str(ve)

    c.close()


def _kafka_string(value):
    encoded = value.encode()
    return struct.pack('>h', len(encoded)) + encoded


def _recv_exact(sock, size):
    data = b''
    while len(data) < size:
        chunk = sock.recv(size - len(data))
        if not chunk:
            raise ConnectionError('broker closed the connection')
        data += chunk
    return data


def _offset_commit_v2(host, port, group, topic, partitions, offset, metadata):
    """
    Commit offsets with a hand-built OffsetCommit v2 request.

    confluent-kafka always commits metadata with its NUL terminator, so this
    is the simplest way to store metadata of its exact length, as kafka-python
    and the Java client do.

    :returns: The set of per-partition error codes.
    """
    request = struct.pack('>hhi', 8, 2, 1) + _kafka_string('test-raw-commit')
    request += _kafka_string(group) + struct.pack('>i', -1) + _kafka_string('') + struct.pack('>q', -1)
    request += struct.pack('>i', 1) + _kafka_string(topic) + struct.pack('>i', len(partitions))
    for partition in partitions:
        request += struct.pack('>iqh', partition, offset, len(metadata)) + metadata

    with socket.create_connection((host, port), timeout=10) as sock:
        sock.sendall(struct.pack('>i', len(request)) + request)
        (size,) = struct.unpack('>i', _recv_exact(sock, 4))
        response = _recv_exact(sock, size)

    # Skip the correlation id, then read topics[].partitions[].error_code.
    position = 4
    errors = set()
    (topic_count,) = struct.unpack_from('>i', response, position)
    position += 4
    for _ in range(topic_count):
        (name_length,) = struct.unpack_from('>h', response, position)
        position += 2 + name_length
        (partition_count,) = struct.unpack_from('>i', response, position)
        position += 4
        for _ in range(partition_count):
            _, error = struct.unpack_from('>ih', response, position)
            position += 6
            errors.add(error)
    return errors


def _commit_to_coordinator(kafka_cluster, group, topic, partitions, offset, metadata):
    brokers = kafka_cluster.admin().list_topics(timeout=10).brokers.values()
    for broker in brokers:
        errors = _offset_commit_v2(broker.host, broker.port, group, topic, partitions, offset, metadata)
        if errors != {_NOT_COORDINATOR}:
            return errors
    raise AssertionError('no broker accepted the commit as group coordinator')


def test_consumer_committed_metadata_without_nul_terminator_returns_committed_bytes(kafka_cluster):
    # 248 bytes: glibc rounds this allocation to a chunk whose usable size ends
    # exactly at the next chunk's header, so an over-read past the buffer finds
    # non-zero bytes and shows up deterministically instead of by chance.
    metadata = 'm' * 248
    partitions = list(range(8))
    topic = kafka_cluster.create_topic_and_wait_propogation(
        "test_topicpartition_unterminated", {'num_partitions': len(partitions)}
    )
    group = 'test_topicpartition_unterminated'

    errors = _commit_to_coordinator(kafka_cluster, group, topic, partitions, 1, metadata.encode())
    assert errors == {0}

    c = kafka_cluster.consumer({'group.id': group})
    offsets = c.committed([TopicPartition(topic, p) for p in partitions], timeout=100)
    c.close()

    assert [tp.metadata for tp in offsets] == [metadata] * len(partitions)
