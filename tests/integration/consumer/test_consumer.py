#!/usr/bin/env python
# -*- coding: utf-8 -*-
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

from uuid import uuid1


from confluent_kafka import Consumer, KafkaError, TopicPartition


def test_consumer(kafka_cluster_fixture, cluster_producer_fixture, topic_fixture,
                  produce_headers_fixture, expected_headers_fixture,
                  print_commit_callback_fixture, error_cb_fixture):
    """ Verify basic Consumer functionality """

    # Create consumer
    conf = kafka_cluster_fixture.client_conf()

    conf.update({
        'group.id': 'test.py',
        'session.timeout.ms': 6000,
        'enable.auto.commit': False,
        'on_commit': print_commit_callback_fixture,
        'error_cb': error_cb_fixture,
        'auto.offset.reset': 'earliest',
        'enable.partition.eof': True}
    )

    c = Consumer(conf)
    expected_headers = expected_headers_fixture

    def print_wmark(consumer, topic_parts):
        # Verify #294: get_watermark_offsets() should not fail on the first call
        #              This is really a librdkafka issue.
        for p in topic_parts:
            wmarks = consumer.get_watermark_offsets(topic_parts[0])
            print('Watermarks for %s: %s' % (p, wmarks))

    # Subscribe to a list of topics
    c.subscribe([topic_fixture], on_assign=print_wmark)

    msgcnt = 0

    first_msg = None

    example_headers = None

    eof_reached = dict()

    max_msgcnt = 100
    cluster_producer_fixture(max_msgcnt, produce_headers_fixture)
    while True:
        # Consume until EOF or error

        # Consume message (error()==0) or event (error()!=0)
        msg = c.poll()
        if msg is None:
            raise Exception('Got timeout from poll() without a timeout set: %s' % msg)

        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                print('Reached end of %s [%d] at offset %d' %
                      (msg.topic(), msg.partition(), msg.offset()))
                eof_reached[(msg.topic(), msg.partition())] = True
                if len(eof_reached) == len(c.assignment()):
                    print('EOF reached for all assigned partitions: exiting')
                    break
            else:
                print('Consumer error: %s: ignoring' % msg.error())
                break

        tstype, timestamp = msg.timestamp()
        headers = msg.headers()
        if headers:
            example_headers = headers

        msg.set_headers([('foo', 'bar')])
        assert msg.headers() == [('foo', 'bar')]

        print('%s[%d]@%d: key=%s, value=%s, tstype=%d, timestamp=%s headers=%s' %
              (msg.topic(), msg.partition(), msg.offset(),
               msg.key(), msg.value(), tstype, timestamp, headers))

        if first_msg is None:
            first_msg = msg

        if (msgcnt == 11):
            parts = c.assignment()
            print('Pausing partitions briefly')
            c.pause(parts)
            exp_None = c.poll(timeout=2.0)
            assert exp_None is None, "expected no messages during pause, got %s" % exp_None
            print('Resuming partitions')
            c.resume(parts)

        if (msg.offset() % 5) == 0:
            # Async commit
            c.commit(msg, asynchronous=True)
        elif (msg.offset() % 4) == 0:
            offsets = c.commit(msg, asynchronous=False)
            assert len(offsets) == 1, 'expected 1 offset, not %s' % (offsets)
            assert offsets[0].offset == msg.offset() + 1, \
                'expected offset %d to be committed, not %s' % \
                (msg.offset(), offsets)
            print('Sync committed offset: %s' % offsets)

        msgcnt += 1
        if msgcnt >= max_msgcnt and example_headers is not None:
            print('max_msgcnt %d reached' % msgcnt)
            break

    assert example_headers, "We should have received at least one header"
    assert example_headers == expected_headers, \
        "example header mismatch:\n{}\nexpected:\n{}".format(example_headers, expected_headers)

    # Get current assignment
    assignment = c.assignment()

    # Get cached watermark offsets
    # Since we're not making use of statistics the low offset is not known so ignore it.
    lo, hi = c.get_watermark_offsets(assignment[0], cached=True)
    print('Cached offsets for %s: %d - %d' % (assignment[0], lo, hi))

    # Query broker for offsets
    lo, hi = c.get_watermark_offsets(assignment[0], timeout=1.0)
    print('Queried offsets for %s: %d - %d' % (assignment[0], lo, hi))

    # Query offsets for timestamps by setting the topic partition offset to a timestamp. 123456789000 + 1
    topic_partions_to_search = list(map(lambda p: TopicPartition(topic_fixture, p, 123456789001), range(0, 3)))
    print("Searching for offsets with %s" % topic_partions_to_search)

    offsets = c.offsets_for_times(topic_partions_to_search, timeout=1.0)
    print("offsets_for_times results: %s" % offsets)

    verify_consumer_seek(c, first_msg)

    # Close consumer
    c.close()

    # Start a new client and get the committed offsets
    c = Consumer(conf)
    offsets = c.committed(list(map(lambda p: TopicPartition(topic_fixture, p), range(0, 3))))
    for tp in offsets:
        print(tp)

    c.close()


def verify_consumer_seek(c, seek_to_msg):
    """ Seek to message and verify the next consumed message matches.
        Must only be performed on an actively consuming consumer. """

    tp = TopicPartition(seek_to_msg.topic(),
                        seek_to_msg.partition(),
                        seek_to_msg.offset())

    print('seek: Seeking to %s' % tp)
    c.seek(tp)

    while True:
        msg = c.poll()
        assert msg is not None
        if msg.error():
            print('seek: Ignoring non-message: %s' % msg.error())
            continue

        if msg.topic() != seek_to_msg.topic() or msg.partition() != seek_to_msg.partition():
            continue

        print('seek: message at offset %d' % msg.offset())
        assert msg.offset() == seek_to_msg.offset(), \
            'expected message at offset %d, not %d' % (seek_to_msg.offset(), msg.offset())
        break


def test_batch_consumer(kafka_cluster_fixture, topic_fixture, cluster_producer_fixture, error_cb_fixture):
    """ Verify basic batch Consumer functionality """

    # Consumer config
    conf = kafka_cluster_fixture.client_conf()

    conf.update({
        'group.id': uuid1(),
        'session.timeout.ms': 6000,
        'error_cb': error_cb_fixture,
        'auto.offset.reset': 'earliest'})

    # Create consumer
    c = Consumer(conf)

    # Subscribe to a list of topics
    c.subscribe([topic_fixture])

    max_msgcnt = 1000
    batch_cnt = 100
    msgcnt = 0

    cluster_producer_fixture(max_msgcnt)
    while msgcnt < max_msgcnt:
        # Consume until we hit max_msgcnt

        # Consume messages (error()==0) or event (error()!=0)
        msglist = c.consume(batch_cnt, 10.0)
        assert len(msglist) == batch_cnt, 'expected %d messages, not %d' % (batch_cnt, len(msglist))

        for msg in msglist:
            if msg.error():
                print('Consumer error: %s: ignoring' % msg.error())
                continue

            tstype, timestamp = msg.timestamp()
            print('%s[%d]@%d: key=%s, value=%s, tstype=%d, timestamp=%s' %
                  (msg.topic(), msg.partition(), msg.offset(),
                   msg.key(), msg.value(), tstype, timestamp))

            if (msg.offset() % 5) == 0:
                # Async commit
                c.commit(msg, asynchronous=True)
            elif (msg.offset() % 4) == 0:
                offsets = c.commit(msg, asynchronous=False)
                assert len(offsets) == 1, 'expected 1 offset, not %s' % (offsets)
                assert offsets[0].offset == msg.offset() + 1, \
                    'expected offset %d to be committed, not %s' % \
                    (msg.offset(), offsets)
                print('Sync committed offset: %s' % offsets)

            msgcnt += 1

    print('max_msgcnt %d reached' % msgcnt)

    # Get current assignment
    assignment = c.assignment()

    # Get cached watermark offsets
    # Since we're not making use of statistics the low offset is not known so ignore it.
    lo, hi = c.get_watermark_offsets(assignment[0], cached=True)
    print('Cached offsets for %s: %d - %d' % (assignment[0], lo, hi))

    # Query broker for offsets
    lo, hi = c.get_watermark_offsets(assignment[0], timeout=1.0)
    print('Queried offsets for %s: %d - %d' % (assignment[0], lo, hi))

    # Close consumer
    c.close()

    # Start a new client and get the committed offsets
    c = Consumer(conf)
    offsets = c.committed(list(map(lambda p: TopicPartition(topic_fixture, p), range(0, 3))))
    for tp in offsets:
        print(tp)

    c.close()
