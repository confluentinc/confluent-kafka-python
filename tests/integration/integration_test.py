#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
#
# Copyright 2016 Confluent Inc.
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


""" Test script for confluent_kafka module """

import confluent_kafka
from confluent_kafka import admin
import os
import time
import uuid
import sys
import json
import gc
import struct

try:
    # Memory tracker
    from pympler import tracker
    with_pympler = True
except Exception:
    with_pympler = False


try:
    from progress.bar import Bar
    with_progress = True
except ImportError:
    with_progress = False

# Default test conf location
testconf_file = "tests/integration/testconf.json"

# Kafka bootstrap server(s)
bootstrap_servers = None

# Confluent schema-registry
schema_registry_url = None

# Topic prefix to use
topic = None

# API version requests are only implemented in Kafka broker >=0.10
# but the client handles failed API version requests gracefully for older
# versions as well, except for 0.9.0.x which will stall for about 10s
# on each connect with this set to True.
api_version_request = True

# global variable to be set by stats_cb call back function
good_stats_cb_result = False

# global counter to be incremented by throttle_cb call back function
throttled_requests = 0

# global variable to track garbage collection of suppressed on_delivery callbacks
DrOnlyTestSuccess_gced = 0

# Shared between producer and consumer tests and used to verify
# that consumed headers are what was actually produced.
produce_headers = [('foo1', 'bar'),
                   ('foo1', 'bar2'),
                   ('foo2', b'1'),
                   (u'Jämtland', u'Härjedalen'),  # automatically utf-8 encoded
                   ('nullheader', None),
                   ('empty', ''),
                   ('foobin', struct.pack('hhl', 10, 20, 30))]

# Identical to produce_headers but with proper binary typing
expected_headers = [('foo1', b'bar'),
                    ('foo1', b'bar2'),
                    ('foo2', b'1'),
                    (u'Jämtland', b'H\xc3\xa4rjedalen'),  # not automatically utf-8 decoded
                    ('nullheader', None),
                    ('empty', b''),
                    ('foobin', struct.pack('hhl', 10, 20, 30))]


def error_cb(err):
    print('Error: %s' % err)


def throttle_cb(throttle_event):
    # validate argument type
    assert isinstance(throttle_event.broker_name, str)
    assert isinstance(throttle_event.broker_id, int)
    assert isinstance(throttle_event.throttle_time, float)

    global throttled_requests
    throttled_requests += 1

    print(throttle_event)


def print_commit_result(err, partitions):
    if err is not None:
        print('# Failed to commit offsets: %s: %s' % (err, partitions))
    else:
        print('# Committed offsets for: %s' % partitions)


def abort_on_missing_configuration(name):
    raise ValueError("{} configuration not provided. Aborting test...".format(name))


class MyTestDr(object):
    """ Producer: Delivery report callback """

    def __init__(self, silent=False):
        super(MyTestDr, self).__init__()
        self.msgs_delivered = 0
        self.bytes_delivered = 0
        self.silent = silent

    @staticmethod
    def _delivery(err, msg, silent=False):
        if err:
            print('Message delivery failed (%s [%s]): %s' %
                  (msg.topic(), str(msg.partition()), err))
            return 0
        else:
            if not silent:
                print('Message delivered to %s [%s] at offset [%s] in %.3fs: %s' %
                      (msg.topic(), msg.partition(), msg.offset(),
                       msg.latency(), msg.value()))
            return 1

    def delivery(self, err, msg):
        if err:
            print('Message delivery failed (%s [%s]): %s' %
                  (msg.topic(), str(msg.partition()), err))
            return
        elif not self.silent:
            print('Message delivered to %s [%s] at offset [%s] in %.3fs: %s' %
                  (msg.topic(), msg.partition(), msg.offset(),
                   msg.latency(), msg.value()))
        self.msgs_delivered += 1
        self.bytes_delivered += len(msg)


def verify_producer():
    """ Verify basic Producer functionality """

    # Producer config
    conf = {'bootstrap.servers': bootstrap_servers,
            'error_cb': error_cb}

    # Create producer
    p = confluent_kafka.Producer(conf)
    print('producer at %s' % p)

    headers = produce_headers

    # Produce some messages
    p.produce(topic, 'Hello Python!', headers=headers)
    p.produce(topic, key='Just a key and headers', headers=headers)
    p.produce(topic, key='Just a key')
    p.produce(topic, partition=1, value='Strictly for partition 1',
              key='mykey', headers=headers)

    # Produce more messages, now with delivery report callbacks in various forms.
    mydr = MyTestDr()
    p.produce(topic, value='This one has a dr callback',
              callback=mydr.delivery)
    p.produce(topic, value='This one has a lambda',
              callback=lambda err, msg: MyTestDr._delivery(err, msg))
    p.produce(topic, value='This one has neither')

    # Try producing with a timestamp
    try:
        p.produce(topic, value='with a timestamp', timestamp=123456789000)
    except NotImplementedError:
        if confluent_kafka.libversion()[1] >= 0x00090400:
            raise

    # Produce even more messages
    for i in range(0, 10):
        p.produce(topic, value='Message #%d' % i, key=str(i),
                  callback=mydr.delivery)
        p.poll(0)

    print('Waiting for %d messages to be delivered' % len(p))

    # Block until all messages are delivered/failed
    p.flush()

    #
    # Additional isolated tests
    #
    test_producer_dr_only_error()


# Global variable to track garbage collection of suppressed on_delivery callbacks
DrOnlyTestSuccess_gced = 0


def test_producer_dr_only_error():
    """
    The C delivery.report.only.error configuration property
    can't be used with the Python client since the Python client
    allocates a msgstate for each produced message that has a callback,
    and on success (with delivery.report.only.error=true) the delivery report
    will not be called and the msgstate will thus never be freed.

    Since a proper broker is required for messages to be succesfully sent
    this test must be run from the integration tests rather than
    the unit tests.
    """
    p = confluent_kafka.Producer({"bootstrap.servers": bootstrap_servers,
                                  'broker.address.family': 'v4',
                                  "delivery.report.only.error": True})

    class DrOnlyTestErr(object):
        def __init__(self):
            self.remaining = 1

        def handle_err(self, err, msg):
            """ This delivery handler should only get called for errored msgs """
            assert "BAD:" in msg.value().decode('utf-8')
            assert err is not None
            self.remaining -= 1

    class DrOnlyTestSuccess(object):
        def handle_success(self, err, msg):
            """ This delivery handler should never get called """
            # FIXME: Can we verify that it is actually garbage collected?
            assert "GOOD:" in msg.value().decode('utf-8')
            assert err is None
            assert False, "should never come here"

        def __del__(self):
            # Indicate that gc has hit this object.
            global DrOnlyTestSuccess_gced
            DrOnlyTestSuccess_gced = 1

    print('only.error: Verifying delivery.report.only.error')

    state = DrOnlyTestErr()
    p.produce(topic, "BAD: This message will make not make it".encode('utf-8'),
              partition=99, on_delivery=state.handle_err)

    not_called_state = DrOnlyTestSuccess()
    p.produce(topic, "GOOD: This message will make make it".encode('utf-8'),
              on_delivery=not_called_state.handle_success)

    # Garbage collection should not kick in yet for not_called_state
    # since there is a on_delivery reference to it.
    not_called_state = None
    gc.collect()
    global DrOnlyTestSuccess_gced
    assert DrOnlyTestSuccess_gced == 0

    print('only.error: Waiting for flush of %d messages' % len(p))
    p.flush(10000)

    print('only.error: Remaining messages now %d' % state.remaining)
    assert state.remaining == 0

    # Now with all messages flushed the reference to not_called_state should be gone.
    gc.collect()
    assert DrOnlyTestSuccess_gced == 1


def verify_producer_performance(with_dr_cb=True):
    """ Time how long it takes to produce and delivery X messages """
    conf = {'bootstrap.servers': bootstrap_servers,
            'linger.ms': 500,
            'error_cb': error_cb}

    p = confluent_kafka.Producer(conf)

    msgcnt = 1000000
    msgsize = 100
    msg_pattern = 'test.py performance'
    msg_payload = (msg_pattern * int(msgsize / len(msg_pattern)))[0:msgsize]

    dr = MyTestDr(silent=True)

    t_produce_start = time.time()
    msgs_produced = 0
    msgs_backpressure = 0
    print('# producing %d messages to topic %s' % (msgcnt, topic))

    if with_progress:
        bar = Bar('Producing', max=msgcnt)
    else:
        bar = None

    for i in range(0, msgcnt):
        while True:
            try:
                if with_dr_cb:
                    p.produce(topic, value=msg_payload, callback=dr.delivery)
                else:
                    p.produce(topic, value=msg_payload)
                break
            except BufferError:
                # Local queue is full (slow broker connection?)
                msgs_backpressure += 1
                if bar is not None and (msgs_backpressure % 1000) == 0:
                    bar.next(n=0)
                p.poll(100)
            continue

        if bar is not None and (msgs_produced % 5000) == 0:
            bar.next(n=5000)
        msgs_produced += 1
        p.poll(0)

    t_produce_spent = time.time() - t_produce_start

    bytecnt = msgs_produced * msgsize

    if bar is not None:
        bar.finish()

    print('# producing %d messages (%.2fMb) took %.3fs: %d msgs/s, %.2f Mb/s' %
          (msgs_produced, bytecnt / (1024*1024), t_produce_spent,
           msgs_produced / t_produce_spent,
           (bytecnt/t_produce_spent) / (1024*1024)))
    print('# %d temporary produce() failures due to backpressure (local queue full)' % msgs_backpressure)

    print('waiting for %d/%d deliveries' % (len(p), msgs_produced))
    # Wait for deliveries
    p.flush()
    t_delivery_spent = time.time() - t_produce_start

    print('# producing %d messages (%.2fMb) took %.3fs: %d msgs/s, %.2f Mb/s' %
          (msgs_produced, bytecnt / (1024*1024), t_produce_spent,
           msgs_produced / t_produce_spent,
           (bytecnt/t_produce_spent) / (1024*1024)))

    # Fake numbers if not using a dr_cb
    if not with_dr_cb:
        print('# not using dr_cb')
        dr.msgs_delivered = msgs_produced
        dr.bytes_delivered = bytecnt

    print('# delivering %d messages (%.2fMb) took %.3fs: %d msgs/s, %.2f Mb/s' %
          (dr.msgs_delivered, dr.bytes_delivered / (1024*1024), t_delivery_spent,
           dr.msgs_delivered / t_delivery_spent,
           (dr.bytes_delivered/t_delivery_spent) / (1024*1024)))
    print('# post-produce delivery wait took %.3fs' %
          (t_delivery_spent - t_produce_spent))


def verify_consumer():
    """ Verify basic Consumer functionality """

    # Consumer config
    conf = {'bootstrap.servers': bootstrap_servers,
            'group.id': 'test.py',
            'session.timeout.ms': 6000,
            'enable.auto.commit': False,
            'on_commit': print_commit_result,
            'error_cb': error_cb,
            'auto.offset.reset': 'earliest',
            'enable.partition.eof': True}

    # Create consumer
    c = confluent_kafka.Consumer(conf)

    def print_wmark(consumer, topic_parts):
        # Verify #294: get_watermark_offsets() should not fail on the first call
        #              This is really a librdkafka issue.
        for p in topic_parts:
            wmarks = consumer.get_watermark_offsets(topic_parts[0])
            print('Watermarks for %s: %s' % (p, wmarks))

    # Subscribe to a list of topics
    c.subscribe([topic], on_assign=print_wmark)

    max_msgcnt = 100
    msgcnt = 0

    first_msg = None

    example_headers = None

    eof_reached = dict()

    while True:
        # Consume until EOF or error

        # Consume message (error()==0) or event (error()!=0)
        msg = c.poll()
        if msg is None:
            raise Exception('Got timeout from poll() without a timeout set: %s' % msg)

        if msg.error():
            if msg.error().code() == confluent_kafka.KafkaError._PARTITION_EOF:
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
            assert offsets[0].offset == msg.offset()+1, \
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
    topic_partions_to_search = list(map(lambda p: confluent_kafka.TopicPartition(topic, p, 123456789001), range(0, 3)))
    print("Searching for offsets with %s" % topic_partions_to_search)

    offsets = c.offsets_for_times(topic_partions_to_search, timeout=1.0)
    print("offsets_for_times results: %s" % offsets)

    verify_consumer_seek(c, first_msg)

    # Close consumer
    c.close()

    # Start a new client and get the committed offsets
    c = confluent_kafka.Consumer(conf)
    offsets = c.committed(list(map(lambda p: confluent_kafka.TopicPartition(topic, p), range(0, 3))))
    for tp in offsets:
        print(tp)

    c.close()


def verify_consumer_performance():
    """ Verify Consumer performance """

    conf = {'bootstrap.servers': bootstrap_servers,
            'group.id': uuid.uuid1(),
            'session.timeout.ms': 6000,
            'error_cb': error_cb,
            'auto.offset.reset': 'earliest'}

    c = confluent_kafka.Consumer(conf)

    def my_on_assign(consumer, partitions):
        print('on_assign:', len(partitions), 'partitions:')
        for p in partitions:
            print(' %s [%d] @ %d' % (p.topic, p.partition, p.offset))
        consumer.assign(partitions)

    def my_on_revoke(consumer, partitions):
        print('on_revoke:', len(partitions), 'partitions:')
        for p in partitions:
            print(' %s [%d] @ %d' % (p.topic, p.partition, p.offset))
        consumer.unassign()

    c.subscribe([topic], on_assign=my_on_assign, on_revoke=my_on_revoke)

    max_msgcnt = 1000000
    bytecnt = 0
    msgcnt = 0

    print('Will now consume %d messages' % max_msgcnt)

    if with_progress:
        bar = Bar('Consuming', max=max_msgcnt,
                  suffix='%(index)d/%(max)d [%(eta_td)s]')
    else:
        bar = None

    while True:
        # Consume until EOF or error

        msg = c.poll(timeout=20.0)
        if msg is None:
            raise Exception('Stalled at %d/%d message, no new messages for 20s' %
                            (msgcnt, max_msgcnt))

        if msg.error():
            raise confluent_kafka.KafkaException(msg.error())

        bytecnt += len(msg)
        msgcnt += 1

        if bar is not None and (msgcnt % 10000) == 0:
            bar.next(n=10000)

        if msgcnt == 1:
            t_first_msg = time.time()
        if msgcnt >= max_msgcnt:
            break

    if bar is not None:
        bar.finish()

    if msgcnt > 0:
        t_spent = time.time() - t_first_msg
        print('%d messages (%.2fMb) consumed in %.3fs: %d msgs/s, %.2f Mb/s' %
              (msgcnt, bytecnt / (1024*1024), t_spent, msgcnt / t_spent,
               (bytecnt / t_spent) / (1024*1024)))

    print('closing consumer')
    c.close()


def verify_consumer_seek(c, seek_to_msg):
    """ Seek to message and verify the next consumed message matches.
        Must only be performed on an actively consuming consumer. """

    tp = confluent_kafka.TopicPartition(seek_to_msg.topic(),
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


def verify_batch_consumer():
    """ Verify basic batch Consumer functionality """

    # Consumer config
    conf = {'bootstrap.servers': bootstrap_servers,
            'group.id': 'test.py',
            'session.timeout.ms': 6000,
            'enable.auto.commit': False,
            'on_commit': print_commit_result,
            'error_cb': error_cb,
            'auto.offset.reset': 'earliest'}

    # Create consumer
    c = confluent_kafka.Consumer(conf)

    # Subscribe to a list of topics
    c.subscribe([topic])

    max_msgcnt = 1000
    batch_cnt = 100
    msgcnt = 0

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
                assert offsets[0].offset == msg.offset()+1, \
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
    c = confluent_kafka.Consumer(conf)
    offsets = c.committed(list(map(lambda p: confluent_kafka.TopicPartition(topic, p), range(0, 3))))
    for tp in offsets:
        print(tp)

    c.close()


def verify_batch_consumer_performance():
    """ Verify batch Consumer performance """

    conf = {'bootstrap.servers': bootstrap_servers,
            'group.id': uuid.uuid1(),
            'session.timeout.ms': 6000,
            'error_cb': error_cb,
            'auto.offset.reset': 'earliest'}

    c = confluent_kafka.Consumer(conf)

    def my_on_assign(consumer, partitions):
        print('on_assign:', len(partitions), 'partitions:')
        for p in partitions:
            print(' %s [%d] @ %d' % (p.topic, p.partition, p.offset))
        consumer.assign(partitions)

    def my_on_revoke(consumer, partitions):
        print('on_revoke:', len(partitions), 'partitions:')
        for p in partitions:
            print(' %s [%d] @ %d' % (p.topic, p.partition, p.offset))
        consumer.unassign()

    c.subscribe([topic], on_assign=my_on_assign, on_revoke=my_on_revoke)

    max_msgcnt = 1000000
    bytecnt = 0
    msgcnt = 0
    batch_size = 1000

    print('Will now consume %d messages' % max_msgcnt)

    if with_progress:
        bar = Bar('Consuming', max=max_msgcnt,
                  suffix='%(index)d/%(max)d [%(eta_td)s]')
    else:
        bar = None

    while msgcnt < max_msgcnt:
        # Consume until we hit max_msgcnt

        msglist = c.consume(num_messages=batch_size, timeout=20.0)

        for msg in msglist:
            if msg.error():
                raise confluent_kafka.KafkaException(msg.error())

            bytecnt += len(msg)
            msgcnt += 1

            if bar is not None and (msgcnt % 10000) == 0:
                bar.next(n=10000)

            if msgcnt == 1:
                t_first_msg = time.time()

    if bar is not None:
        bar.finish()

    if msgcnt > 0:
        t_spent = time.time() - t_first_msg
        print('%d messages (%.2fMb) consumed in %.3fs: %d msgs/s, %.2f Mb/s' %
              (msgcnt, bytecnt / (1024*1024), t_spent, msgcnt / t_spent,
               (bytecnt / t_spent) / (1024*1024)))

    print('closing consumer')
    c.close()


def verify_schema_registry_client():
    from confluent_kafka import avro

    sr_conf = {'url': schema_registry_url}
    sr = avro.CachedSchemaRegistryClient(sr_conf)

    subject = str(uuid.uuid4())

    avsc_dir = os.path.join(os.path.dirname(__file__), os.pardir, 'avro')
    schema = avro.load(os.path.join(avsc_dir, "primitive_float.avsc"))

    schema_id = sr.register(subject, schema)
    assert schema_id == sr.check_registration(subject, schema)
    assert schema == sr.get_by_id(schema_id)
    latest_id, latest_schema, latest_version = sr.get_latest_schema(subject)
    assert schema == latest_schema
    assert sr.get_version(subject, schema) == latest_version
    sr.update_compatibility("FULL", subject)
    assert sr.get_compatibility(subject) == "FULL"
    assert sr.test_compatibility(subject, schema)
    assert sr.delete_subject(subject) == [1]


def verify_avro():
    base_conf = {'bootstrap.servers': bootstrap_servers,
                 'error_cb': error_cb,
                 'api.version.fallback.ms': 0,
                 'broker.version.fallback': '0.11.0.0',
                 'schema.registry.url': schema_registry_url}

    consumer_conf = dict(base_conf, **{
        'group.id': 'test.py',
        'session.timeout.ms': 6000,
        'enable.auto.commit': False,
        'on_commit': print_commit_result,
        'auto.offset.reset': 'earliest'})

    run_avro_loop(base_conf, consumer_conf)


def verify_avro_https(mode_conf):
    if mode_conf is None:
        abort_on_missing_configuration('avro-https')

    base_conf = dict(mode_conf, **{'bootstrap.servers': bootstrap_servers,
                                   'error_cb': error_cb})

    consumer_conf = dict(base_conf, **{'group.id': generate_group_id(),
                                       'session.timeout.ms': 6000,
                                       'enable.auto.commit': False,
                                       'on_commit': print_commit_result,
                                       'auto.offset.reset': 'earliest'})

    run_avro_loop(base_conf, consumer_conf)


def verify_avro_basic_auth(mode_conf):
    if mode_conf is None:
        abort_on_missing_configuration('avro-basic-auth')

    url = {
        'schema.registry.basic.auth.credentials.source': 'URL'
    }

    user_info = {
        'schema.registry.basic.auth.credentials.source': 'USER_INFO',
        'schema.registry.basic.auth.user.info': mode_conf.get('schema.registry.basic.auth.user.info')
    }

    sasl_inherit = {
        'schema.registry.basic.auth.credentials.source': 'sasl_inherit',
        'schema.registry.sasl.username': mode_conf.get('sasl.username'),
        'schema.registry.sasl.password': mode_conf.get('sasl.password')
    }

    base_conf = {
            'bootstrap.servers': bootstrap_servers,
            'error_cb': error_cb,
            'schema.registry.url': schema_registry_url
            }

    consumer_conf = dict({'group.id': generate_group_id(),
                          'session.timeout.ms': 6000,
                          'enable.auto.commit': False,
                          'auto.offset.reset': 'earliest'
                          }, **base_conf)

    print('-' * 10, 'Verifying basic auth source USER_INFO', '-' * 10)
    run_avro_loop(dict(base_conf, **user_info), dict(consumer_conf, **user_info))

    print('-' * 10, 'Verifying basic auth source SASL_INHERIT', '-' * 10)
    run_avro_loop(dict(base_conf, **sasl_inherit), dict(consumer_conf, **sasl_inherit))

    print('-' * 10, 'Verifying basic auth source URL', '-' * 10)
    run_avro_loop(dict(base_conf, **url), dict(consumer_conf, **url))


def run_avro_loop(producer_conf, consumer_conf):
    from confluent_kafka import avro
    avsc_dir = os.path.join(os.path.dirname(__file__), os.pardir, 'avro')

    p = avro.AvroProducer(producer_conf)

    prim_float = avro.load(os.path.join(avsc_dir, "primitive_float.avsc"))
    prim_string = avro.load(os.path.join(avsc_dir, "primitive_string.avsc"))
    basic = avro.load(os.path.join(avsc_dir, "basic_schema.avsc"))
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
        combo['topic'] = str(uuid.uuid4())
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


def verify_throttle_cb():
    """ Verify throttle_cb is invoked
        This test requires client quotas be configured.
        See tests/README.md for more information
    """
    conf = {'bootstrap.servers': bootstrap_servers,
            'linger.ms': 500,
            'client.id': 'throttled_client',
            'throttle_cb': throttle_cb}

    p = confluent_kafka.Producer(conf)

    msgcnt = 1000
    msgsize = 100
    msg_pattern = 'test.py throttled client'
    msg_payload = (msg_pattern * int(msgsize / len(msg_pattern)))[0:msgsize]

    msgs_produced = 0
    msgs_backpressure = 0
    print('# producing %d messages to topic %s' % (msgcnt, topic))

    if with_progress:
        bar = Bar('Producing', max=msgcnt)
    else:
        bar = None

    for i in range(0, msgcnt):
        while True:
            try:
                p.produce(topic, value=msg_payload)
                break
            except BufferError:
                # Local queue is full (slow broker connection?)
                msgs_backpressure += 1
                if bar is not None and (msgs_backpressure % 1000) == 0:
                    bar.next(n=0)
                p.poll(100)
            continue

        if bar is not None and (msgs_produced % 5000) == 0:
            bar.next(n=5000)
        msgs_produced += 1
        p.poll(0)

    if bar is not None:
        bar.finish()

    p.flush()
    print('# {} of {} produce requests were throttled'.format(throttled_requests, msgs_produced))
    assert throttled_requests >= 1


def verify_stats_cb():
    """ Verify stats_cb """

    def stats_cb(stats_json_str):
        global good_stats_cb_result
        stats_json = json.loads(stats_json_str)
        if topic in stats_json['topics']:
            app_offset = stats_json['topics'][topic]['partitions']['0']['app_offset']
            if app_offset > 0:
                print("# app_offset stats for topic %s partition 0: %d" %
                      (topic, app_offset))
                good_stats_cb_result = True

    conf = {'bootstrap.servers': bootstrap_servers,
            'group.id': uuid.uuid1(),
            'session.timeout.ms': 6000,
            'error_cb': error_cb,
            'stats_cb': stats_cb,
            'statistics.interval.ms': 200,
            'auto.offset.reset': 'earliest'}

    c = confluent_kafka.Consumer(conf)
    c.subscribe([topic])

    max_msgcnt = 1000000
    bytecnt = 0
    msgcnt = 0

    print('Will now consume %d messages' % max_msgcnt)

    if with_progress:
        bar = Bar('Consuming', max=max_msgcnt,
                  suffix='%(index)d/%(max)d [%(eta_td)s]')
    else:
        bar = None

    while not good_stats_cb_result:
        # Consume until EOF or error

        msg = c.poll(timeout=20.0)
        if msg is None:
            raise Exception('Stalled at %d/%d message, no new messages for 20s' %
                            (msgcnt, max_msgcnt))

        if msg.error():
            raise confluent_kafka.KafkaException(msg.error())

        bytecnt += len(msg)
        msgcnt += 1

        if bar is not None and (msgcnt % 10000) == 0:
            bar.next(n=10000)

        if msgcnt == 1:
            t_first_msg = time.time()
        if msgcnt >= max_msgcnt:
            break

    if bar is not None:
        bar.finish()

    if msgcnt > 0:
        t_spent = time.time() - t_first_msg
        print('%d messages (%.2fMb) consumed in %.3fs: %d msgs/s, %.2f Mb/s' %
              (msgcnt, bytecnt / (1024*1024), t_spent, msgcnt / t_spent,
               (bytecnt / t_spent) / (1024*1024)))

    print('closing consumer')
    c.close()


def verify_topic_metadata(client, exp_topics):
    """
    Verify that exp_topics (dict<topicname,partcnt>) is reported in metadata.
    Will retry and wait for some time to let changes propagate.

    Non-controller brokers may return the previous partition count for some
    time before being updated, in this case simply retry.
    """

    for retry in range(0, 3):
        do_retry = 0

        md = client.list_topics()

        for exptopic, exppartcnt in exp_topics.items():
            if exptopic not in md.topics:
                print("Topic {} not yet reported in metadata: retrying".format(exptopic))
                do_retry += 1
                continue

            if len(md.topics[exptopic].partitions) < exppartcnt:
                print("Topic {} partition count not yet updated ({} != expected {}): retrying".format(
                    exptopic, len(md.topics[exptopic].partitions), exppartcnt))
                do_retry += 1
                continue

            assert len(md.topics[exptopic].partitions) == exppartcnt, \
                "Expected {} partitions for topic {}, not {}".format(
                    exppartcnt, exptopic, md.topics[exptopic].partitions)

        if do_retry == 0:
            return  # All topics okay.

        time.sleep(1)

    raise Exception("Timed out waiting for topics {} in metadata".format(exp_topics))


def verify_admin():
    """ Verify Admin API """

    a = admin.AdminClient({'bootstrap.servers': bootstrap_servers})
    our_topic = topic + '_admin_' + str(uuid.uuid4())
    num_partitions = 2

    topic_config = {"compression.type": "gzip"}

    #
    # First iteration: validate our_topic creation.
    # Second iteration: create topic.
    #
    for validate in (True, False):
        fs = a.create_topics([admin.NewTopic(our_topic,
                                             num_partitions=num_partitions,
                                             config=topic_config,
                                             replication_factor=1)],
                             validate_only=validate,
                             operation_timeout=10.0)

        for topic2, f in fs.items():
            f.result()  # trigger exception if there was an error

    #
    # Find the topic in list_topics
    #
    verify_topic_metadata(a, {our_topic: num_partitions})

    #
    # Increase the partition count
    #
    num_partitions += 3
    fs = a.create_partitions([admin.NewPartitions(our_topic,
                                                  new_total_count=num_partitions)],
                             operation_timeout=10.0)

    for topic2, f in fs.items():
        f.result()  # trigger exception if there was an error

    #
    # Verify with list_topics.
    #
    verify_topic_metadata(a, {our_topic: num_partitions})

    #
    # Verify with list_groups.
    #

    # Produce some messages
    p = confluent_kafka.Producer({"bootstrap.servers": bootstrap_servers})
    p.produce(our_topic, 'Hello Python!', headers=produce_headers)
    p.produce(our_topic, key='Just a key and headers', headers=produce_headers)

    def consume_messages(group_id):
        # Consume messages
        conf = {'bootstrap.servers': bootstrap_servers,
                'group.id': group_id,
                'session.timeout.ms': 6000,
                'enable.auto.commit': False,
                'on_commit': print_commit_result,
                'error_cb': error_cb,
                'auto.offset.reset': 'earliest',
                'enable.partition.eof': True}
        c = confluent_kafka.Consumer(conf)
        c.subscribe([our_topic])
        eof_reached = dict()
        while True:
            msg = c.poll()
            if msg is None:
                raise Exception('Got timeout from poll() without a timeout set: %s' % msg)

            if msg.error():
                if msg.error().code() == confluent_kafka.KafkaError._PARTITION_EOF:
                    print('Reached end of %s [%d] at offset %d' % (
                          msg.topic(), msg.partition(), msg.offset()))
                    eof_reached[(msg.topic(), msg.partition())] = True
                    if len(eof_reached) == len(c.assignment()):
                        print('EOF reached for all assigned partitions: exiting')
                        break
                else:
                    print('Consumer error: %s: ignoring' % msg.error())
                    break
            # Commit offset
            c.commit(msg, asynchronous=False)

    group1 = 'test-group-1'
    group2 = 'test-group-2'
    consume_messages(group1)
    consume_messages(group2)
    # list_groups without group argument
    groups = set(group.id for group in a.list_groups(timeout=10))
    assert group1 in groups, "Consumer group {} not found".format(group1)
    assert group2 in groups, "Consumer group {} not found".format(group2)
    # list_groups with group argument
    groups = set(group.id for group in a.list_groups(group1))
    assert group1 in groups, "Consumer group {} not found".format(group1)
    groups = set(group.id for group in a.list_groups(group2))
    assert group2 in groups, "Consumer group {} not found".format(group2)

    def verify_config(expconfig, configs):
        """
        Verify that the config key,values in expconfig are found
        and matches the ConfigEntry in configs.
        """
        for key, expvalue in expconfig.items():
            entry = configs.get(key, None)
            assert entry is not None, "Config {} not found in returned configs".format(key)

            assert entry.value == str(expvalue), \
                "Config {} with value {} does not match expected value {}".format(key, entry, expvalue)

    #
    # Get current topic config
    #
    resource = admin.ConfigResource(admin.RESOURCE_TOPIC, our_topic)
    fs = a.describe_configs([resource])
    configs = fs[resource].result()  # will raise exception on failure

    # Verify config matches our expectations
    verify_config(topic_config, configs)

    #
    # Now change the config.
    #
    topic_config["file.delete.delay.ms"] = 12345
    topic_config["compression.type"] = "snappy"

    for key, value in topic_config.items():
        resource.set_config(key, value)

    fs = a.alter_configs([resource])
    fs[resource].result()  # will raise exception on failure

    #
    # Read the config back again and verify.
    #
    fs = a.describe_configs([resource])
    configs = fs[resource].result()  # will raise exception on failure

    # Verify config matches our expectations
    verify_config(topic_config, configs)

    #
    # Delete the topic
    #
    fs = a.delete_topics([our_topic])
    fs[our_topic].result()  # will raise exception on failure
    print("Topic {} marked for deletion".format(our_topic))


def verify_avro_explicit_read_schema():
    from confluent_kafka import avro

    """ verify that reading Avro with explicit reader schema works"""
    base_conf = {'bootstrap.servers': bootstrap_servers,
                 'error_cb': error_cb,
                 'schema.registry.url': schema_registry_url}

    consumer_conf = dict(base_conf, **{
        'group.id': 'test.py',
        'session.timeout.ms': 6000,
        'enable.auto.commit': False,
        'on_commit': print_commit_result,
        'auto.offset.reset': 'earliest',
        'schema.registry.url': schema_registry_url})

    avsc_dir = os.path.join(os.path.dirname(__file__), os.pardir, 'avro')
    writer_schema = avro.load(os.path.join(avsc_dir, "user_v1.avsc"))
    reader_schema = avro.load(os.path.join(avsc_dir, "user_v2.avsc"))

    user_value1 = {
        "name": " Rogers Nelson"
    }

    user_value2 = {
        "name": "Kenny Loggins"
    }

    combinations = [
        dict(key=user_value1, key_schema=writer_schema, value=user_value2, value_schema=writer_schema),
        dict(key=user_value2, key_schema=writer_schema, value=user_value1, value_schema=writer_schema)
    ]
    avro_topic = topic + str(uuid.uuid4())

    p = avro.AvroProducer(base_conf)
    for i, combo in enumerate(combinations):
        p.produce(topic=avro_topic, **combo)
    p.flush()

    c = avro.AvroConsumer(consumer_conf, reader_key_schema=reader_schema, reader_value_schema=reader_schema)
    c.subscribe([avro_topic])

    msgcount = 0
    while msgcount < len(combinations):
        msg = c.poll(1)

        if msg is None:
            continue
        if msg.error():
            print("Consumer error {}".format(msg.error()))
            continue

        msgcount += 1
        # Avro schema projection should return the two fields not present in the writer schema
        try:
            assert(msg.key().get('favorite_number') == 42)
            assert(msg.key().get('favorite_color') == "purple")
            assert(msg.value().get('favorite_number') == 42)
            assert(msg.value().get('favorite_color') == "purple")
            print("success: schema projection worked for explicit reader schema")
        except KeyError:
            raise confluent_kafka.avro.SerializerError("Schema projection failed when setting reader schema.")


def generate_group_id():
    return str(uuid.uuid1())


def resolve_envs(_conf):
    """Resolve environment variables"""

    for k, v in _conf.items():
        if isinstance(v, dict):
            resolve_envs(v)

        if str(v).startswith('$'):
            _conf[k] = os.getenv(v[1:])


test_modes = ['consumer', 'producer', 'avro', 'performance', 'admin', 'avro-https', 'avro-basic-auth', 'throttle']


def print_usage(exitcode, reason=None):
    """ Print usage and exit with exitcode """
    if reason is not None:
        print('Error: %s' % reason)
    print('Usage: %s [options] <testconf>' % sys.argv[0])
    print('Options:')
    print(' %s - limit to matching tests' % ', '.join(['--' + x for x in test_modes]))

    sys.exit(exitcode)


if __name__ == '__main__':
    """Run test suites"""

    if with_pympler:
        tr = tracker.SummaryTracker()
        print("Running with pympler memory tracker")

    modes = list()

    # Parse options
    while len(sys.argv) > 1 and sys.argv[1].startswith('--'):
        opt = sys.argv.pop(1)[2:]

        if opt not in test_modes:
            print_usage(1, 'unknown option --' + opt)
        modes.append(opt)

    if len(sys.argv) == 2:
        testconf_file = sys.argv.pop(1)

    with open(testconf_file) as f:
        testconf = json.load(f)
        resolve_envs(testconf)

    bootstrap_servers = testconf.get('bootstrap.servers')
    topic = testconf.get('topic')
    schema_registry_url = testconf.get('schema.registry.url')

    if len(modes) == 0:
        modes = test_modes

    if bootstrap_servers is None or topic is None:
        print_usage(1, "Missing required property bootstrap.servers")

    if topic is None:
        print_usage(1, "Missing required property topic")

    print('Using confluent_kafka module version %s (0x%x)' % confluent_kafka.version())
    print('Using librdkafka version %s (0x%x)' % confluent_kafka.libversion())
    print('Testing: %s' % modes)
    print('Brokers: %s' % bootstrap_servers)
    print('Topic prefix: %s' % topic)

    if 'producer' in modes:
        print('=' * 30, 'Verifying Producer', '=' * 30)
        verify_producer()

        if 'performance' in modes:
            print('=' * 30, 'Verifying Producer performance (with dr_cb)', '=' * 30)
            verify_producer_performance(with_dr_cb=True)

        if 'performance' in modes:
            print('=' * 30, 'Verifying Producer performance (without dr_cb)', '=' * 30)
            verify_producer_performance(with_dr_cb=False)

    if 'consumer' in modes:
        print('=' * 30, 'Verifying Consumer', '=' * 30)
        verify_consumer()

        print('=' * 30, 'Verifying batch Consumer', '=' * 30)
        verify_batch_consumer()

        if 'performance' in modes:
            print('=' * 30, 'Verifying Consumer performance', '=' * 30)
            verify_consumer_performance()

            print('=' * 30, 'Verifying batch Consumer performance', '=' * 30)
            verify_batch_consumer_performance()

        # The stats test is utilizing the consumer.
        print('=' * 30, 'Verifying stats_cb', '=' * 30)
        verify_stats_cb()

    # The throttle test is utilizing the producer.
    if 'throttle' in modes:
        print('=' * 30, 'Verifying throttle_cb', '=' * 30)
        verify_throttle_cb()

    if 'avro' in modes:
        print('=' * 30, 'Verifying Schema Registry Client', '=' * 30)
        verify_schema_registry_client()
        print('=' * 30, 'Verifying AVRO', '=' * 30)
        verify_avro()

        print('=' * 30, 'Verifying AVRO with explicit reader schema', '=' * 30)
        verify_avro_explicit_read_schema()

    if 'avro-https' in modes:
        print('=' * 30, 'Verifying AVRO with HTTPS', '=' * 30)
        verify_avro_https(testconf.get('avro-https', None))

    if 'avro-basic-auth' in modes:
        print("=" * 30, 'Verifying AVRO with Basic Auth', '=' * 30)
        verify_avro_basic_auth(testconf.get('avro-basic-auth', None))

    if 'admin' in modes:
        print('=' * 30, 'Verifying Admin API', '=' * 30)
        verify_admin()

    print('=' * 30, 'Done', '=' * 30)

    if with_pympler:
        gc.collect()
        print('Memory tracker results')
        tr.print_diff()
