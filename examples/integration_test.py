#!/usr/bin/env python
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
import os
import time
import uuid
import sys
import json
import gc
from copy import copy

try:
    from progress.bar import Bar
    with_progress = True
except ImportError as e:
    with_progress = False

# Kafka bootstrap server(s)
bootstrap_servers = None

# Confluent schema-registry
schema_registry_url = None

# Topic to use
topic = 'test'

# API version requests are only implemented in Kafka broker >=0.10
# but the client handles failed API version requests gracefully for older
# versions as well, except for 0.9.0.x which will stall for about 10s
# on each connect with this set to True.
api_version_request = True

# global variable to be set by stats_cb call back function
good_stats_cb_result = False


def error_cb(err):
    print('Error: %s' % err)


class InMemorySchemaRegistry(object):

    schemas = {}
    next_idx = 0

    def register(self, subject, schema):
        self.next_idx += 1
        self.schemas[self.next_idx] = schema
        return self.next_idx

    def get_by_id(self, idx):
        return self.schemas.get(idx)


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
                print('Message delivered to %s [%s] at offset [%s]: %s' %
                      (msg.topic(), msg.partition(), msg.offset(), msg.value()))
            return 1

    def delivery(self, err, msg):
        if err:
            print('Message delivery failed (%s [%s]): %s' %
                  (msg.topic(), str(msg.partition()), err))
            return
        elif not self.silent:
            print('Message delivered to %s [%s] at offset [%s]: %s' %
                  (msg.topic(), msg.partition(), msg.offset(), msg.value()))
        self.msgs_delivered += 1
        self.bytes_delivered += len(msg)


def verify_producer():
    """ Verify basic Producer functionality """

    # Producer config
    conf = {'bootstrap.servers': bootstrap_servers,
            'error_cb': error_cb,
            'api.version.request': api_version_request,
            'default.topic.config': {'produce.offset.report': True}}

    # Create producer
    p = confluent_kafka.Producer(**conf)
    print('producer at %s' % p)

    # Produce some messages
    p.produce(topic, 'Hello Python!')
    p.produce(topic, key='Just a key')
    p.produce(topic, partition=1, value='Strictly for partition 1',
              key='mykey')

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


def verify_avro():
    from confluent_kafka import avro
    avsc_dir = os.path.join(os.path.dirname(__file__), os.pardir, 'tests', 'avro')

    # Producer config
    conf = {'bootstrap.servers': bootstrap_servers,
            'error_cb': error_cb,
            'api.version.request': api_version_request,
            'default.topic.config': {'produce.offset.report': True}}

    # Create producer
    if schema_registry_url:
        conf['schema.registry.url'] = schema_registry_url
        p = avro.AvroProducer(conf)
    else:
        p = avro.AvroProducer(conf, schema_registry=InMemorySchemaRegistry())

    prim_float = avro.load(os.path.join(avsc_dir, "primitive_float.avsc"))
    prim_string = avro.load(os.path.join(avsc_dir, "primitive_string.avsc"))
    basic = avro.load(os.path.join(avsc_dir, "basic_schema.avsc"))
    str_value = 'abc'
    float_value = 32.

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
    ]

    # Consumer config
    cons_conf = {'bootstrap.servers': bootstrap_servers,
                 'group.id': 'test.py',
                 'session.timeout.ms': 6000,
                 'enable.auto.commit': False,
                 'api.version.request': api_version_request,
                 'on_commit': print_commit_result,
                 'error_cb': error_cb,
                 'default.topic.config': {
                     'auto.offset.reset': 'earliest'
                 }}

    for i, combo in enumerate(combinations):
        combo['topic'] = str(uuid.uuid4())
        p.produce(**combo)
        p.poll(0)
        p.flush()

        # Create consumer
        conf = copy(cons_conf)
        if schema_registry_url:
            conf['schema.registry.url'] = schema_registry_url
            c = avro.AvroConsumer(conf)
        else:
            c = avro.AvroConsumer(conf, schema_registry=InMemorySchemaRegistry())
        c.subscribe([combo['topic']])

        while True:
            msg = c.poll(0)
            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == confluent_kafka.KafkaError._PARTITION_EOF:
                    break
                else:
                    continue

            tstype, timestamp = msg.timestamp()
            print('%s[%d]@%d: key=%s, value=%s, tstype=%d, timestamp=%s' %
                  (msg.topic(), msg.partition(), msg.offset(),
                   msg.key(), msg.value(), tstype, timestamp))

            c.commit(msg, async=False)

        # Close consumer
        c.close()


def verify_producer_performance(with_dr_cb=True):
    """ Time how long it takes to produce and delivery X messages """
    conf = {'bootstrap.servers': bootstrap_servers,
            'api.version.request': api_version_request,
            'error_cb': error_cb}

    p = confluent_kafka.Producer(**conf)

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


def print_commit_result(err, partitions):
    if err is not None:
        print('# Failed to commit offsets: %s: %s' % (err, partitions))
    else:
        print('# Committed offsets for: %s' % partitions)


def verify_consumer():
    """ Verify basic Consumer functionality """

    # Consumer config
    conf = {'bootstrap.servers': bootstrap_servers,
            'group.id': 'test.py',
            'session.timeout.ms': 6000,
            'enable.auto.commit': False,
            'api.version.request': api_version_request,
            'on_commit': print_commit_result,
            'error_cb': error_cb,
            'default.topic.config': {
                'auto.offset.reset': 'earliest'
            }}

    # Create consumer
    c = confluent_kafka.Consumer(**conf)

    # Subscribe to a list of topics
    c.subscribe([topic])

    max_msgcnt = 100
    msgcnt = 0

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
                break
            else:
                print('Consumer error: %s: ignoring' % msg.error())
                break

        tstype, timestamp = msg.timestamp()
        print('%s[%d]@%d: key=%s, value=%s, tstype=%d, timestamp=%s' %
              (msg.topic(), msg.partition(), msg.offset(),
               msg.key(), msg.value(), tstype, timestamp))

        if (msg.offset() % 5) == 0:
            # Async commit
            c.commit(msg, async=True)
        elif (msg.offset() % 4) == 0:
            offsets = c.commit(msg, async=False)
            assert len(offsets) == 1, 'expected 1 offset, not %s' % (offsets)
            assert offsets[0].offset == msg.offset()+1, \
                'expected offset %d to be committed, not %s' % \
                (msg.offset(), offsets)
            print('Sync committed offset: %s' % offsets)

        msgcnt += 1
        if msgcnt >= max_msgcnt:
            print('max_msgcnt %d reached' % msgcnt)
            break

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
    c = confluent_kafka.Consumer(**conf)
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
            'default.topic.config': {
                'auto.offset.reset': 'earliest'
            }}

    c = confluent_kafka.Consumer(**conf)

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
            if msg.error().code() == confluent_kafka.KafkaError._PARTITION_EOF:
                # Reached EOF for a partition, ignore.
                continue
            else:
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
            'default.topic.config': {
                'auto.offset.reset': 'earliest'
            }}

    c = confluent_kafka.Consumer(**conf)
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
            if msg.error().code() == confluent_kafka.KafkaError._PARTITION_EOF:
                # Reached EOF for a partition, ignore.
                continue
            else:
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


if __name__ == '__main__':

    if len(sys.argv) > 1:
        bootstrap_servers = sys.argv[1]
        if len(sys.argv) > 2:
            topic = sys.argv[2]
        if len(sys.argv) > 3:
            schema_registry_url = sys.argv[3]
    else:
        print('Usage: %s <broker> [<topic>] [<schema_registry>]' % sys.argv[0])
        sys.exit(1)

    print('Using confluent_kafka module version %s (0x%x)' % confluent_kafka.version())
    print('Using librdkafka version %s (0x%x)' % confluent_kafka.libversion())

    print('=' * 30, 'Verifying Producer', '=' * 30)
    verify_producer()

    print('=' * 30, 'Verifying Consumer', '=' * 30)
    verify_consumer()

    print('=' * 30, 'Verifying Producer performance (with dr_cb)', '=' * 30)
    verify_producer_performance(with_dr_cb=True)

    print('=' * 30, 'Verifying Producer performance (without dr_cb)', '=' * 30)
    verify_producer_performance(with_dr_cb=False)

    print('=' * 30, 'Verifying Consumer performance', '=' * 30)
    verify_consumer_performance()

    print('=' * 30, 'Verifying stats_cb', '=' * 30)
    verify_stats_cb()

    print('=' * 30, 'Verifying AVRO', '=' * 30)
    verify_avro()

    print('=' * 30, 'Done', '=' * 30)
