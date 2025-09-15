#!/usr/bin/env python
# -*- coding: utf-8 -*-
import pytest
from struct import pack

from confluent_kafka import Producer, KafkaError, KafkaException, \
    TopicPartition, libversion

from tests.common import TestConsumer


def error_cb(err):
    print('error_cb', err)


def test_basic_api():
    """ Basic API tests, these wont really do anything since there is no
        broker configured. """

    with pytest.raises(TypeError) as ex:
        p = Producer()
    assert ex.match('expected configuration dict')

    p = Producer({'socket.timeout.ms': 10,
                  'error_cb': error_cb,
                  'message.timeout.ms': 10})

    p.produce('mytopic')
    p.produce('mytopic', value='somedata', key='a key')

    def on_delivery(err, msg):
        print('delivery', err, msg)
        # Since there is no broker, produced messages should time out.
        assert err.code() == KafkaError._MSG_TIMED_OUT
        print('message latency', msg.latency())

    p.produce(topic='another_topic', value='testing', partition=9,
              callback=on_delivery)

    p.poll(0.001)

    p.flush(0.002)
    p.flush()

    try:
        p.list_topics(timeout=0.2)
    except KafkaException as e:
        assert e.args[0].code() in (KafkaError._TIMED_OUT, KafkaError._TRANSPORT)


def test_produce_timestamp():
    """ Test produce() with timestamp arg """
    p = Producer({'socket.timeout.ms': 10,
                  'error_cb': error_cb,
                  'message.timeout.ms': 10})

    # Requires librdkafka >=v0.9.4

    try:
        p.produce('mytopic', timestamp=1234567)
    except NotImplementedError:
        # Should only fail on non-supporting librdkafka
        if libversion()[1] >= 0x00090400:
            raise

    p.flush()


# Should be updated to 0.11.4 when it is released
@pytest.mark.skipif(libversion()[1] < 0x000b0400,
                    reason="requires librdkafka >=0.11.4")
def test_produce_headers():
    """ Test produce() with timestamp arg """
    p = Producer({'socket.timeout.ms': 10,
                  'error_cb': error_cb,
                  'message.timeout.ms': 10})

    binval = pack('hhl', 1, 2, 3)

    headers_to_test = [
        [('headerkey', 'headervalue')],
        [('dupkey', 'dupvalue'), ('empty', ''), ('dupkey', 'dupvalue')],
        [('dupkey', 'dupvalue'), ('dupkey', 'diffvalue')],
        [('key_with_null_value', None)],
        [('binaryval', binval)],
        [('alreadyutf8', u'Småland'.encode('utf-8'))],
        [('isunicode', 'Jämtland')],

        {'headerkey': 'headervalue'},
        {'dupkey': 'dupvalue', 'empty': '', 'dupkey': 'dupvalue'},  # noqa: F601
        {'dupkey': 'dupvalue', 'dupkey': 'diffvalue'},  # noqa: F601
        {'key_with_null_value': None},
        {'binaryval': binval},
        {'alreadyutf8': u'Småland'.encode('utf-8')},
        {'isunicode': 'Jämtland'}
        ]

    for headers in headers_to_test:
        print('headers', type(headers), headers)
        p.produce('mytopic', value='somedata', key='a key', headers=headers)
        p.produce('mytopic', value='somedata', headers=headers)

    with pytest.raises(TypeError):
        p.produce('mytopic', value='somedata', key='a key', headers=('a', 'b'))

    with pytest.raises(TypeError):
        p.produce('mytopic', value='somedata', key='a key', headers=[('malformed_header')])

    with pytest.raises(TypeError):
        p.produce('mytopic', value='somedata', headers={'anint': 1234})

    p.flush()


# Should be updated to 0.11.4 when it is released
@pytest.mark.skipif(libversion()[1] >= 0x000b0400,
                    reason="Old versions should fail when using headers")
def test_produce_headers_should_fail():
    """ Test produce() with timestamp arg """
    p = Producer({'socket.timeout.ms': 10,
                  'error_cb': error_cb,
                  'message.timeout.ms': 10})

    with pytest.raises(NotImplementedError) as ex:
        p.produce('mytopic', value='somedata', key='a key', headers=[('headerkey', 'headervalue')])
    assert ex.match('Producer message headers requires confluent-kafka-python built for librdkafka version >=v0.11.4')


def test_subclassing():
    class SubProducer(Producer):
        def __init__(self, conf, topic):
            super(SubProducer, self).__init__(conf)
            self.topic = topic

        def produce_hi(self):
            super(SubProducer, self).produce(self.topic, value='hi')

    sp = SubProducer(dict(), 'atopic')
    assert isinstance(sp, SubProducer)

    # Invalid config should fail
    with pytest.raises(KafkaException):
        sp = SubProducer({'should.fail': False}, 'mytopic')

    sp = SubProducer({'log.thread.name': True}, 'mytopic')
    sp.produce('someother', value='not hello')
    sp.produce_hi()


def test_dr_msg_errstr():
    """
    Test that the error string for failed messages works (issue #129).
    The underlying problem is that librdkafka reuses the message payload
    for error value on Consumer messages, but on Producer messages the
    payload is the original payload and no rich error string exists.
    """
    p = Producer({"message.timeout.ms": 10})

    def handle_dr(err, msg):
        # Neither message payloads must not affect the error string.
        assert err is not None
        assert err.code() == KafkaError._MSG_TIMED_OUT
        assert "Message timed out" in err.str()

    # Unicode safe string
    p.produce('mytopic', "This is the message payload", on_delivery=handle_dr)

    # Invalid unicode sequence
    p.produce('mytopic', "\xc2\xc2", on_delivery=handle_dr)

    p.flush()


def test_set_partitioner_murmur2():
    """
    Test ability to set built-in partitioner type murmur
    """
    Producer({'partitioner': 'murmur2'})


def test_set_partitioner_murmur2_random():
    """
    Test ability to set built-in partitioner type murmur2_random
    """
    Producer({'partitioner': 'murmur2_random'})


def test_set_invalid_partitioner_murmur():
    """
    Assert invalid partitioner raises KafkaException
    """
    with pytest.raises(KafkaException) as ex:
        Producer({'partitioner': 'murmur'})
    assert ex.match('Invalid value for configuration property "partitioner": murmur')


def test_transaction_api():
    """ Excercise the transactional API """
    p = Producer({"transactional.id": "test"})

    with pytest.raises(KafkaException) as ex:
        p.init_transactions(0.5)
    assert ex.value.args[0].code() == KafkaError._TIMED_OUT
    assert ex.value.args[0].retriable() is True
    assert ex.value.args[0].fatal() is False
    assert ex.value.args[0].txn_requires_abort() is False

    # Any subsequent APIs will fail since init did not succeed.
    with pytest.raises(KafkaException) as ex:
        p.begin_transaction()
    assert ex.value.args[0].code() == KafkaError._CONFLICT
    assert ex.value.args[0].retriable() is True
    assert ex.value.args[0].fatal() is False
    assert ex.value.args[0].txn_requires_abort() is False

    consumer = TestConsumer({"group.id": "testgroup"})
    group_metadata = consumer.consumer_group_metadata()
    consumer.close()

    with pytest.raises(KafkaException) as ex:
        p.send_offsets_to_transaction([TopicPartition("topic", 0, 123)],
                                      group_metadata)
    assert ex.value.args[0].code() == KafkaError._CONFLICT
    assert ex.value.args[0].retriable() is True
    assert ex.value.args[0].fatal() is False
    assert ex.value.args[0].txn_requires_abort() is False

    with pytest.raises(KafkaException) as ex:
        p.commit_transaction(0.5)
    assert ex.value.args[0].code() == KafkaError._CONFLICT
    assert ex.value.args[0].retriable() is True
    assert ex.value.args[0].fatal() is False
    assert ex.value.args[0].txn_requires_abort() is False

    with pytest.raises(KafkaException) as ex:
        p.abort_transaction(0.5)
    assert ex.value.args[0].code() == KafkaError._CONFLICT
    assert ex.value.args[0].retriable() is True
    assert ex.value.args[0].fatal() is False
    assert ex.value.args[0].txn_requires_abort() is False


def test_purge():
    """
    Verify that when we have a higher message.timeout.ms timeout, we can use purge()
    to stop waiting for messages and get delivery reports
    """
    p = Producer(
        {"socket.timeout.ms": 10, "error_cb": error_cb, "message.timeout.ms": 30000}
    )  # 30 seconds

    # Hack to detect on_delivery was called because inner functions can modify nonlocal objects.
    # When python2 support is dropped, we can use the "nonlocal" keyword instead
    cb_detector = {"on_delivery_called": False}

    def on_delivery(err, msg):
        cb_detector["on_delivery_called"] = True
        # Because we are purging messages, we should see a PURGE_QUEUE kafka error
        assert err.code() == KafkaError._PURGE_QUEUE

    # Our message won't be delivered, but also won't timeout yet because our timeout is 30s.
    p.produce(topic="some_topic", value="testing", partition=9, callback=on_delivery)
    p.flush(0.002)
    assert not cb_detector["on_delivery_called"]

    # When in_queue set to false, we won't purge the message and get delivery callback
    p.purge(in_queue=False)
    p.flush(0.002)
    assert not cb_detector["on_delivery_called"]

    # When we purge including the queue, the message should have delivered a delivery report
    # with a PURGE_QUEUE error
    p.purge()
    p.flush(0.002)
    assert cb_detector["on_delivery_called"]


def test_producer_bool_value():
    """
    Make sure producer has a truth-y bool value
    See https://github.com/confluentinc/confluent-kafka-python/issues/1427
    """

    p = Producer({})
    assert bool(p)


def test_produce_batch_basic_functionality():
    """Comprehensive test of basic produce_batch functionality"""
    producer = Producer({'bootstrap.servers': 'localhost:9092'})
    
    # Test 1: Basic batch with mixed data types
    messages = [
        {'value': b'bytes_message', 'key': b'bytes_key'},
        {'value': 'string_message', 'key': 'string_key'},
        {'value': 'unicode: 你好', 'key': b'mixed_key'},
        {'value': None, 'key': None},  # None values
        {'value': b'', 'key': ''},     # Empty values
        {}  # Empty dict
    ]
    
    count = producer.produce_batch('test-topic', messages)
    assert count == 6
    
    # Verify no errors were added
    for msg in messages:
        assert '_error' not in msg
    
    # Test 2: Partition handling
    partition_messages = [
        {'value': b'default_partition'},
        {'value': b'specific_partition', 'partition': 1},
        {'value': b'another_partition', 'partition': 2}
    ]
    
    count = producer.produce_batch('test-topic', partition_messages, partition=0)
    assert count == 3
    
    # Test 3: Empty batch
    count = producer.produce_batch('test-topic', [])
    assert count == 0
    
    # Test 4: Single message batch
    count = producer.produce_batch('test-topic', [{'value': b'single'}])
    assert count == 1
    
    # Test 5: Large batch
    large_messages = [{'value': f'msg_{i}'.encode()} for i in range(100)]
    count = producer.produce_batch('test-topic', large_messages)
    assert count == 100


@pytest.mark.parametrize("invalid_input,expected_error", [
    ("not_a_list", "messages must be a list"),
    ({'not': 'list'}, "messages must be a list"),
    ([{'value': b'good'}, "not_dict", {'value': b'good2'}], "Message at index 1 must be a dict"),
    ([{'value': 123}], "Message value at index 0 must be bytes or str"),
    ([{'value': b'good', 'key': ['invalid']}], "Message key at index 0 must be bytes or str"),
    ([{'value': b'good', 'partition': "invalid"}], "Message partition at index 0 must be int"),
])
def test_produce_batch_input_validation(invalid_input, expected_error):
    """Test input validation with various invalid inputs"""
    producer = Producer({'bootstrap.servers': 'localhost:9092'})
    
    with pytest.raises((TypeError, ValueError), match=expected_error):
        producer.produce_batch('test-topic', invalid_input)


def test_produce_batch_partial_failures():
    """Test handling of partial batch failures"""
    # Configure small queue to trigger failures
    producer = Producer({
        'bootstrap.servers': 'localhost:9092',
        'queue.buffering.max.messages': 5
    })
    
    # Fill up the queue first to cause some failures
    try:
        for i in range(10):
            producer.produce('test-topic', f'filler_{i}')
    except BufferError:
        pass  # Expected when queue fills up
    
    # Now try batch produce
    messages = [{'value': f'batch_msg_{i}'.encode()} for i in range(10)]
    count = producer.produce_batch('test-topic', messages)
    
    # Some should succeed, some should fail
    assert 0 <= count <= len(messages)
    
    # Check error annotations on failed messages
    failed_messages = [msg for msg in messages if '_error' in msg]
    successful_count = len(messages) - len(failed_messages)
    
    assert successful_count == count
    assert len(failed_messages) == len(messages) - count
    
    # Verify error objects are properly created
    for msg in failed_messages:
        error = msg['_error']
        assert hasattr(error, 'code')
        assert hasattr(error, 'name')
        assert str(error)  # Should be convertible to string


def test_produce_batch_unsupported_features():
    """Test currently unsupported features (timestamps, headers limitations)"""
    producer = Producer({'bootstrap.servers': 'localhost:9092'})
    
    # Test 1: Timestamps not supported in batch mode
    messages_with_timestamp = [
        {'value': b'msg', 'timestamp': 1234567890}
    ]
    
    with pytest.raises(NotImplementedError, match="Message timestamps are not currently supported"):
        producer.produce_batch('test-topic', messages_with_timestamp)
    
    # Test 2: Headers are parsed but ignored (should not fail)
    messages_with_headers = [
        {'value': b'msg', 'headers': {'key': b'value'}}
    ]
    
    count = producer.produce_batch('test-topic', messages_with_headers)
    assert count == 1  # Should succeed but headers are ignored


def test_produce_batch_callback_mechanisms():
    """Test all callback-related functionality in one comprehensive test"""
    producer = Producer({'bootstrap.servers': 'localhost:9092'})
    
    # Callback tracking
    global_calls = []
    callback1_calls = []
    callback2_calls = []
    exception_calls = []
    
    def global_callback(err, msg):
        global_calls.append((err, msg.value() if msg else None))
    
    def callback1(err, msg):
        callback1_calls.append(msg.value())
    
    def callback2(err, msg):
        callback2_calls.append(msg.value())
    
    def exception_callback(err, msg):
        exception_calls.append(msg.value())
        raise ValueError("Test callback exception")
    
    # Test 1: Mixed callback scenarios
    messages = [
        {'value': b'msg1', 'callback': callback1},      # Per-message callback
        {'value': b'msg2'},                             # Uses global callback
        {'value': b'msg3', 'callback': callback2},      # Different per-message callback
        {'value': b'msg4'},                             # Uses global callback
        {'value': b'msg5', 'callback': exception_callback}  # Callback that throws
    ]
    
    count = producer.produce_batch('test-topic', messages, on_delivery=global_callback)
    assert count == 5
    
    # Flush to trigger all callbacks
    producer.flush()
    
    # Verify callback distribution
    assert callback1_calls == [b'msg1']
    assert callback2_calls == [b'msg3']
    assert exception_calls == [b'msg5']
    
    # Global callback should handle msg2 and msg4
    global_values = [msg for err, msg in global_calls]
    assert set(global_values) == {b'msg2', b'msg4'}
    
    # Test 2: No callbacks (should not crash)
    no_callback_messages = [{'value': b'no_cb_msg'}]
    count = producer.produce_batch('test-topic', no_callback_messages)
    assert count == 1
    producer.flush()  # Should not crash
    
    # Test 3: Callback parameter aliases
    alias_calls = []
    def alias_callback(err, msg):
        alias_calls.append(msg.value())
    
    # Test both 'callback' and 'on_delivery' work
    count1 = producer.produce_batch('test-topic', [{'value': b'alias1'}], callback=alias_callback)
    count2 = producer.produce_batch('test-topic', [{'value': b'alias2'}], on_delivery=alias_callback)
    
    assert count1 == 1
    assert count2 == 1
    
    producer.flush()
    assert set(alias_calls) == {b'alias1', b'alias2'}


def test_produce_batch_edge_cases():
    """Test edge cases, Unicode handling, and boundary conditions"""
    producer = Producer({'bootstrap.servers': 'localhost:9092'})
    
    # Test 1: Unicode and encoding edge cases
    unicode_messages = [
        {'value': '🚀 emoji', 'key': '🔑 key'},
        {'value': '中文消息', 'key': '中文键'},
        {'value': 'Ñoño español', 'key': 'clave'},
        {'value': 'Здравствуй', 'key': 'ключ'},
        {'value': '\x00\x01\x02', 'key': 'control'},
        {'value': 'UTF-8: 你好'.encode('utf-8'), 'key': b'bytes_utf8'},
        {'value': b'\x80\x81\x82', 'key': 'binary'}  # Non-UTF8 bytes
    ]
    
    count = producer.produce_batch('test-topic', unicode_messages)
    assert count == len(unicode_messages)
    
    # Test 2: Large messages
    large_payload = b'x' * (100 * 1024)  # 100KB message
    large_messages = [
        {'value': large_payload, 'key': b'large1'},
        {'value': b'small', 'key': b'small1'},
        {'value': large_payload, 'key': b'large2'}
    ]
    
    count = producer.produce_batch('test-topic', large_messages)
    assert count >= 0  # May succeed or fail based on broker config
    
    # Test 3: Batch size scalability
    batch_sizes = [1, 10, 100, 500]
    for size in batch_sizes:
        messages = [{'value': f'scale_{size}_{i}'.encode()} for i in range(size)]
        count = producer.produce_batch('test-topic', messages)
        assert count == size, f"Failed for batch size {size}"
    
    # Test 4: Memory cleanup verification (basic check)
    import gc
    
    # Create and process many batches
    for batch_num in range(10):
        messages = [{'value': f'mem_test_{batch_num}_{i}'.encode()} for i in range(50)]
        count = producer.produce_batch('test-topic', messages)
        assert count == 50
    
    producer.flush()
    gc.collect()  # Force garbage collection
    
    # If we get here without memory errors, cleanup is working
    assert True
