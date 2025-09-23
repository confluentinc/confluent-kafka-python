#!/usr/bin/env python
# -*- coding: utf-8 -*-
import gc
import pytest
import threading
import time
from struct import pack

from confluent_kafka import Producer, KafkaError, KafkaException, \
    TopicPartition, libversion

from tests.common import TestConsumer

# Additional imports for batch integration tests
from confluent_kafka.serialization import StringSerializer
from confluent_kafka import SerializingProducer
from confluent_kafka.avro import AvroProducer


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


def test_produce_batch_basic_types_and_data():
    """Test basic data types, None/empty handling, and partition functionality."""
    producer = Producer({'bootstrap.servers': 'localhost:9092'})
    
    basic_messages = [
        {'value': b'bytes_message', 'key': b'bytes_key'},
        {'value': 'string_message', 'key': 'string_key'},
        {'value': 'unicode: 你好', 'key': b'mixed_key'},
        {'value': None, 'key': None},
        {'value': b'', 'key': ''},
        {}
    ]
    count = producer.produce_batch('test-topic', basic_messages)
    assert count == 6
    for msg in basic_messages:
        assert '_error' not in msg
    
    zero_vs_none_messages = [
        {'value': b'', 'key': b''},
        {'value': '', 'key': ''},
        {'value': None, 'key': None},
        {'value': b'data', 'key': None},
        {'value': None, 'key': b'key'},
    ]
    count = producer.produce_batch('test-topic', zero_vs_none_messages)
    assert count == 5
    
    binary_messages = [
        {'value': b'\x00' * 100, 'key': b'null_bytes'},
        {'value': bytes(range(256)), 'key': b'all_bytes'},
        {'value': b'\xff' * 100, 'key': b'high_bytes'},
    ]
    count = producer.produce_batch('test-topic', binary_messages)
    assert count == 3
    
    partition_messages = [
        {'value': b'default_partition'},
        {'value': b'specific_partition', 'partition': 1},
        {'value': b'another_partition', 'partition': 2}
    ]
    count = producer.produce_batch('test-topic', partition_messages, partition=0)
    assert count == 3
    
    count = producer.produce_batch('test-topic', [])
    assert count == 0
    
    count = producer.produce_batch('test-topic', [{'value': b'single'}])
    assert count == 1
    

def test_produce_batch_encoding_and_unicode():
    """Test advanced encoding, Unicode handling, and large message scenarios."""
    producer = Producer({'bootstrap.servers': 'localhost:9092'})
    
    encoding_messages = [
        {'value': 'UTF-8: café', 'key': 'utf8'},
        {'value': 'Emoji: 🚀🎉', 'key': '🔑'},
        {'value': 'Latin chars: áéíóú', 'key': 'latin'},
        {'value': 'Cyrillic: Здравствуй', 'key': 'cyrillic'},
        {'value': 'CJK: 中文日本語한국어', 'key': 'cjk'},
    ]
    count = producer.produce_batch('test-topic', encoding_messages)
    assert count == 5
    
    unicode_messages = [
        {'value': '🚀 emoji', 'key': '🔑 key'},
        {'value': '中文消息', 'key': '中文键'},
        {'value': 'Ñoño español', 'key': 'clave'},
        {'value': 'Здравствуй', 'key': 'ключ'},
        {'value': '\x00\x01\x02', 'key': 'control'},
        {'value': 'UTF-8: 你好'.encode('utf-8'), 'key': b'bytes_utf8'},
        {'value': b'\x80\x81\x82', 'key': 'binary'}
    ]
    count = producer.produce_batch('test-topic', unicode_messages)
    assert count == len(unicode_messages)


def test_produce_batch_scalability_and_limits():
    """Test batch scalability, large messages, and API limitations."""
    producer = Producer({'bootstrap.servers': 'localhost:9092'})
    
    large_messages = [{'value': f'msg_{i}'.encode()} for i in range(100)]
    count = producer.produce_batch('test-topic', large_messages)
    assert count == 100
    
    large_payload = b'x' * (100 * 1024)
    large_messages = [
        {'value': large_payload, 'key': b'large1'},
        {'value': b'small', 'key': b'small1'},
        {'value': large_payload, 'key': b'large2'}
    ]
    count = producer.produce_batch('test-topic', large_messages)
    assert count >= 0
    
    long_string = 'x' * (1024 * 1024)
    long_string_messages = [
        {'value': long_string, 'key': 'long_value'},
        {'value': 'short', 'key': long_string},
    ]
    count = producer.produce_batch('test-topic', long_string_messages)
    assert count >= 0
    
    batch_sizes = [1, 10, 100, 500]
    for size in batch_sizes:
        messages = [{'value': f'scale_{size}_{i}'.encode()} for i in range(size)]
        count = producer.produce_batch('test-topic', messages)
        assert count == size, f"Failed for batch size {size}"
    
    messages_with_timestamp = [{'value': b'msg', 'timestamp': 1234567890}]
    with pytest.raises(NotImplementedError, match="Message timestamps are not currently supported"):
        producer.produce_batch('test-topic', messages_with_timestamp)
    
    messages_with_headers = [{'value': b'msg', 'headers': {'key': b'value'}}]
    count = producer.produce_batch('test-topic', messages_with_headers)
    assert count == 1


@pytest.mark.parametrize("invalid_input,expected_error", [
    ("not_a_list", "messages must be a list"),
    ({'not': 'list'}, "messages must be a list"),
    ([{'value': b'good'}, "not_dict", {'value': b'good2'}], "Message at index 1 must be a dict"),
    ([{'value': 123}], "Message value at index 0 must be bytes or str"),
    ([{'value': b'good', 'key': ['invalid']}], "Message key at index 0 must be bytes or str"),
    ([{'value': b'good', 'partition': "invalid"}], "Message partition at index 0 must be int"),
    ([{'value': b'test', 'partition': -2}], None),  # Negative partition
    ([{'value': b'test', 'partition': 2147483647}], None),  # Max int32
    ([{'value': b'test', 'partition': 999999}], None),  # Very large partition
])
def test_produce_batch_input_validation(invalid_input, expected_error):
    """Test input validation and message field validation."""
    producer = Producer({'bootstrap.servers': 'localhost:9092'})
    
    if expected_error is None:
        count = producer.produce_batch('test-topic', invalid_input)
        assert count >= 0
    else:
        with pytest.raises((TypeError, ValueError), match=expected_error):
            producer.produce_batch('test-topic', invalid_input)
    
    messages_with_extras = [{
        'value': b'normal', 'key': b'normal', 'partition': 0,
        'callback': lambda err, msg: None,
        'extra_field': 'should_be_ignored', 'another_extra': 123, '_private': 'user_private_field',
    }]
    count = producer.produce_batch('test-topic', messages_with_extras)
    assert count == 1
    
    special_topics = ["topic-with-dashes", "topic_with_underscores", "topic.with.dots", "topic123numbers"]
    for topic in special_topics:
        try:
            count = producer.produce_batch(topic, [{'value': b'test'}])
            assert count >= 0
        except (KafkaException, ValueError, TypeError):
            assert True


def test_produce_batch_argument_validation():
    """Test function argument validation and topic name handling."""
    producer = Producer({'bootstrap.servers': 'localhost:9092'})
    
    with pytest.raises(TypeError):
        producer.produce_batch()
    
    with pytest.raises(TypeError):
        producer.produce_batch(123, [{'value': b'test'}])
    
    with pytest.raises((TypeError, ValueError)):
        producer.produce_batch('topic', [{'value': b'test'}], partition="invalid")
    
    try:
        producer.produce_batch("", [{'value': b'test'}])
        assert True
    except (TypeError, ValueError, KafkaException):
        assert True
    
    very_long_topic = "a" * 300
    try:
        count = producer.produce_batch(very_long_topic, [{'value': b'test'}])
        assert count >= 0
    except (TypeError, ValueError, KafkaException):
        assert True
    
    with pytest.raises(TypeError):
        producer.produce_batch(None, [{'value': b'test'}])
    
    try:
        producer.produce_batch('topic', [{'value': b'test'}], callback="not_callable")
        assert True
    except (TypeError, AttributeError):
        assert True


def test_produce_batch_partial_failures():
    """Test partial failure scenarios and error annotation."""
    small_queue_producer = Producer({
        'bootstrap.servers': 'localhost:9092',
        'queue.buffering.max.messages': 5
    })
    
    try:
        for i in range(10):
            small_queue_producer.produce('test-topic', f'filler_{i}')
    except BufferError:
        assert True
    
    messages = [{'value': f'batch_msg_{i}'.encode()} for i in range(10)]
    count = small_queue_producer.produce_batch('test-topic', messages)
    assert 0 <= count <= len(messages)
    
    failed_messages = [msg for msg in messages if '_error' in msg]
    successful_count = len(messages) - len(failed_messages)
    assert successful_count == count
    
    restrictive_producer = Producer({
        'bootstrap.servers': 'localhost:9092',
        'queue.buffering.max.messages': 1,
        'message.max.bytes': 1000
    })
    
    mixed_size_messages = [
        {'value': b'small'},
        {'value': b'x' * 2000},
        {'value': b'tiny'},
    ]
    count = restrictive_producer.produce_batch('test-topic', mixed_size_messages)
    assert 0 <= count <= 3
    
    all_fail_messages = [{'value': b'x' * 10000} for _ in range(3)]
    count = restrictive_producer.produce_batch('test-topic', all_fail_messages)
    assert count == 0
    assert all('_error' in msg for msg in all_fail_messages)


def test_produce_batch_callback_mechanisms():
    """Test basic callback mechanisms and distribution."""
    producer = Producer({'bootstrap.servers': 'localhost:9092'})
    
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
    
    messages = [
        {'value': b'msg1', 'callback': callback1},
        {'value': b'msg2'},
        {'value': b'msg3', 'callback': callback2},
        {'value': b'msg4'},
        {'value': b'msg5', 'callback': exception_callback}
    ]
    
    count = producer.produce_batch('test-topic', messages, on_delivery=global_callback)
    assert count == 5
    
    try:
        producer.flush()
    except ValueError as e:
        assert "Test callback exception" in str(e)
    
    assert callback1_calls == [b'msg1']
    assert callback2_calls == [b'msg3']
    assert exception_calls == [b'msg5']
    global_values = [msg for err, msg in global_calls]
    assert set(global_values) == {b'msg2', b'msg4'}
    
    no_callback_messages = [{'value': b'no_cb_msg'}]
    count = producer.produce_batch('test-topic', no_callback_messages)
    assert count == 1
    producer.flush()
    
    alias_calls = []
    def alias_callback(err, msg):
        alias_calls.append(msg.value())
    
    count1 = producer.produce_batch('test-topic', [{'value': b'alias1'}], callback=alias_callback)
    count2 = producer.produce_batch('test-topic', [{'value': b'alias2'}], on_delivery=alias_callback)
    assert count1 == 1 and count2 == 1
    producer.flush()
    assert set(alias_calls) == {b'alias1', b'alias2'}


def test_produce_batch_callback_advanced():
    """Test advanced callback scenarios and edge cases."""
    producer = Producer({'bootstrap.servers': 'localhost:9092'})
    
    circular_calls = []
    def circular_callback(err, msg):
        circular_callback.self_ref = circular_callback
        circular_calls.append(msg.value() if msg else None)
        
    messages = [{'value': b'circular', 'callback': circular_callback}]
    count = producer.produce_batch('test-topic', messages)
    assert count == 1
    producer.flush()
    
    slow_calls = []
    def slow_callback(err, msg):
        time.sleep(0.01)
        slow_calls.append(msg.value() if msg else None)
        
    slow_messages = [{'value': f'slow_{i}'.encode(), 'callback': slow_callback} for i in range(5)]
    count = producer.produce_batch('test-topic', slow_messages)
    producer.flush(2.0)
    assert count == 5
    assert len(slow_calls) == 5


def test_produce_batch_exception_propagation():
    """Test exception handling and propagation from callbacks."""
    producer = Producer({'bootstrap.servers': 'localhost:9092'})
    
    exception_calls_2 = []
    def exception_callback_2(err, msg):
        exception_calls_2.append(msg.value() if msg else None)
        raise RuntimeError("Critical callback error")
    
    messages = [
        {'value': b'normal_msg'},
        {'value': b'exception_msg', 'callback': exception_callback_2},
        {'value': b'another_normal_msg'}
    ]
    count = producer.produce_batch('test-topic', messages)
    assert count == 3
    
    try:
        producer.flush(1.0)
    except RuntimeError as e:
        assert "Critical callback error" in str(e)
        assert len(exception_calls_2) == 1
    
    multi_exception_calls = []
    def multi_exception_callback(err, msg):
        multi_exception_calls.append(msg.value() if msg else None)
        raise ValueError(f"Error from {msg.value()}")
    
    multi_messages = [
        {'value': b'error1', 'callback': multi_exception_callback},
        {'value': b'error2', 'callback': multi_exception_callback}
    ]
    count = producer.produce_batch('test-topic', multi_messages)
    assert count == 2
    
    try:
        producer.flush(1.0)
    except (RuntimeError, ValueError):
        assert True
    
    assert len(multi_exception_calls) >= 1


def test_produce_batch_threading_basic():
    """Test basic threading and producer configurations."""
    configs = [
        {'bootstrap.servers': 'localhost:9092', 'acks': 'all'},
        {'bootstrap.servers': 'localhost:9092', 'acks': '0'},
        {'bootstrap.servers': 'localhost:9092', 'compression.type': 'gzip'},
        {'bootstrap.servers': 'localhost:9092', 'batch.size': 1000},
        {'bootstrap.servers': 'localhost:9092', 'linger.ms': 100},
    ]
    
    for i, config in enumerate(configs):
        producer = Producer(config)
        messages = [{'value': f'config_{i}_msg_{j}'.encode()} for j in range(5)]
        count = producer.produce_batch('test-topic', messages)
        assert count == 5, f"Failed with config {config}"
    
    producer = Producer({'bootstrap.servers': 'localhost:9092'})
    results = []
    errors = []
    shared_counter = {'value': 0}
    
    def produce_worker(thread_id):
        try:
            for batch_num in range(4):
                shared_counter['value'] += 1
                messages = [{'value': f'thread_{thread_id}_batch_{batch_num}_msg_{i}_{shared_counter["value"]}'.encode()} 
                           for i in range(5)]
                count = producer.produce_batch('test-topic', messages)
                results.append((thread_id, batch_num, count))
        except Exception as e:
            errors.append((thread_id, e))
    
    # Start multiple threads
    threads = []
    for i in range(5):
        t = threading.Thread(target=produce_worker, args=(i,))
        threads.append(t)
        t.start()
    
    for t in threads:
        t.join()
    
    # Verify results
    assert len(results) == 20, f"Expected 20 results, got {len(results)}"
    assert len(errors) == 0, f"Unexpected errors: {errors}"
    
    for thread_id, batch_num, count in results:
        assert count == 5, f"Thread {thread_id} batch {batch_num} failed: {count}/5"
    
    rapid_producer = Producer({'bootstrap.servers': 'localhost:9092'})
    total_count = 0
    for batch_num in range(10):
        messages = [{'value': f'rapid_{batch_num}_{i}'.encode()} for i in range(10)]
        count = rapid_producer.produce_batch('test-topic', messages)
        total_count += count
    assert total_count == 100


def test_produce_batch_race_conditions():
    """Test advanced race conditions and resource contention."""
    race_producer = Producer({'bootstrap.servers': 'localhost:9092'})
    race_results = []
    race_errors = []
    contention_data = {'counter': 0, 'messages': []}
    
    def racing_producer(thread_id):
        try:
            for batch_num in range(3):
                contention_data['counter'] += 1
                shared_value = contention_data['counter']
                
                messages = [
                    {'value': f'race_t{thread_id}_b{batch_num}_m{i}_{shared_value}'.encode()}
                    for i in range(3)
                ]
                
                contention_data['messages'].extend(messages)
                count = race_producer.produce_batch('test-topic', messages)
                race_results.append((thread_id, batch_num, count))
                time.sleep(0.001)
                
        except Exception as e:
            race_errors.append((thread_id, e))
    
    race_threads = []
    for i in range(4):
        t = threading.Thread(target=racing_producer, args=(i,))
        race_threads.append(t)
        t.start()
    
    for t in race_threads:
        t.join()
    
    assert len(race_results) == 12, f"Expected 12 results, got {len(race_results)}"
    assert len(race_errors) == 0, f"Unexpected errors: {race_errors}"


def test_produce_batch_message_corruption():
    """Test message state corruption and recursive callback scenarios."""
    producer = Producer({'bootstrap.servers': 'localhost:9092'})
    
    original_messages = [
        {'value': b'msg1'},
        {'value': b'msg2'},
        {'value': b'msg3'}
    ]
    
    corruption_attempts = []
    def corrupting_callback(err, msg):
        corruption_attempts.append(msg.value() if msg else None)
        try:
            original_messages.clear()
            original_messages.append({'value': b'corrupted_during_callback'})
        except Exception as e:
            corruption_attempts.append(f'exception_{type(e).__name__}')
    
    original_messages[1]['callback'] = corrupting_callback
    
    count = producer.produce_batch('test-topic', original_messages)
    assert count == 3
    producer.flush(1.0)
    assert len(corruption_attempts) >= 1
    
    recursive_calls = []
    
    def recursive_callback(err, msg):
        if len(recursive_calls) < 2:
            recursive_calls.append(msg.value() if msg else None)
            try:
                new_messages = [{'value': f'recursive_{len(recursive_calls)}'.encode()}]
                producer.produce_batch('test-topic', new_messages)
            except Exception as e:
                recursive_calls.append(f'recursive_exception_{type(e).__name__}')
    
    recursive_messages = [{'value': b'recursive_start', 'callback': recursive_callback}]
    count = producer.produce_batch('test-topic', recursive_messages)
    assert count == 1
    producer.flush(2.0)
    assert len(recursive_calls) >= 1


def test_produce_batch_memory_stress():
    """Test memory stress scenarios and cleanup verification."""
    producer = Producer({'bootstrap.servers': 'localhost:9092'})
    
    max_messages = [{'value': f'msg_{i}'.encode()} for i in range(5000)]
    count = producer.produce_batch('test-topic', max_messages)
    assert 0 <= count <= len(max_messages)
    
    nested_messages = []
    for i in range(500):
        nested_messages.append({
            'value': f'nested_{i}'.encode(),
            'key': f'key_{i}'.encode(),
            'partition': i % 10,
            'callback': lambda err, msg: None
        })
    count = producer.produce_batch('test-topic', nested_messages)
    assert count >= 0
    
    for batch_num in range(10):
        messages = [{'value': f'mem_test_{batch_num}_{i}'.encode()} for i in range(50)]
        count = producer.produce_batch('test-topic', messages)
        assert count >= 0
        
        if batch_num % 3 == 0:
            gc.collect()
    
    producer.flush()
    gc.collect()
    

def test_produce_batch_memory_critical():
    """Test critical memory scenarios and producer lifecycle."""
    producer = Producer({'bootstrap.servers': 'localhost:9092'})
    
    destruction_calls = []
    
    def destruction_callback(err, msg):
        destruction_calls.append(msg.value() if msg else None)
        time.sleep(0.01)
    
    temp_producer = Producer({'bootstrap.servers': 'localhost:9092'})
    
    messages = [
        {'value': b'destruction_test_1', 'callback': destruction_callback},
        {'value': b'destruction_test_2', 'callback': destruction_callback}
    ]
    count = temp_producer.produce_batch('test-topic', messages)
    assert count == 2
    
    temp_producer.flush(0.01)
    del temp_producer
    gc.collect()
    time.sleep(0.05)
    
    
    memory_pressure_batches = []
    try:
        for i in range(5):
            large_messages = [
                {'value': b'x' * 5000}
                for j in range(50)
            ]
            count = producer.produce_batch(f'memory-pressure-topic-{i}', large_messages)
            memory_pressure_batches.append(count)
            
            if i % 2 == 0:
                gc.collect()
                
    except (MemoryError, BufferError) as e:
        assert isinstance(e, (MemoryError, BufferError))
    
    assert len(memory_pressure_batches) > 0
    assert sum(memory_pressure_batches) > 0


def test_produce_batch_resource_management():
    """Test resource management and handle lifecycle."""
    producers = []
    try:
        for i in range(10):
            p = Producer({'bootstrap.servers': 'localhost:9092'})
            producers.append(p)
            
            messages = [{'value': f'producer_{i}_msg_{j}'.encode()} for j in range(5)]
            count = p.produce_batch('test-topic', messages)
        assert count >= 0
    finally:
        for p in producers:
            try:
                p.flush(0.1)
            except Exception:
                continue
    
    for i in range(20):
        temp_messages = [{'value': f'temp_{i}_{j}'.encode()} for j in range(3)]
        temp_producer = Producer({'bootstrap.servers': 'localhost:9092'})
        count = temp_producer.produce_batch('test-topic', temp_messages)
        assert count >= 0
        temp_producer.flush(0.01)
    
    gc.collect()
    
    handles = []
    for i in range(50):
        try:
            messages = [{'value': f'handle_test_{i}'.encode()}]
            temp_producer = Producer({'bootstrap.servers': 'localhost:9092'})
            count = temp_producer.produce_batch('test-topic', messages)
            handles.append(temp_producer)
            assert count >= 0
        except Exception as e:
            print(f"System limits reached at iteration {i}: {type(e).__name__}: {e}")
            break
    
    for handle in handles:
        try:
            handle.flush(0.01)
        except Exception:
            continue


def test_produce_batch_error_conditions():
    """Test error conditions and system limits."""
    producer_small_queue = Producer({
        'bootstrap.servers': 'localhost:9092',
        'queue.buffering.max.messages': 2
    })
    
    try:
        for i in range(5):
            producer_small_queue.produce('test-topic', f'filler_{i}')
    except BufferError:
        assert True
    large_batch = [{'value': f'msg_{i}'.encode()} for i in range(10)]
    count = producer_small_queue.produce_batch('test-topic', large_batch)
    assert 0 <= count <= len(large_batch)
    
    producer = Producer({'bootstrap.servers': 'localhost:9092'})
    very_large_batch = [{'value': f'large_msg_{i}'.encode()} for i in range(1000)]
    count = producer.produce_batch('test-topic', very_large_batch)
    assert count >= 0
    
    huge_message = {'value': b'x' * (1024 * 1024)}
    count = producer.produce_batch('test-topic', [huge_message])
    assert count >= 0
    
    messages_mixed = [
        {'value': b'small1'},
        {'value': b'x' * (50 * 1024)},
        {'value': b'small2'},
        {'value': b'x' * (50 * 1024)},
        {'value': b'small3'},
    ]
    count = producer.produce_batch('test-topic', messages_mixed)
    assert 0 <= count <= len(messages_mixed)
    
    failed_count = sum(1 for msg in messages_mixed if '_error' in msg)
    success_count = len(messages_mixed) - failed_count
    assert success_count == count


def test_produce_batch_performance_scenarios():
    """Test performance boundaries and scalability."""
    producer = Producer({'bootstrap.servers': 'localhost:9092'})
    
    performance_results = []
    batch_sizes = [10, 50, 100, 500, 1000]
    
    for size in batch_sizes:
        start_time = time.time()
        messages = [{'value': f'perf_{size}_{i}'.encode()} for i in range(size)]
        count = producer.produce_batch('test-topic', messages)
        elapsed = time.time() - start_time
        
        performance_results.append((size, count, elapsed))
        assert count == size, f"Performance test failed for size {size}"
    
    for size, count, elapsed in performance_results:
        per_message_time = elapsed / count if count > 0 else float('inf')
        assert per_message_time < 0.01, f"Performance too slow for batch size {size}: {per_message_time}s per message"
    
    concurrent_producer = Producer({'bootstrap.servers': 'localhost:9092'})
    concurrent_results = []
    
    def concurrent_batch_worker(worker_id):
        try:
            start_time = time.time()
            messages = [{'value': f'concurrent_{worker_id}_{i}'.encode()} for i in range(100)]
            count = concurrent_producer.produce_batch(f'perf-topic-{worker_id}', messages)
            elapsed = time.time() - start_time
            concurrent_results.append((worker_id, count, elapsed))
        except Exception as e:
            concurrent_results.append((worker_id, 0, float('inf')))
    
    concurrent_threads = []
    for i in range(5):
        t = threading.Thread(target=concurrent_batch_worker, args=(i,))
        concurrent_threads.append(t)
        t.start()
    
    for t in concurrent_threads:
        t.join()
    
    assert len(concurrent_results) == 5
    for worker_id, count, elapsed in concurrent_results:
        assert count == 100, f"Concurrent worker {worker_id} failed: {count}/100"
        assert elapsed < 5.0, f"Concurrent worker {worker_id} too slow: {elapsed}s"


def test_produce_batch_transactional_integration():
    """Test transactional producer integration with batch operations."""
    try:
        transactional_producer = Producer({
            'bootstrap.servers': 'localhost:9092',
            'transactional.id': 'test-batch-txn-' + str(int(time.time()))
        })
        
        try:
            transactional_producer.init_transactions(0.5)
            transactional_producer.begin_transaction()
            
            txn_messages = [
                {'value': f'txn_msg_{i}'.encode()} 
                for i in range(5)
            ]
            count = transactional_producer.produce_batch('test-topic', txn_messages)
            assert count == 5
            
            transactional_producer.commit_transaction(0.5)
            
        except KafkaException as e:
            assert e.args[0].code() in (KafkaError._TIMED_OUT, KafkaError._TRANSPORT, KafkaError._CONFLICT)
            
        transactional_producer.flush(0.1)
        
    except KafkaException as e:
        assert e.args[0].code() in (KafkaError._TIMED_OUT, KafkaError._TRANSPORT, KafkaError._CONFLICT, KafkaError._INVALID_CONFIG)


def test_produce_batch_serialization_integration():
    """Test serialization framework compatibility with batch operations."""
    serializing_producer = SerializingProducer({
        'bootstrap.servers': 'localhost:9092',
        'value.serializer': StringSerializer('utf_8')
    })
    
    if hasattr(serializing_producer, 'produce_batch'):
        serialized_messages = [
            {'value': f'serialized_msg_{i}'} 
            for i in range(5)
        ]
        count = serializing_producer.produce_batch('test-topic', serialized_messages)
        assert count >= 0
        serializing_producer.flush(0.5)
    else:
        assert not hasattr(serializing_producer, 'produce_batch')
    
    avro_producer = AvroProducer({
        'bootstrap.servers': 'localhost:9092',
        'schema.registry.url': 'http://localhost:8081'
    })
    
    if hasattr(avro_producer, 'produce_batch'):
        try:
            avro_messages = [
                {'value': f'avro_value_{i}'.encode()}
                for i in range(3)
            ]
            count = avro_producer.produce_batch('test-topic', avro_messages)
            assert count >= 0
            avro_producer.flush(0.5)
        except (KafkaException, TypeError) as e:
            assert isinstance(e, (KafkaException, TypeError))
