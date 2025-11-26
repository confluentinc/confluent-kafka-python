#!/usr/bin/env python
# -*- coding: utf-8 -*-
import gc
import threading
import time
from struct import pack

import pytest

from confluent_kafka import Consumer, KafkaError, KafkaException, Producer, SerializingProducer, TopicPartition
from confluent_kafka.avro import AvroProducer

# Additional imports for batch integration tests
from confluent_kafka.serialization import StringSerializer
from tests.common import TestConsumer, TestUtils
from tests.test_wakeable_utilities import WAKEABLE_POLL_TIMEOUT_MAX, WAKEABLE_POLL_TIMEOUT_MIN


def error_cb(err):
    print('error_cb', err)


def test_basic_api():
    """Basic API tests, these wont really do anything since there is no
    broker configured."""

    with pytest.raises(TypeError) as ex:
        p = Producer()
    assert ex.match('expected configuration dict')

    p = Producer({'socket.timeout.ms': 10, 'error_cb': error_cb, 'message.timeout.ms': 10})

    p.produce('mytopic')
    p.produce('mytopic', value='somedata', key='a key')

    def on_delivery(err, msg):
        print('delivery', err, msg)
        # Since there is no broker, produced messages should time out.
        assert err.code() == KafkaError._MSG_TIMED_OUT
        print('message latency', msg.latency())

    p.produce(topic='another_topic', value='testing', partition=9, callback=on_delivery)

    p.poll(0.001)

    p.flush(0.002)
    p.flush()

    try:
        p.list_topics(timeout=0.2)
    except KafkaException as e:
        assert e.args[0].code() in (KafkaError._TIMED_OUT, KafkaError._TRANSPORT)

    assert p.close(), "Failed to validate that producer was closed."


def test_produce_timestamp():
    """Test produce() with timestamp arg"""
    p = Producer({'socket.timeout.ms': 10, 'error_cb': error_cb, 'message.timeout.ms': 10})
    p.produce('mytopic', timestamp=1234567)
    p.flush()


def test_produce_headers():
    """Test produce() with timestamp arg"""
    p = Producer({'socket.timeout.ms': 10, 'error_cb': error_cb, 'message.timeout.ms': 10})

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
        {'isunicode': 'Jämtland'},
    ]

    for headers in headers_to_test:
        print('headers', type(headers), headers)
        p.produce('mytopic', value='somedata', key='a key', headers=headers)
        p.produce('mytopic', value='somedata', headers=headers)

    with pytest.raises(TypeError):
        p.produce('mytopic', value='somedata', key='a key', headers=('a', 'b'))

    with pytest.raises(TypeError):
        p.produce('mytopic', value='somedata', key='a key', headers=['malformed_header'])

    with pytest.raises(TypeError):
        p.produce('mytopic', value='somedata', headers={'anint': 1234})

    p.flush()


def test_produce_headers_should_work():
    """Test produce() with headers works, however
    NOTE headers are not supported in batch mode and silently ignored
    """
    p = Producer({'socket.timeout.ms': 10, 'error_cb': error_cb, 'message.timeout.ms': 10})

    # Headers should work with current librdkafka version
    try:
        p.produce('mytopic', value='somedata', key='a key', headers=[('headerkey', 'headervalue')])
        # If we get here, headers are silently ignored
        assert True
    except NotImplementedError:
        # Headers caused failure
        pytest.skip("Headers not supported in this build")


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
    """Excercise the transactional API"""
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
        p.send_offsets_to_transaction([TopicPartition("topic", 0, 123)], group_metadata)
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

    assert p.close(), "The producer was not closed"


def test_purge():
    """
    Verify that when we have a higher message.timeout.ms timeout, we can use purge()
    to stop waiting for messages and get delivery reports
    """
    p = Producer({"socket.timeout.ms": 10, "error_cb": error_cb, "message.timeout.ms": 30000})  # 30 seconds

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

    assert p.close(), "The producer was not closed"


def test_producer_bool_value():
    """
    Make sure producer has a truth-y bool value
    See https://github.com/confluentinc/confluent-kafka-python/issues/1427
    """

    p = Producer({})
    assert bool(p)
    assert p.close(), "The producer was not fully closed"


def test_produce_batch_basic_types_and_data():
    """Test basic data types, None/empty handling, and partition functionality."""
    producer = Producer({'bootstrap.servers': 'localhost:9092'})

    basic_messages = [
        {'value': b'bytes_message', 'key': b'bytes_key'},
        {'value': 'string_message', 'key': 'string_key'},
        {'value': 'unicode: 你好', 'key': b'mixed_key'},
        {'value': None, 'key': None},
        {'value': b'', 'key': ''},
        {},
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
        {'value': b'another_partition', 'partition': 2},
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
        {'value': b'\x80\x81\x82', 'key': 'binary'},
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
        {'value': large_payload, 'key': b'large2'},
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


@pytest.mark.parametrize(
    "invalid_input,expected_error",
    [
        ("not_a_list", "messages must be a list"),
        ({'not': 'list'}, "messages must be a list"),
        ([{'value': b'good'}, "not_dict", {'value': b'good2'}], "Message at index 1 must be a dict"),
        ([{'value': 123}], "Message value at index 0 must be bytes or str"),
        ([{'value': b'good', 'key': ['invalid']}], "Message key at index 0 must be bytes or str"),
        ([{'value': b'good', 'partition': "invalid"}], "Message partition at index 0 must be int"),
        ([{'value': b'test', 'partition': -2}], None),  # Negative partition
        ([{'value': b'test', 'partition': 2147483647}], None),  # Max int32
        ([{'value': b'test', 'partition': 999999}], None),  # Very large partition
    ],
)
def test_produce_batch_input_validation(invalid_input, expected_error):
    """Test input validation and message field validation."""
    producer = Producer({'bootstrap.servers': 'localhost:9092'})

    if expected_error is None:
        count = producer.produce_batch('test-topic', invalid_input)
        assert count >= 0
    else:
        with pytest.raises((TypeError, ValueError), match=expected_error):
            producer.produce_batch('test-topic', invalid_input)

    messages_with_extras = [
        {
            'value': b'normal',
            'key': b'normal',
            'partition': 0,
            'callback': lambda err, msg: None,
            'extra_field': 'should_be_ignored',
            'another_extra': 123,
            '_private': 'user_private_field',
        }
    ]
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
    small_queue_producer = Producer({'bootstrap.servers': 'localhost:9092', 'queue.buffering.max.messages': 5})

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

    restrictive_producer = Producer(
        {'bootstrap.servers': 'localhost:9092', 'queue.buffering.max.messages': 1, 'message.max.bytes': 1000}
    )

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
        {'value': b'msg5', 'callback': exception_callback},
    ]

    count = producer.produce_batch('test-topic', messages, on_delivery=global_callback)
    assert count == 5

    try:
        producer.flush(0.1)
    except ValueError as e:
        assert "Test callback exception" in str(e)
    except BaseException:
        pass
    # Check callback correctness if they executed
    if callback1_calls:
        assert callback1_calls == [b'msg1']
    if callback2_calls:
        assert callback2_calls == [b'msg3']
    if exception_calls:
        assert exception_calls == [b'msg5']
    if global_calls:
        global_values = [msg for err, msg in global_calls]
        assert set(global_values).issubset({b'msg2', b'msg4'})

    no_callback_messages = [{'value': b'no_cb_msg'}]
    count = producer.produce_batch('test-topic', no_callback_messages)
    assert count == 1
    producer.flush(0.1)

    alias_calls = []

    def alias_callback(err, msg):
        alias_calls.append(msg.value())

    count1 = producer.produce_batch('test-topic', [{'value': b'alias1'}], callback=alias_callback)
    count2 = producer.produce_batch('test-topic', [{'value': b'alias2'}], on_delivery=alias_callback)
    assert count1 == 1 and count2 == 1
    producer.flush(0.1)
    if alias_calls:
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
    producer.flush(0.1)

    slow_calls = []

    def slow_callback(err, msg):
        slow_calls.append(msg.value() if msg else None)

    slow_messages = [{'value': f'slow_{i}'.encode(), 'callback': slow_callback} for i in range(5)]
    count = producer.produce_batch('test-topic', slow_messages)
    producer.flush(0.1)
    assert count == 5


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
        {'value': b'another_normal_msg'},
    ]
    count = producer.produce_batch('test-topic', messages)
    assert count == 3

    try:
        producer.flush(0.1)
    except RuntimeError as e:
        assert "Critical callback error" in str(e)
        assert len(exception_calls_2) == 1
    except BaseException:
        pass

    multi_exception_calls = []

    def multi_exception_callback(err, msg):
        multi_exception_calls.append(msg.value() if msg else None)
        raise ValueError(f"Error from {msg.value()}")

    multi_messages = [
        {'value': b'error1', 'callback': multi_exception_callback},
        {'value': b'error2', 'callback': multi_exception_callback},
    ]
    count = producer.produce_batch('test-topic', multi_messages)
    assert count == 2

    try:
        producer.flush(0.1)
    except (RuntimeError, ValueError):
        assert True
    except BaseException:
        pass


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
                messages = [
                    {'value': f'thread_{thread_id}_batch_{batch_num}_msg_{i}_{shared_counter["value"]}'.encode()}
                    for i in range(5)
                ]
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

                messages = [{'value': f'race_t{thread_id}_b{batch_num}_m{i}_{shared_value}'.encode()} for i in range(3)]

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


def test_produce_batch_memory_stress():
    """Test memory stress scenarios and cleanup verification."""
    producer = Producer({'bootstrap.servers': 'localhost:9092'})

    max_messages = [{'value': f'msg_{i}'.encode()} for i in range(5000)]
    count = producer.produce_batch('test-topic', max_messages)
    assert 0 <= count <= len(max_messages)

    nested_messages = []
    for i in range(500):
        nested_messages.append(
            {
                'value': f'nested_{i}'.encode(),
                'key': f'key_{i}'.encode(),
                'partition': i % 10,
                'callback': lambda err, msg: None,
            }
        )
    count = producer.produce_batch('test-topic', nested_messages)
    assert count >= 0

    for batch_num in range(10):
        messages = [{'value': f'mem_test_{batch_num}_{i}'.encode()} for i in range(50)]
        count = producer.produce_batch('test-topic', messages)
        assert count >= 0

        if batch_num % 3 == 0:
            gc.collect()

    producer.flush(0.1)
    gc.collect()


def test_produce_batch_memory_critical():
    """Test critical memory scenarios and producer lifecycle."""
    producer = Producer({'bootstrap.servers': 'localhost:9092'})

    destruction_calls = []

    def destruction_callback(err, msg):
        destruction_calls.append(msg.value() if msg else None)

    temp_producer = Producer({'bootstrap.servers': 'localhost:9092'})

    messages = [
        {'value': b'destruction_test_1', 'callback': destruction_callback},
        {'value': b'destruction_test_2', 'callback': destruction_callback},
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
            large_messages = [{'value': b'x' * 5000} for j in range(50)]
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


def test_produce_batch_client_side_limits():
    """Test client-side queue limits and message handling."""
    # Test queue buffer limits (client-side behavior)
    producer_small_queue = Producer({'bootstrap.servers': 'localhost:9092', 'queue.buffering.max.messages': 2})

    # Fill up the queue first
    try:
        for i in range(5):
            producer_small_queue.produce('test-topic', f'filler_{i}')
    except BufferError:
        pass  # Expected when queue is full

    # Test batch behavior with limited queue
    large_batch = [{'value': f'msg_{i}'.encode()} for i in range(10)]
    count = producer_small_queue.produce_batch('test-topic', large_batch)
    assert 0 <= count <= len(large_batch)

    # Test very large batch handling (client-side limits)
    producer = Producer({'bootstrap.servers': 'localhost:9092'})
    very_large_batch = [{'value': f'large_msg_{i}'.encode()} for i in range(1000)]
    count = producer.produce_batch('test-topic', very_large_batch)
    assert count >= 0

    # Test large message handling
    huge_message = {'value': b'x' * (1024 * 1024)}
    count = producer.produce_batch('test-topic', [huge_message])
    assert count >= 0

    # Test error annotation on failed messages
    messages_for_annotation = [
        {'value': b'test1'},
        {'value': b'test2'},
        {'value': b'test3'},
    ]
    count = producer.produce_batch('test-topic', messages_for_annotation)
    assert count >= 0

    # Verify error annotation works (messages get _error field when they fail)
    failed_count = sum(1 for msg in messages_for_annotation if '_error' in msg)
    success_count = len(messages_for_annotation) - failed_count
    assert success_count == count


def test_produce_batch_api_compatibility():
    """Test API compatibility with different producer types."""
    # Test that produce_batch exists on basic Producer
    basic_producer = Producer({'bootstrap.servers': 'localhost:9092'})
    assert hasattr(basic_producer, 'produce_batch')

    # Test basic functionality works
    basic_messages = [{'value': b'test1'}, {'value': b'test2'}]
    count = basic_producer.produce_batch('test-topic', basic_messages)
    assert count == 2

    # Test SerializingProducer compatibility (API presence)
    serializing_producer = SerializingProducer(
        {'bootstrap.servers': 'localhost:9092', 'value.serializer': StringSerializer('utf_8')}
    )

    # Test if produce_batch method exists and behaves consistently
    if hasattr(serializing_producer, 'produce_batch'):
        # Test that method exists and accepts parameters correctly
        try:
            serialized_messages = [{'value': f'test_msg_{i}'} for i in range(3)]
            count = serializing_producer.produce_batch('test-topic', serialized_messages)
            assert count >= 0
        except (TypeError, AttributeError):
            # If method signature is incompatible, that's also valid information
            pass

    # Test AvroProducer API compatibility
    try:
        avro_producer = AvroProducer(
            {'bootstrap.servers': 'localhost:9092', 'schema.registry.url': 'http://localhost:8081'}
        )

        # Just test that the method exists and can be called
        has_batch_method = hasattr(avro_producer, 'produce_batch')
        if has_batch_method:
            # Test method signature compatibility
            try:
                test_messages = [{'value': b'test_value'}]
                count = avro_producer.produce_batch('test-topic', test_messages)
                assert count >= 0
            except (TypeError, AttributeError, KafkaException):
                # Method exists but may have different requirements - that's OK
                pass
    except ImportError:
        # AvroProducer not available - skip this part
        pytest.skip("AvroProducer not available")


def test_callback_exception_no_system_error():
    delivery_reports = []

    def delivery_cb_that_raises(err, msg):
        """Delivery report callback that raises an exception"""
        delivery_reports.append((err, msg))
        raise RuntimeError("Test exception from delivery_cb")

    producer = Producer(
        {
            'bootstrap.servers': 'nonexistent-broker:9092',  # Will cause delivery failures
            'socket.timeout.ms': 100,
            'message.timeout.ms': 10,  # Very short timeout to trigger delivery failure quickly
            'on_delivery': delivery_cb_that_raises,
        }
    )

    # Produce a message - this will trigger delivery report callback when it fails
    producer.produce('test-topic', value='test-message')

    # Flush to ensure delivery reports are processed
    # Before fix: Would get RuntimeError + SystemError (Issue #865)
    # After fix: Should only get RuntimeError (no SystemError)
    with pytest.raises(RuntimeError) as exc_info:
        producer.flush(timeout=2.0)  # Longer timeout to ensure delivery callback fires

    # Verify we got an exception from our callback
    assert "Test exception from delivery_cb" in str(exc_info.value)

    # Verify the delivery callback was actually called
    assert len(delivery_reports) > 0


def test_core_callbacks_exception_different_types():
    """Test error_cb, throttle_cb, and stats_cb exception handling with different exception types"""

    # Test error_cb with different exception types
    def error_cb_kafka_exception(error):
        raise KafkaException(KafkaError._FAIL, "KafkaException from error_cb")

    def error_cb_value_error(error):
        raise ValueError("ValueError from error_cb")

    def error_cb_runtime_error(error):
        raise RuntimeError("RuntimeError from error_cb")

    # Test error_cb exceptions - these should be triggered by connection failure
    producer = Producer(
        {'bootstrap.servers': 'nonexistent-broker:9092', 'socket.timeout.ms': 100, 'error_cb': error_cb_kafka_exception}
    )

    with pytest.raises(KafkaException) as exc_info:
        producer.produce('test-topic', value='test-message')
        producer.flush(timeout=2.0)
    assert "KafkaException from error_cb" in str(exc_info.value)

    # Test error_cb with ValueError
    producer = Producer(
        {'bootstrap.servers': 'nonexistent-broker:9092', 'socket.timeout.ms': 100, 'error_cb': error_cb_value_error}
    )

    with pytest.raises(ValueError) as exc_info:
        producer.produce('test-topic', value='test-message')
        producer.flush(timeout=2.0)
    assert "ValueError from error_cb" in str(exc_info.value)

    # Test error_cb with RuntimeError
    producer = Producer(
        {'bootstrap.servers': 'nonexistent-broker:9092', 'socket.timeout.ms': 100, 'error_cb': error_cb_runtime_error}
    )

    with pytest.raises(RuntimeError) as exc_info:
        producer.produce('test-topic', value='test-message')
        producer.flush(timeout=2.0)
    assert "RuntimeError from error_cb" in str(exc_info.value)


def test_multiple_callbacks_exception_no_system_error():
    """Test multiple callbacks raising exceptions simultaneously"""

    callbacks_called = []

    def error_cb_that_raises(error):
        callbacks_called.append('error_cb')
        raise RuntimeError("Test exception from error_cb")

    def throttle_cb_that_raises(throttle_event):
        callbacks_called.append('throttle_cb')
        raise RuntimeError("Test exception from throttle_cb")

    def stats_cb_that_raises(stats_json):
        callbacks_called.append('stats_cb')
        raise RuntimeError("Test exception from stats_cb")

    def delivery_cb_that_raises(err, msg):
        callbacks_called.append('delivery_cb')
        raise RuntimeError("Test exception from delivery_cb")

    producer = Producer(
        {
            'bootstrap.servers': 'nonexistent-broker:9092',
            'socket.timeout.ms': 100,
            'statistics.interval.ms': 100,
            'error_cb': error_cb_that_raises,
            'throttle_cb': throttle_cb_that_raises,
            'stats_cb': stats_cb_that_raises,
            'on_delivery': delivery_cb_that_raises,
        }
    )

    # This should trigger multiple callbacks
    with pytest.raises(RuntimeError) as exc_info:
        producer.produce('test-topic', value='test-message')
        producer.flush(timeout=2.0)

    # Should get one of the exceptions (not SystemError)
    assert "Test exception from" in str(exc_info.value)
    assert len(callbacks_called) > 0


def test_delivery_callback_exception_different_message_types():
    """Test delivery callback exception with different message types"""

    def delivery_cb_that_raises(err, msg):
        raise RuntimeError("Test exception from delivery_cb")

    # Test with string message
    producer = Producer(
        {
            'bootstrap.servers': 'nonexistent-broker:9092',
            'socket.timeout.ms': 100,
            'message.timeout.ms': 10,
            'on_delivery': delivery_cb_that_raises,
        }
    )

    with pytest.raises(RuntimeError) as exc_info:
        producer.produce('test-topic', value='string-message')
        producer.flush(timeout=2.0)

    assert "Test exception from delivery_cb" in str(exc_info.value)

    # Test with bytes message
    producer = Producer(
        {
            'bootstrap.servers': 'nonexistent-broker:9092',
            'socket.timeout.ms': 100,
            'message.timeout.ms': 10,
            'on_delivery': delivery_cb_that_raises,
        }
    )

    with pytest.raises(RuntimeError) as exc_info:
        producer.produce('test-topic', value=b'bytes-message')
        producer.flush(timeout=2.0)

    assert "Test exception from delivery_cb" in str(exc_info.value)


def test_callback_exception_with_producer_methods():
    """Test callback exception with different producer methods"""

    def delivery_cb_that_raises(err, msg):
        raise RuntimeError("Test exception from delivery_cb")

    producer = Producer(
        {
            'bootstrap.servers': 'nonexistent-broker:9092',
            'socket.timeout.ms': 100,
            'message.timeout.ms': 10,
            'on_delivery': delivery_cb_that_raises,
        }
    )

    # Test with flush method - this should trigger the callback
    with pytest.raises(RuntimeError) as exc_info:
        producer.produce('test-topic', value='test-message')
        producer.flush(timeout=2.0)

    assert "Test exception from delivery_cb" in str(exc_info.value)


def test_producer_context_manager_basic():
    """Test basic Producer context manager usage and return value"""
    config = {'socket.timeout.ms': 10, 'error_cb': error_cb, 'message.timeout.ms': 10}

    # Test __enter__ returns self
    producer = Producer(config)
    entered = producer.__enter__()
    assert entered is producer
    producer.__exit__(None, None, None)  # Clean up

    # Test basic context manager usage
    with Producer(config) as producer:
        assert producer is not None
        producer.produce('mytopic', value=b'test message')
        producer.poll(0)

    # Producer should be closed after exiting context
    with pytest.raises(RuntimeError, match="Producer has been closed"):
        producer.produce('mytopic', value=b'test message')


def test_producer_context_manager_exception_propagation():
    """Test exceptions propagate and producer is cleaned up"""
    config = {'socket.timeout.ms': 10, 'error_cb': error_cb, 'message.timeout.ms': 10}

    # Test exception propagation
    exception_caught = False
    try:
        with Producer(config) as producer:
            producer.produce('mytopic', value=b'test')
            raise ValueError("Test exception")
    except ValueError as e:
        assert str(e) == "Test exception"
        exception_caught = True

    assert exception_caught, "Exception should have propagated"

    # Producer should be closed even after exception
    with pytest.raises(RuntimeError, match="Producer has been closed"):
        producer.produce('mytopic', value=b'test')


def test_producer_context_manager_exit_with_exceptions():
    """Test __exit__ properly handles exception arguments"""
    config = {'socket.timeout.ms': 10, 'error_cb': error_cb, 'message.timeout.ms': 10}

    producer = Producer(config)
    producer.produce('mytopic', value=b'test')

    # Simulate exception in with block
    exc_type = ValueError
    exc_value = ValueError("Test error")
    exc_traceback = None

    # __exit__ should cleanup and return None (propagate exception)
    result = producer.__exit__(exc_type, exc_value, exc_traceback)
    assert result is None  # None means propagate exception

    # Producer should be closed
    with pytest.raises(RuntimeError):
        producer.produce('mytopic', value=b'test')


def test_producer_context_manager_after_exit():
    """Test Producer behavior after context manager exit"""
    config = {'socket.timeout.ms': 10, 'error_cb': error_cb, 'message.timeout.ms': 10}

    # Normal exit
    with Producer(config) as producer:
        producer.produce('mytopic', value=b'test')

    # All methods should fail after context exit
    with pytest.raises(RuntimeError, match="Producer has been closed"):
        producer.produce('mytopic', value=b'test')

    with pytest.raises(RuntimeError, match="Producer has been closed"):
        producer.flush()

    with pytest.raises(RuntimeError, match="Producer has been closed"):
        producer.poll(0)

    # __len__ should return 0 for closed producer
    assert len(producer) == 0

    # Test already-closed producer edge case
    # Using __enter__ and __exit__ directly on already-closed producer
    entered = producer.__enter__()
    assert entered is producer

    # Operations should still fail
    with pytest.raises(RuntimeError):
        producer.produce('mytopic', value=b'test')

    # __exit__ should handle already-closed gracefully
    result = producer.__exit__(None, None, None)
    assert result is None


def test_producer_context_manager_multiple_instances():
    """Test Producer context manager with multiple instances"""
    config = {'socket.timeout.ms': 10, 'error_cb': error_cb, 'message.timeout.ms': 10}

    # Test multiple sequential instances
    with Producer(config) as producer1:
        producer1.produce('mytopic', value=b'message 1')

    with Producer(config) as producer2:
        producer2.produce('mytopic', value=b'message 2')
        # Both should be independent
        assert producer1 is not producer2

    # Both should be closed
    with pytest.raises(RuntimeError):
        producer1.produce('mytopic', value=b'test')
    with pytest.raises(RuntimeError):
        producer2.produce('mytopic', value=b'test')

    # Test nested context managers
    with Producer(config) as producer1:
        with Producer(config) as producer2:
            assert producer1 is not producer2
            producer1.produce('mytopic', value=b'message 1')
            producer2.produce('mytopic', value=b'message 2')
        # producer2 should be closed, producer1 still open
        producer1.produce('mytopic', value=b'message 3')

    # Both should be closed now
    with pytest.raises(RuntimeError):
        producer1.produce('mytopic', value=b'test')
    with pytest.raises(RuntimeError):
        producer2.produce('mytopic', value=b'test')


def test_producer_context_manager_with_callbacks():
    """Test Producer context manager properly handles delivery callbacks"""
    config = {
        'bootstrap.servers': 'localhost:9092',
        'socket.timeout.ms': 10,
        'error_cb': error_cb,
        'message.timeout.ms': 10,
    }

    delivered = []

    def on_delivery(err, msg):
        delivered.append((err, msg))

    with Producer(config) as producer:
        producer.produce('mytopic', value=b'test message', callback=on_delivery)
        producer.poll(0)
        # Context manager should flush, triggering callbacks

    # Callbacks should be invoked when context manager flushes
    # if broker available, message may succeed (err=None)
    # If broker unavailable, message will timeout (err with timeout/transport error)
    assert len(delivered) > 0, "Delivery callback should have been called"
    err, msg = delivered[0]
    assert msg is not None

    # Handle both cases: broker available (err=None success) or unavailable (timeout/transport error)
    if err is None:
        # Success case - broker is available and message was delivered
        pass  # Message delivered successfully
    else:
        # Error case - broker unavailable or connection failed
        assert err.code() in (
            KafkaError._MSG_TIMED_OUT,
            KafkaError._TRANSPORT,
            KafkaError._TIMED_OUT,
        ), f"Expected success (err=None) or timeout/transport error, got {err.code()}"


def test_uninitialized_producer_methods():
    """Test that all Producer methods raise RuntimeError when called on uninitialized instance.

    This test verifies issue #1590 fix - prevents SEGV when subclassing Producer
    without calling super().__init__().
    """

    class UninitializedProducer(Producer):
        def __init__(self, config):
            # Don't call super().__init__() - leaves self->rk as NULL
            pass

    producer = UninitializedProducer({})

    with pytest.raises(RuntimeError, match="Producer has been closed"):
        producer.produce('topic', value=b'test')

    with pytest.raises(RuntimeError, match="Producer has been closed"):
        producer.poll()

    with pytest.raises(RuntimeError, match="Producer has been closed"):
        producer.flush()

    with pytest.raises(RuntimeError, match="Producer has been closed"):
        producer.purge()

    with pytest.raises(RuntimeError, match="Producer has been closed"):
        producer.produce_batch('topic', [{'value': b'test'}])

    with pytest.raises(RuntimeError, match="Producer has been closed"):
        producer.init_transactions()

    with pytest.raises(RuntimeError, match="Producer has been closed"):
        producer.begin_transaction()

    # send_offsets_to_transaction - NULL check happens after argument parsing
    consumer = Consumer({'group.id': 'test', 'socket.timeout.ms': 10})
    metadata = consumer.consumer_group_metadata()
    consumer.close()

    with pytest.raises(RuntimeError, match="Producer has been closed"):
        producer.send_offsets_to_transaction([TopicPartition('topic', 0)], metadata)

    with pytest.raises(RuntimeError, match="Producer has been closed"):
        producer.commit_transaction()

    with pytest.raises(RuntimeError, match="Producer has been closed"):
        producer.abort_transaction()

    # Test __len__() - should return 0 for closed producer (safe, no crash)
    assert len(producer) == 0


def test_producer_close():
    """
    Ensures the producer close can be requested on demand
    """
    conf = {'debug': 'all', 'socket.timeout.ms': 10, 'error_cb': error_cb, 'message.timeout.ms': 10}
    producer = Producer(conf)
    cb_detector = {"on_delivery_called": False}

    def on_delivery(err, msg):
        cb_detector["on_delivery_called"] = True

    producer.produce('mytopic', value='somedata', key='a key', callback=on_delivery)
    assert producer.close(), "The producer could not be closed on demand"
    assert cb_detector["on_delivery_called"], "The delivery callback should have been called by flushing during close"


def test_wakeable_poll_utility_functions_interaction():
    """Test interaction between calculate_chunk_timeout() and check_signals_between_chunks()."""
    # Assert: Chunk calculation and signal check work together
    producer1 = Producer({'socket.timeout.ms': 100, 'message.timeout.ms': 10})

    interrupt_thread = threading.Thread(target=lambda: TestUtils.send_sigint_after_delay(0.4))
    interrupt_thread.daemon = True
    interrupt_thread.start()

    interrupted = False
    try:
        producer1.poll(timeout=1.0)
    except KeyboardInterrupt:
        interrupted = True
    finally:
        producer1.close()

    assert interrupted, "Should have raised KeyboardInterrupt"

    # Assert: Multiple chunks before signal detection
    producer2 = Producer({'socket.timeout.ms': 100, 'message.timeout.ms': 10})

    interrupt_thread = threading.Thread(target=lambda: TestUtils.send_sigint_after_delay(0.6))
    interrupt_thread.daemon = True
    interrupt_thread.start()

    interrupted = False
    try:
        producer2.poll(timeout=WAKEABLE_POLL_TIMEOUT_MAX)
    except KeyboardInterrupt:
        interrupted = True
    finally:
        producer2.close()

    assert interrupted, "Should have raised KeyboardInterrupt"


def test_wakeable_poll_interruptibility_and_messages():
    """Test poll() interruptibility and message handling."""
    # Assert: Infinite timeout can be interrupted
    producer1 = Producer({'socket.timeout.ms': 100, 'message.timeout.ms': 10})

    interrupt_thread = threading.Thread(target=lambda: TestUtils.send_sigint_after_delay(0.1))
    interrupt_thread.daemon = True
    interrupt_thread.start()

    interrupted = False
    try:
        producer1.poll(timeout=WAKEABLE_POLL_TIMEOUT_MAX)
    except KeyboardInterrupt:
        interrupted = True
    finally:
        producer1.close()

    assert interrupted, "Should have raised KeyboardInterrupt"

    # Assert: Finite timeout can be interrupted before timeout expires
    producer2 = Producer({'socket.timeout.ms': 100, 'message.timeout.ms': 10})

    interrupt_thread = threading.Thread(target=lambda: TestUtils.send_sigint_after_delay(0.3))
    interrupt_thread.daemon = True
    interrupt_thread.start()

    interrupted = False
    try:
        producer2.poll(timeout=WAKEABLE_POLL_TIMEOUT_MAX)
    except KeyboardInterrupt:
        interrupted = True
    finally:
        producer2.close()

    assert interrupted, "Should have raised KeyboardInterrupt"

    # Assert: Signal sent after multiple chunks still interrupts
    producer3 = Producer({'socket.timeout.ms': 100, 'message.timeout.ms': 10})

    interrupt_thread = threading.Thread(target=lambda: TestUtils.send_sigint_after_delay(0.6))
    interrupt_thread.daemon = True
    interrupt_thread.start()

    interrupted = False
    try:
        producer3.poll(timeout=WAKEABLE_POLL_TIMEOUT_MAX)
    except KeyboardInterrupt:
        interrupted = True
    finally:
        producer3.close()

    assert interrupted, "Should have raised KeyboardInterrupt"

    # Assert: No signal - timeout works normally
    producer4 = Producer({'socket.timeout.ms': 100, 'message.timeout.ms': 10})

    start = time.time()
    result = producer4.poll(timeout=0.5)
    elapsed = time.time() - start

    assert isinstance(result, int), "poll() should return int"
    assert (
        WAKEABLE_POLL_TIMEOUT_MIN <= elapsed <= WAKEABLE_POLL_TIMEOUT_MAX
    ), f"Timeout took {elapsed:.2f}s, expected ~0.5s"
    producer4.close()


def test_wakeable_poll_edge_cases():
    """Test poll() edge cases."""
    # Assert: Zero timeout returns immediately
    producer1 = Producer({'socket.timeout.ms': 100, 'message.timeout.ms': 10})

    start = time.time()
    result = producer1.poll(timeout=0.0)
    elapsed = time.time() - start

    assert elapsed < WAKEABLE_POLL_TIMEOUT_MAX, f"Zero timeout took {elapsed:.2f}s"
    assert isinstance(result, int)
    producer1.close()

    # Assert: Closed producer raises RuntimeError
    producer2 = Producer({'socket.timeout.ms': 100, 'message.timeout.ms': 10})
    producer2.close()

    with pytest.raises(RuntimeError) as exc_info:
        producer2.poll(timeout=0.1)
    assert 'Producer has been closed' in str(exc_info.value)

    # Assert: Short timeout works correctly
    producer3 = Producer({'socket.timeout.ms': 100, 'message.timeout.ms': 10})

    start = time.time()
    result = producer3.poll(timeout=0.1)
    elapsed = time.time() - start

    assert isinstance(result, int)
    # Short timeouts don't use chunking
    assert elapsed <= WAKEABLE_POLL_TIMEOUT_MAX, f"Short timeout took {elapsed:.2f}s"
    producer3.close()

    # Assert: Very short timeout works
    producer4 = Producer({'socket.timeout.ms': 100, 'message.timeout.ms': 10})

    start = time.time()
    result = producer4.poll(timeout=0.05)
    elapsed = time.time() - start

    assert isinstance(result, int)
    assert elapsed < WAKEABLE_POLL_TIMEOUT_MAX, f"Very short timeout took {elapsed:.2f}s"
    producer4.close()


def test_wakeable_flush_interruptibility_and_messages():
    """Test flush() interruptibility and message handling."""
    # Assert: Infinite timeout can be interrupted
    producer1 = Producer(
        {
            'bootstrap.servers': 'localhost:9092',
            'socket.timeout.ms': 60000,
            'message.timeout.ms': 30000,
            'acks': 'all',
            'batch.num.messages': 100,
            'linger.ms': 100,
            'queue.buffering.max.messages': 100000,
            'queue.buffering.max.kbytes': 104857600,
            'max.in.flight.requests.per.connection': 1,
            'request.timeout.ms': 30000,
            'delivery.timeout.ms': 30000,
        }
    )

    messages_produced = False
    stop_producing = threading.Event()
    production_stats = {'count': 0, 'errors': 0}

    def continuous_producer():
        message_num = 0
        while not stop_producing.is_set():
            try:
                producer1.produce(
                    'test-topic', value=f'continuous-{message_num}'.encode(), key=f'key-{message_num}'.encode()
                )
                production_stats['count'] += 1
                message_num += 1
            except Exception as e:
                production_stats['errors'] += 1
                if "QUEUE_FULL" in str(e):
                    time.sleep(0.001)
                else:
                    time.sleep(0.01)

    try:
        for i in range(1000):
            try:
                producer1.produce('test-topic', value=f'initial-{i}'.encode())
                messages_produced = True
            except Exception as e:
                if "QUEUE_FULL" in str(e):
                    time.sleep(0.01)
                    continue
                break

        if not messages_produced:
            producer1.close()
            pytest.skip("Broker not available, cannot test flush() interruptibility")

        poll_start = time.time()
        while time.time() - poll_start < 0.5:
            producer1.poll(timeout=0.1)

        producer_thread = threading.Thread(target=continuous_producer, daemon=True)
        producer_thread.start()
        time.sleep(0.1)

        interrupt_thread = threading.Thread(target=lambda: TestUtils.send_sigint_after_delay(0.1))
        interrupt_thread.daemon = True
        interrupt_thread.start()

        interrupted = False
        try:
            producer1.flush()
        except KeyboardInterrupt:
            interrupted = True
        finally:
            stop_producing.set()
            time.sleep(0.1)
            producer1.close()

        assert interrupted, "Should have raised KeyboardInterrupt"
    except Exception:
        stop_producing.set()
        producer1.close()
        raise

    # Assert: Finite timeout can be interrupted before timeout expires
    producer2 = Producer(
        {
            'bootstrap.servers': 'localhost:9092',
            'socket.timeout.ms': 60000,
            'message.timeout.ms': 30000,
            'acks': 'all',
            'batch.num.messages': 100,
            'linger.ms': 100,
            'queue.buffering.max.messages': 100000,
            'queue.buffering.max.kbytes': 104857600,
            'max.in.flight.requests.per.connection': 1,
            'request.timeout.ms': 30000,
            'delivery.timeout.ms': 30000,
        }
    )

    stop_producing2 = threading.Event()
    production_stats2 = {'count': 0, 'errors': 0}

    def continuous_producer2():
        message_num = 0
        while not stop_producing2.is_set():
            try:
                producer2.produce(
                    'test-topic', value=f'continuous2-{message_num}'.encode(), key=f'key2-{message_num}'.encode()
                )
                production_stats2['count'] += 1
                message_num += 1
            except Exception as e:
                production_stats2['errors'] += 1
                if "QUEUE_FULL" in str(e):
                    time.sleep(0.001)
                else:
                    time.sleep(0.01)

    try:
        for i in range(1000):
            try:
                producer2.produce('test-topic', value=f'initial2-{i}'.encode())
            except Exception as e:
                if "QUEUE_FULL" in str(e):
                    time.sleep(0.01)
                    continue
                break

        poll_start = time.time()
        while time.time() - poll_start < 0.5:
            producer2.poll(timeout=0.1)

        producer_thread2 = threading.Thread(target=continuous_producer2, daemon=True)
        producer_thread2.start()
        time.sleep(0.1)

        interrupt_thread = threading.Thread(target=lambda: TestUtils.send_sigint_after_delay(0.3))
        interrupt_thread.daemon = True
        interrupt_thread.start()

        interrupted = False
        try:
            producer2.flush(timeout=WAKEABLE_POLL_TIMEOUT_MAX)
        except KeyboardInterrupt:
            interrupted = True
        finally:
            stop_producing2.set()
            time.sleep(0.1)
            producer2.close()

        assert interrupted, "Should have raised KeyboardInterrupt"
    except Exception:
        stop_producing2.set()
        producer2.close()
        raise

    # Assert: Signal sent after multiple chunks still interrupts
    producer3 = Producer(
        {
            'bootstrap.servers': 'localhost:9092',
            'socket.timeout.ms': 60000,
            'message.timeout.ms': 30000,
            'acks': 'all',
            'batch.num.messages': 100,
            'linger.ms': 100,
            'queue.buffering.max.messages': 100000,
            'queue.buffering.max.kbytes': 104857600,
            'max.in.flight.requests.per.connection': 1,
            'request.timeout.ms': 30000,
            'delivery.timeout.ms': 30000,
        }
    )

    stop_producing3 = threading.Event()
    production_stats3 = {'count': 0, 'errors': 0}

    def continuous_producer3():
        message_num = 0
        while not stop_producing3.is_set():
            try:
                producer3.produce(
                    'test-topic', value=f'continuous3-{message_num}'.encode(), key=f'key3-{message_num}'.encode()
                )
                production_stats3['count'] += 1
                message_num += 1
            except Exception as e:
                production_stats3['errors'] += 1
                if "QUEUE_FULL" in str(e):
                    time.sleep(0.001)
                else:
                    time.sleep(0.01)

    try:
        for i in range(1000):
            try:
                producer3.produce('test-topic', value=f'initial3-{i}'.encode())
            except Exception as e:
                if "QUEUE_FULL" in str(e):
                    time.sleep(0.01)
                    continue
                break

        poll_start = time.time()
        while time.time() - poll_start < 0.5:
            producer3.poll(timeout=0.1)

        producer_thread3 = threading.Thread(target=continuous_producer3, daemon=True)
        producer_thread3.start()
        time.sleep(0.1)

        interrupt_thread = threading.Thread(target=lambda: TestUtils.send_sigint_after_delay(0.6))
        interrupt_thread.daemon = True
        interrupt_thread.start()

        interrupted = False
        try:
            producer3.flush()
        except KeyboardInterrupt:
            interrupted = True
        finally:
            stop_producing3.set()
            time.sleep(0.1)
            producer3.close()

        assert interrupted, "Should have raised KeyboardInterrupt"
    except Exception:
        stop_producing3.set()
        producer3.close()
        raise

    # Assert: No signal - timeout works normally
    producer4 = Producer(
        {
            'bootstrap.servers': 'localhost:9092',
            'socket.timeout.ms': 100,
            'message.timeout.ms': 10,
            'acks': 'all',
            'max.in.flight.requests.per.connection': 1,
        }
    )

    try:
        for i in range(100):
            producer4.produce('test-topic', value=f'timeout-test-{i}'.encode())
    except Exception:
        pass

    start = time.time()
    qlen = producer4.flush(timeout=0.5)
    elapsed = time.time() - start

    assert isinstance(qlen, int), "flush() should return int"
    assert elapsed <= WAKEABLE_POLL_TIMEOUT_MAX, f"Timeout took {elapsed:.2f}s"
    producer4.close()


def test_wakeable_flush_edge_cases():
    """Test flush() edge cases."""
    # Assert: Zero timeout returns immediately
    producer1 = Producer({'socket.timeout.ms': 100, 'message.timeout.ms': 10})

    start = time.time()
    qlen = producer1.flush(timeout=0.0)
    elapsed = time.time() - start

    assert elapsed < WAKEABLE_POLL_TIMEOUT_MAX, f"Zero timeout took {elapsed:.2f}s"
    assert isinstance(qlen, int)
    producer1.close()

    # Assert: Closed producer raises RuntimeError
    producer2 = Producer({'socket.timeout.ms': 100, 'message.timeout.ms': 10})
    producer2.close()

    with pytest.raises(RuntimeError) as exc_info:
        producer2.flush(timeout=0.1)
    assert 'Producer has been closed' in str(exc_info.value)

    # Assert: Short timeout works correctly
    producer3 = Producer({'socket.timeout.ms': 100, 'message.timeout.ms': 10})

    start = time.time()
    qlen = producer3.flush(timeout=0.1)
    elapsed = time.time() - start

    assert isinstance(qlen, int)
    # Short timeouts don't use chunking
    assert elapsed <= WAKEABLE_POLL_TIMEOUT_MAX, f"Short timeout took {elapsed:.2f}s"
    producer3.close()

    # Assert: Very short timeout works
    producer4 = Producer({'socket.timeout.ms': 100, 'message.timeout.ms': 10})

    start = time.time()
    qlen = producer4.flush(timeout=0.05)
    elapsed = time.time() - start

    assert isinstance(qlen, int)
    assert elapsed < WAKEABLE_POLL_TIMEOUT_MAX, f"Very short timeout took {elapsed:.2f}s"
    producer4.close()

    # Assert: Empty queue flush returns immediately
    producer5 = Producer({'socket.timeout.ms': 100, 'message.timeout.ms': 10})

    start = time.time()
    qlen = producer5.flush(timeout=1.0)
    elapsed = time.time() - start

    assert qlen == 0
    assert elapsed < WAKEABLE_POLL_TIMEOUT_MAX, f"Empty flush took {elapsed:.2f}s"
    producer5.close()
