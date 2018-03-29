#!/usr/bin/env python
import pytest

from confluent_kafka import Producer, KafkaError, KafkaException, libversion


def error_cb(err):
    print('error_cb', err)


def test_basic_api():
    """ Basic API tests, these wont really do anything since there is no
        broker configured. """

    try:
        p = Producer()
    except TypeError as e:
        assert str(e) == "expected configuration dict"

    p = Producer({'socket.timeout.ms': 10,
                  'error_cb': error_cb,
                  'default.topic.config': {'message.timeout.ms': 10}})

    p.produce('mytopic')
    p.produce('mytopic', value='somedata', key='a key')

    def on_delivery(err, msg):
        print('delivery', str)
        # Since there is no broker, produced messages should time out.
        assert err.code() == KafkaError._MSG_TIMED_OUT

    p.produce(topic='another_topic', value='testing', partition=9,
              callback=on_delivery)

    p.poll(0.001)

    p.flush(0.002)
    p.flush()


def test_produce_timestamp():
    """ Test produce() with timestamp arg """
    p = Producer({'socket.timeout.ms': 10,
                  'error_cb': error_cb,
                  'default.topic.config': {'message.timeout.ms': 10}})

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
                  'default.topic.config': {'message.timeout.ms': 10}})

    p.produce('mytopic', value='somedata', key='a key', headers=[('headerkey', 'headervalue')])
    p.produce('mytopic', value='somedata', key='a key', headers=[('dupkey', 'dupvalue'), ('dupkey', 'dupvalue')])
    p.produce('mytopic', value='somedata', key='a key', headers=[('dupkey', 'dupvalue'), ('dupkey', 'diffvalue')])
    p.produce('mytopic', value='somedata', key='a key', headers=[('key_with_null_value', None)])
    p.produce('mytopic', value='somedata', key='a key', headers=[])

    with pytest.raises(TypeError) as ex:
        p.produce('mytopic', value='somedata', key='a key', headers={'my': 'dict'})
    assert 'Headers are expected to be a list of (key,value) tuples' == str(ex.value)

    with pytest.raises(TypeError) as ex:
        p.produce('mytopic', value='somedata', key='a key', headers=[('malformed_header')])
    assert 'Headers are expected to be a list of (key,value) tuples' == str(ex.value)

    p.flush()


# Should be updated to 0.11.4 when it is released
@pytest.mark.skipif(libversion()[1] >= 0x000b0400,
                    reason="Old versions should fail when using headers")
def test_produce_headers_should_fail():
    """ Test produce() with timestamp arg """
    p = Producer({'socket.timeout.ms': 10,
                  'error_cb': error_cb,
                  'default.topic.config': {'message.timeout.ms': 10}})

    with pytest.raises(NotImplementedError) as e:
        p.produce('mytopic', value='somedata', key='a key', headers=[('headerkey', 'headervalue')])
    assert 'Producer message headers requires confluent-kafka-python built for librdkafka version >=v0.11.4' in str(e)


def test_subclassing():
    class SubProducer(Producer):
        def __init__(self, conf, topic):
            super(SubProducer, self).__init__(conf)
            self.topic = topic

        def produce_hi(self):
            super(SubProducer, self).produce(self.topic, value='hi')

    sp = SubProducer(dict(), 'atopic')
    assert type(sp) == SubProducer

    # Invalid config should fail
    try:
        sp = SubProducer({'should.fail': False}, 'mytopic')
    except KafkaException:
        pass

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
    p = Producer({"default.topic.config": {"message.timeout.ms": 10}})

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
