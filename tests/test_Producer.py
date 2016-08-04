from confluent_kafka import Producer, KafkaError, KafkaException


def test_basic_api():
    """ Basic API tests, these wont really do anything since there is no
        broker configured. """

    try:
        p = Producer()
    except TypeError as e:
        assert str(e) == "expected configuration dict"


    p = Producer({'socket.timeout.ms':10,
                  'default.topic.config': {'message.timeout.ms': 10}})

    p.produce('mytopic')
    p.produce('mytopic', value='somedata', key='a key')

    def on_delivery(err,msg):
        print('delivery', str)
        # Since there is no broker, produced messages should time out.
        assert err.code() == KafkaError._MSG_TIMED_OUT

    p.produce(topic='another_topic', value='testing', partition=9,
              callback=on_delivery)

    p.poll(0.001)

    p.flush()
