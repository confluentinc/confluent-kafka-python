from confluent_kafka import KafkaError


def test_error():
    try:
        raise KafkaError(KafkaError._MSG_TIMED_OUT)
    except KafkaError as e:
        if e.retriable():
            do_something()