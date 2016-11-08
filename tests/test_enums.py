#!/usr/bin/env python

import confluent_kafka

def test_enums():
    """ Make sure librdkafka error enums are reachable directly from the
        KafkaError class without an instantiated object. """
    print(confluent_kafka.KafkaError._NO_OFFSET)
    print(confluent_kafka.KafkaError.REBALANCE_IN_PROGRESS)

def test_tstype_enums():
    """ Make sure librdkafka tstype enums are available. """
    assert confluent_kafka.TIMESTAMP_NOT_AVAILABLE == 0
    assert confluent_kafka.TIMESTAMP_CREATE_TIME == 1
    assert confluent_kafka.TIMESTAMP_LOG_APPEND_TIME == 2
