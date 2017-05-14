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


def test_offset_consts():
    """ Make sure librdkafka's logical offsets are available. """
    assert confluent_kafka.OFFSET_BEGINNING == -2
    assert confluent_kafka.OFFSET_END == -1
    assert confluent_kafka.OFFSET_STORED == -1000
    assert confluent_kafka.OFFSET_INVALID == -1001
