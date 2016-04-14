#!/usr/bin/env python

import confluent_kafka

def test_enums():
    """ Make sure librdkafka error enums are reachable directly from the
        KafkaError class without an instantiated object. """
    print(confluent_kafka.KafkaError._NO_OFFSET)
    print(confluent_kafka.KafkaError.REBALANCE_IN_PROGRESS)
