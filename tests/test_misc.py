#!/usr/bin/env python

import confluent_kafka


def test_version():
    print('Using confluent_kafka module version %s (0x%x)' % confluent_kafka.version())
    sver, iver = confluent_kafka.version()
    assert len(sver) > 0
    assert iver > 0

    print('Using librdkafka version %s (0x%x)' % confluent_kafka.libversion())
    sver, iver = confluent_kafka.libversion()
    assert len(sver) > 0
    assert iver > 0

