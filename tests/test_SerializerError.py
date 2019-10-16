#!/usr/bin/env python
# -*- coding: utf-8 -*-
import unittest
from confluent_kafka.avro.serializer import SerializerError


class SerializerErrorTest(unittest.TestCase):

    def test_message(self):
        try:
            raise SerializerError("message")
        except SerializerError as e:
            assert e.message == 'message'
