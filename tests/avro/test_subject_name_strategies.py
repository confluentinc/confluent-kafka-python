import unittest

from confluent_kafka import avro
from confluent_kafka.avro.cached_schema_registry_client import (
    topic_name_strategy,
    record_name_strategy,
    topic_record_name_strategy,
)

from tests.avro import data_gen


class TestSubjectNameStrategies(unittest.TestCase):
    def setUp(self):
        self.record = avro.loads(data_gen.ADVANCED_SCHEMA)

    def test_topic_name_strategy(self):
        subject = topic_name_strategy("topic", self.record)
        expected = "topic"

        self.assertEqual(expected, subject)

    def test_record_name_strategy(self):
        subject = record_name_strategy("topic", self.record)
        expected = self.record.name

        self.assertEqual(expected, subject)

    def test_topic_record_name_strategy(self):
        subject = topic_record_name_strategy("topic", self.record)
        expected = "topic-%s" % self.record.name

        self.assertEqual(expected, subject)
