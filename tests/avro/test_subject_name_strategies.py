import os.path
import unittest

from confluent_kafka import avro
from confluent_kafka.avro.cached_schema_registry_client import (
    topic_name_strategy,
    record_name_strategy,
    topic_record_name_strategy,
)
from confluent_kafka.avro.error import SubjectNameStrategyError

from tests.avro import data_gen

avsc_dir = os.path.dirname(os.path.realpath(__file__))


class TestSubjectNameStrategies(unittest.TestCase):
    def setUp(self):
        self.schema = avro.loads(data_gen.ADVANCED_SCHEMA)

    def test_topic_name_strategy(self):
        subject = topic_name_strategy("topic", self.schema, False)
        expected = "topic-value"

        self.assertEqual(expected, subject)

    def test_record_name_strategy(self):
        subject = record_name_strategy("topic", self.schema, False)
        expected = self.schema.fullname

        self.assertEqual(expected, subject)

    def test_topic_record_name_strategy(self):
        subject = topic_record_name_strategy("topic", self.schema, False)
        expected = "topic-%s" % self.schema.fullname

        self.assertEqual(expected, subject)

    def test_should_raise_exception_for_schema_without_name(self):
        schema = avro.load(os.path.join(avsc_dir, "primitive_string.avsc"))

        with self.assertRaises(SubjectNameStrategyError, msg="the message key must have a name"):  # noqa
            record_name_strategy("topic", schema, is_key=True)

        with self.assertRaises(SubjectNameStrategyError, msg="the message value must have a name"):  # noqa
            record_name_strategy("topic", schema, is_key=False)
