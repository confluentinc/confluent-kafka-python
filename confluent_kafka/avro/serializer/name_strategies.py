#!/usr/bin/env python
#
# Copyright 2016 Confluent Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
A set of common Subject naming strategies for use with the Confluent Schema Registry.  The built-in strategies mirror
the strategies provided via the Java client (https://github.com/confluentinc/schema-registry).

The built in strategies include:
``TopicName``
  The topic name is used along with either "key" or "value".

``RecordName``
  The fully-qualified name of the avro schema is used.

``TopicRecordName``
  The topic name is used along with the fully-qualified name of the avro schema.

Additional strategies may be provided at run time by registering a class with the ``subject.name.strategy`` entry
point.

"""


class SubjectNameStrategy(object):
    """
    A {@link SubjectNameStrategy} is used by the Avro serializer to determine
    the subject name under which the event record schemas should be registered
    in the schema registry. The default is {@link TopicNameStrategy}.
    """
    def get_subject_name(self, topic, is_key, schema):
        """
        For a given topic and message, returns the subject name under which the
        schema should be registered in the schema registry.
        :param topic: The Kafka topic name to which the message is being published.
        :param is_key: True when encoding a message key, false for a message value.
        :param schema: The value to be published in the message.
        :return: The subject name under which the schema should be registered.
        """
        raise NotImplemented("SubjectNameStrategy implementations must implement get_subject_name.")


class TopicNameStrategy(SubjectNameStrategy):
    """
    Default {@link SubjectNameStrategy}: for any messages published to
    `topic`, the schema of the message key is registered under
    the subject name `topic`-key, and the message value is registered
    under the subject name `topic`-value.
    """
    def get_subject_name(self, topic, is_key, schema):
        suffix = "-key" if is_key else "-value"
        return topic + suffix


class RecordNameStrategy(SubjectNameStrategy):
    """
    For any Avro record type that is published to Kafka, registers the schema
    in the registry under the fully-qualified record name (regardless of the
    topic). This strategy allows a topic to contain a mixture of different
    record types, since no intra-topic compatibility checking is performed.
    Instead, checks compatibility of any occurrences of the same record name
    across `all` topics.
    """
    def get_subject_name(self, topic, is_key, schema):
        return schema.fullname


class TopicRecordNameStrategy(RecordNameStrategy):
    """
    For any Avro record type that is published to Kafka topic `topic`,
    registers the schema in the registry under the subject name
    `topic`-`recordName`, where `recordName` is the
    fully-qualified Avro record name. This strategy allows a topic to contain
    a mixture of different record types, since no intra-topic compatibility
    checking is performed. Moreover, different topics may contain mutually
    incompatible versions of the same record name, since the compatibility
    check is scoped to a particular record name within a particular topic.
    """
    def get_subject_name(self, topic, is_key, schema):
        return topic + "-" + RecordNameStrategy.get_subject_name(self, topic, is_key, schema)
