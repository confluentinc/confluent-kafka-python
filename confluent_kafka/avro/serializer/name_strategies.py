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
``topic_name_strategy``
  The topic name is used along with either "key" or "value".

``record_name_strategy``
  The fully-qualified name of the avro schema is used.

``topic_record_name_strategy``
  The topic name is used along with the fully-qualified name of the avro schema.

Additional strategies may be provided by passing a callable accepting topic, is_key, and schema as arguments.

"""


def topic_name_strategy(topic, is_key, schema):
    """
    Default {@link SubjectNameStrategy}: for any messages published to
    `topic`, the schema of the message key is registered under
    the subject name `topic`-key, and the message value is registered
    under the subject name `topic`-value.
    """
    suffix = "-key" if is_key else "-value"
    return topic + suffix


def record_name_strategy(topic, is_key, schema):
    """
    For any Avro record type that is published to Kafka, registers the schema
    in the registry under the fully-qualified record name (regardless of the
    topic). This strategy allows a topic to contain a mixture of different
    record types, since no intra-topic compatibility checking is performed.
    Instead, checks compatibility of any occurrences of the same record name
    across `all` topics.
    """
    return schema.fullname if hasattr(schema, 'fullname') \
        else schema.name if hasattr(schema, 'name') \
        else schema.type if schema is not None \
        else 'null'


def topic_record_name_strategy(topic, is_key, schema):
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
    return topic + "-" + record_name_strategy(topic, is_key, schema)
