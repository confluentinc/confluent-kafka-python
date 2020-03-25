#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright 2020 Confluent Inc.
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
from io import BytesIO
from json import loads
from struct import pack, unpack

from fastavro import (parse_schema,
                      schemaless_reader,
                      schemaless_writer)

from . import (_MAGIC_BYTE,
               Schema,
               topic_subject_name_strategy)
from confluent_kafka.serialization import (Deserializer,
                                           SerializationError,
                                           Serializer)


class _ContextStringIO(BytesIO):
    """
    Wrapper to allow use of StringIO via 'with' constructs.

    """

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
        return False


def _schema_loads(schema_str):
    """
    Instantiates a Schema instance from  a string

    Args:
        schema_str (str): Avro Schema declaration.

    .. _Schema declaration:
        https://avro.apache.org/docs/current/spec.html#schemas

    Returns:
        Schema: Normalized schema string.

    """
    schema_str = schema_str.strip()

    # canonical form primitive declarations are not supported
    if schema_str[0] != "{":
        schema_str = '{"type":"' + schema_str + '"}'

    return Schema(schema_str, schema_type='AVRO')


class AvroSerializer(Serializer):
    """
    AvroSerializer encodes objects in the Confluent Schema Registry binary
    format for Avro.

    AvroSerializer configuration properties:
    +-----------------------+--------------------------------------------------+
    | Property Name         | Description                                      |
    +=======================+==================================================+
    |                       | Registers schemas automatically if not           |
    | auto.register.schemas | previously associated with a particular subject. |
    |                       | Defaults to True.                                |
    +-----------------------|--------------------------------------------------+
    |                       | Instructs the AvroSerializer on how to Construct |
    | subject.name.strategy | Schema Registry subject names.                   |
    |                       | Defaults to topic_subject_name_strategy.         |
    +-----------------------+--------------------------------------------------+

    Schemas are registered to namespaces known as Subjects which define how a
    schema may evolve over time. By default the subject name is formed by
    concatenating the topic name with the message field separated by a hyphen.

    i.e. {topic name}-{message field}

    Alternative naming strategies may be configured with the property
    `subject.name.strategy`.

    Supported subject name strategies:
    +--------------------------------------+------------------------------+
    | Subject Name Strategy                | Output Format                |
    +======================================+==============================+
    | topic_subject_name_strategy(default) | {topic name}-{message field} |
    +--------------------------------------+------------------------------+
    | topic_record_subject_name_strategy   | {topic name}-{record name}   |
    +--------------------------------------+------------------------------+
    | record_subject_name_strategy         | {record name}                |
    +--------------------------------------+------------------------------+

    See ``Subject name strategy`` for additional details.

    Note:
        Prior to serialization all ``Complex Types`` must first be converted to
        a dictionary instance. This may handled manually prior to calling
        :py:func:`SerializingProducer.produce()` or by registering a `to_dict`
        callable with the AvroSerializer.

        See ``avroproducer.py`` in the examples directory for more details on
        usage.

    Args:
        schema_registry_client (SchemaRegistryClient): Schema Registry client.

        schema_str (str): Avro Schema declaration.

        to_dict (callable): Callable(object) -> dict. Converts object to a dict.

        conf (dict): AvroSerializer configuration.

    .. _Subject name strategy:
        https://docs.confluent.io/current/schema-registry/serializer-formatter.html#subject-name-strategy

    .. _Schema declaration:
        https://avro.apache.org/docs/current/spec.html#schemas

    """  # noqa: E501
    __slots__ = ['_hash', 'auto_register', 'known_subjects', 'parsed_schema',
                 'registry', 'schema', 'schema_id', 'schema_name',
                 'subject_name_func', 'to_dict']

    # default configuration
    _default = {'auto.register.schemas': True,
                'subject.name.strategy': topic_subject_name_strategy}

    def __init__(self, schema_registry_client, schema_str,
                 to_dict=None, conf=None):
        self.registry = schema_registry_client
        self.schema_id = None
        # Avoid calling registry if schema is known to be registered
        self.known_subjects = set()

        if to_dict is not None and not callable(to_dict):
            raise ValueError("to_dict must  be callable  with the signature"
                             " to_dict(object, Serialization Context)->dict")

        self.to_dict = to_dict

        # handle configuration
        conf_copy = self._default.copy()
        if conf is not None:
            conf_copy.update(conf)

        self.auto_register = conf_copy.pop('auto.register.schemas')

        if not isinstance(self.auto_register, bool):
            raise ValueError("auto.register.schemas must be a boolean value")

        subject_name_func = conf_copy.pop('subject.name.strategy')

        if not callable(subject_name_func):
            raise ValueError("subject.name.strategy must be callable.")

        self.subject_name_func = subject_name_func

        if len(conf_copy) > 0:
            raise ValueError("Unrecognized property(ies) {}"
                             .format(conf_copy.keys()))

        # convert schema_str to Schema instance
        schema = _schema_loads(schema_str)
        schema_dict = loads(schema.schema_str)
        parsed_schema = parse_schema(schema_dict)
        #  https://github.com/fastavro/fastavro/issues/415
        schema_name = parsed_schema.get('name', schema_dict['type'])

        self.schema = schema
        self.schema_name = schema_name
        self.parsed_schema = parsed_schema
        self._hash = hash(schema.schema_str)

    def __hash__(self):
        return self._hash

    def __call__(self, obj, ctx):
        """
        Serializes an object to the Confluent Schema Registry binary format
        for Avro.

        Arguments:
            obj (object): object instance to encode
            ctx (SerializationContext): Metadata pertaining to the serialization
                operation.

        Note:
            None objects are represented as Kafka Null.

        Raises:
            SerializerError if any error occurs encoding obj

        Returns:
            bytes: Confluent Schema Registry formatted binary blob

        """
        if obj is None:
            return None

        subject = self.subject_name_func(self.schema_name, ctx)

        if self.auto_register and subject not in self.known_subjects:
            self.schema_id = self.registry.register_schema(subject,
                                                           self.schema)
            self.known_subjects.add(subject)
        elif not self.auto_register and subject not in self.known_subjects:
            registered_schema = self.registry.lookup_schema(subject,
                                                            self.schema)
            self.schema_id = registered_schema.schema_id
            self.known_subjects.add(subject)

        if self.to_dict is not None:
            obj = self.to_dict(obj, ctx)

        with _ContextStringIO() as fo:
            # Write the magic byte and schema ID in network byte order (big endian)
            fo.write(pack('>bI', _MAGIC_BYTE, self.schema_id))
            # write the record to the rest of the buffer
            schemaless_writer(fo, self.parsed_schema, obj)

            return fo.getvalue()


class AvroDeserializer(Deserializer):
    """
    AvroDeserializer decodes binary blobs written in the Schema Registry
    binary format for Avro to an object.

    Note:
        ``Complex Types`` are returned as dicts. If a more specific instance
        type is desired a callable , ``to_object``, may be registered with
        the AvroDeserializer which converts a dict to the desired type.

        See ``avroconsumer.py`` in the examples directory for example usage.

    Args:
        schema_registry_client (SchemaRegistryClient): Confluent Schema Registry
            client instance.

        schema_str (str): Avro reader schema declaration.

        from_dict (callable): Callable(dict, SerializationContext) -> object.
            Converts dict to an instance of some object.

    .. _Schema declaration:
        https://avro.apache.org/docs/current/spec.html#schemas

    .. _Schema Resolution
        https://avro.apache.org/docs/1.8.2/spec.html#Schema+Resolution

    """
    __slots__ = ['reader_schema', 'registry', 'from_dict', 'writer_schemas']

    def __init__(self, schema_registry_client, schema_str, from_dict=None):
        self.registry = schema_registry_client
        self.writer_schemas = {}

        self.reader_schema = parse_schema(loads(schema_str))

        if from_dict is not None and not callable(from_dict):
            raise ValueError("to_object must be callable with the signature"
                             " to_object(SerializationContext, dict) -> object")
        self.from_dict = from_dict

    def __call__(self, value, ctx):
        """
        Decodes a Confluent Schema Registry Avro formatted binary blob
        to an object.

        Arguments:
            value (bytes): binary blob

            ctx (SerializationContext): Metadata pertaining to the serialization
                operation.

        Raises:
            SerializerError if an error occurs ready data.

        Returns:
            dict: decoded object literal if value was not None, otherwise None

        """
        if value is None:
            return None

        if len(value) <= 5:
            raise SerializationError("Message too small. This message was not"
                                     " produced with a Confluent"
                                     " Schema Registry serializer.")

        with _ContextStringIO(value) as payload:
            magic, schema_id = unpack('>bI', payload.read(5))
            if magic != _MAGIC_BYTE:
                raise SerializationError("Unknown magic byte. This message was"
                                         " not produced with a Confluent"
                                         " Schema Registry serializer.")

            writer_schema = self.writer_schemas.get(schema_id, None)
            if writer_schema is None:
                schema = self.registry.get_schema(schema_id)
                prepared_schema = _schema_loads(schema.schema_str)
                writer_schema = parse_schema(loads(
                    prepared_schema.schema_str))
                self.writer_schemas[schema_id] = writer_schema

            obj_dict = schemaless_reader(payload,
                                         writer_schema,
                                         self.reader_schema)

            if self.from_dict is not None:
                return self.from_dict(obj_dict, ctx)

            return obj_dict
