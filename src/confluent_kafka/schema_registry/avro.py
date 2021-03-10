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
    Instantiates a Schema instance from a declaration string

    Args:
        schema_str (str): Avro Schema declaration.

    .. _Schema declaration:
        https://avro.apache.org/docs/current/spec.html#schemas

    Returns:
        Schema: Schema instance

    """
    schema_str = schema_str.strip()

    # canonical form primitive declarations are not supported
    if schema_str[0] != "{":
        schema_str = '{"type":"' + schema_str + '"}'

    return Schema(schema_str, schema_type='AVRO')


class AvroSerializer(Serializer):
    """
    AvroSerializer serializes objects in the Confluent Schema Registry binary
    format for Avro.


    AvroSerializer configuration properties:

    +---------------------------+----------+--------------------------------------------------+
    | Property Name             | Type     | Description                                      |
    +===========================+==========+==================================================+
    |                           |          | Registers schemas automatically if not           |
    | ``auto.register.schemas`` | bool     | previously associated with a particular subject. |
    |                           |          | Defaults to True.                                |
    +---------------------------+----------+--------------------------------------------------+
    |                           |          | Callable(SerializationContext, str) -> str       |
    |                           |          |                                                  |
    | ``subject.name.strategy`` | callable | Instructs the AvroSerializer on how to construct |
    |                           |          | Schema Registry subject names.                   |
    |                           |          | Defaults to topic_subject_name_strategy.         |
    +---------------------------+----------+--------------------------------------------------+

    Schemas are registered to namespaces known as Subjects which define how a
    schema may evolve over time. By default the subject name is formed by
    concatenating the topic name with the message field separated by a hyphen.

    i.e. {topic name}-{message field}

    Alternative naming strategies may be configured with the property
    ``subject.name.strategy``.

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

    See `Subject name strategy <https://docs.confluent.io/current/schema-registry/serializer-formatter.html#subject-name-strategy>`_ for additional details.

    Note:
        Prior to serialization all ``Complex Types`` must first be converted to
        a dict instance. This may handled manually prior to calling
        :py:func:`SerializingProducer.produce()` or by registering a `to_dict`
        callable with the AvroSerializer.

        See ``avro_producer.py`` in the examples directory for example usage.

    Args:
        schema_registry_client (SchemaRegistryClient): Schema Registry client instance.

        schema_str (str): Avro `Schema Declaration. <https://avro.apache.org/docs/current/spec.html#schemas>`_

        to_dict (callable, optional): Callable(object, SerializationContext) -> dict. Converts object to a dict.

        conf (dict): AvroSerializer configuration.

    """  # noqa: E501
    __slots__ = ['_hash', '_auto_register', '_known_subjects', '_parsed_schema',
                 '_registry', '_schema', '_schema_id', '_schema_name',
                 '_subject_name_func', '_to_dict']

    # default configuration
    _default_conf = {'auto.register.schemas': True,
                     'subject.name.strategy': topic_subject_name_strategy}

    def __init__(self, schema_registry_client, schema_str,
                 to_dict=None, conf=None):
        self._registry = schema_registry_client
        self._schema_id = None
        # Avoid calling registry if schema is known to be registered
        self._known_subjects = set()

        if to_dict is not None and not callable(to_dict):
            raise ValueError("to_dict must be callable with the signature"
                             " to_dict(object, SerializationContext)->dict")

        self._to_dict = to_dict

        # handle configuration
        conf_copy = self._default_conf.copy()
        if conf is not None:
            conf_copy.update(conf)

        self._auto_register = conf_copy.pop('auto.register.schemas')
        if not isinstance(self._auto_register, bool):
            raise ValueError("auto.register.schemas must be a boolean value")

        self._subject_name_func = conf_copy.pop('subject.name.strategy')
        if not callable(self._subject_name_func):
            raise ValueError("subject.name.strategy must be callable")

        if len(conf_copy) > 0:
            raise ValueError("Unrecognized properties: {}"
                             .format(", ".join(conf_copy.keys())))

        # convert schema_str to Schema instance
        schema = _schema_loads(schema_str)
        schema_dict = loads(schema.schema_str)
        parsed_schema = parse_schema(schema_dict)
        # The Avro spec states primitives have a name equal to their type
        # i.e. {"type": "string"} has a name of string.
        # This function does not comply.
        # https://github.com/fastavro/fastavro/issues/415
        schema_name = parsed_schema.get('name', schema_dict['type'])

        self._schema = schema
        self._schema_name = schema_name
        self._parsed_schema = parsed_schema

    def __call__(self, obj, ctx):
        """
        Serializes an object to the Confluent Schema Registry's Avro binary
        format.

        Args:
            obj (object): object instance to serializes.

            ctx (SerializationContext): Metadata pertaining to the serialization operation.

        Note:
            None objects are represented as Kafka Null.

        Raises:
            SerializerError: if any error occurs serializing obj

        Returns:
            bytes: Confluent Schema Registry formatted Avro bytes

        """
        if obj is None:
            return None

        subject = self._subject_name_func(ctx, self._schema_name)

        # Check to ensure this schema has been registered under subject_name.
        if self._auto_register and subject not in self._known_subjects:
            # The schema name will always be the same. We can't however register
            # a schema without a subject so we set the schema_id here to handle
            # the initial registration.
            self._schema_id = self._registry.register_schema(subject,
                                                             self._schema)
            self._known_subjects.add(subject)
        elif not self._auto_register and subject not in self._known_subjects:
            registered_schema = self._registry.lookup_schema(subject,
                                                             self._schema)
            self._schema_id = registered_schema.schema_id
            self._known_subjects.add(subject)

        if self._to_dict is not None:
            value = self._to_dict(obj, ctx)
        else:
            value = obj

        with _ContextStringIO() as fo:
            # Write the magic byte and schema ID in network byte order (big endian)
            fo.write(pack('>bI', _MAGIC_BYTE, self._schema_id))
            # write the record to the rest of the buffer
            schemaless_writer(fo, self._parsed_schema, value)

            return fo.getvalue()


class AvroDeserializer(Deserializer):
    """
    AvroDeserializer decodes bytes written in the Schema Registry
    Avro format to an object.

    Note:
        ``Complex Types`` are returned as dicts. If a more specific instance
        type is desired a callable, ``from_dict``, may be registered with
        the AvroDeserializer which converts a dict to the desired type.

        See ``avro_consumer.py`` in the examples directory in the examples
        directory for example usage.

    Args:
        schema_registry_client (SchemaRegistryClient): Confluent Schema Registry
            client instance.

        schema_str (str, optional): Avro reader schema declaration.
            If not provided, writer schema is used for deserialization.

        from_dict (callable, optional): Callable(dict, SerializationContext) -> object.
            Converts dict to an instance of some object.

        return_record_name (bool): If True, when reading a union of records, the result will
                                   be a tuple where the first value is the name of the record and the second value is
                                   the record itself.  Defaults to False.

    See Also:
        `Apache Avro Schema Declaration <https://avro.apache.org/docs/current/spec.html#schemas>`_

        `Apache Avro Schema Resolution <https://avro.apache.org/docs/1.8.2/spec.html#Schema+Resolution>`_

    """
    __slots__ = ['_reader_schema', '_registry', '_from_dict', '_writer_schemas', '_return_record_name']

    def __init__(self, schema_registry_client, schema_str=None, from_dict=None, return_record_name=False):
        self._registry = schema_registry_client
        self._writer_schemas = {}

        self._reader_schema = parse_schema(loads(schema_str)) if schema_str else None

        if from_dict is not None and not callable(from_dict):
            raise ValueError("from_dict must be callable with the signature"
                             " from_dict(SerializationContext, dict) -> object")
        self._from_dict = from_dict

        self._return_record_name = return_record_name
        if not isinstance(self._return_record_name, bool):
            raise ValueError("return_record_name must be a boolean value")

    def __call__(self, value, ctx):
        """
        Decodes a Confluent Schema Registry formatted Avro bytes to an object.

        Arguments:
            value (bytes): bytes

            ctx (SerializationContext): Metadata pertaining to the serialization
                operation.

        Raises:
            SerializerError: if an error occurs ready data.

        Returns:
            object: object if ``from_dict`` is set, otherwise dict. If no value is supplied None is returned.

        """  # noqa: E501
        if value is None:
            return None

        if len(value) <= 5:
            raise SerializationError("Message too small. This message was not"
                                     " produced with a Confluent"
                                     " Schema Registry serializer")

        with _ContextStringIO(value) as payload:
            magic, schema_id = unpack('>bI', payload.read(5))
            if magic != _MAGIC_BYTE:
                raise SerializationError("Unknown magic byte. This message was"
                                         " not produced with a Confluent"
                                         " Schema Registry serializer")

            writer_schema = self._writer_schemas.get(schema_id, None)

            if writer_schema is None:
                schema = self._registry.get_schema(schema_id)
                prepared_schema = _schema_loads(schema.schema_str)
                writer_schema = parse_schema(loads(
                    prepared_schema.schema_str))
                self._writer_schemas[schema_id] = writer_schema

            obj_dict = schemaless_reader(payload,
                                         writer_schema,
                                         self._reader_schema,
                                         self._return_record_name)

            if self._from_dict is not None:
                return self._from_dict(obj_dict, ctx)

            return obj_dict
