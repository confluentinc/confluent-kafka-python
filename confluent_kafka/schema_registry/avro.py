import json
import struct
from copy import deepcopy

from fastavro import parse_schema, schemaless_writer, schemaless_reader

from confluent_kafka.schema_registry import MessageField, TopicNameStrategy, _MAGIC_BYTE, \
    SubjectNameStrategy, ContextStringIO
from confluent_kafka.serialization import Serializer, Deserializer, SerializationError


class AvroSerializer(Serializer):
    """
    AvroSerializer encodes objects into the Avro binary format in accordance
    with the schema provided at instantiation.

    Unless configured otherwise the AvroSerializer will automatically register
    its schema with the provided registry instance.

    Args:
        conf (dict): Avro Serializer configuration instance.
        schema_registry_client (SchemaRegistryClient): Schema Registry client.
        schema (Schema): Schema instance to use when encoding Avro records.

    .. _Schema Registry specification
        https://avro.apache.org/docs/current/spec.html for more details.

    .. _Schema Resolution
        https://avro.apache.org/docs/1.8.2/spec.html#Schema+Resolution

    """

    def __init__(self, conf, schema_registry_client, schema):
        self.schema = schema
        self.schema_id = None
        self.parsed_schema = parse_schema(self._prepare_schema(schema))
        self.key_subject_func, self.value_subject_func = self.handle_config(conf)

        self.registry = schema_registry_client
        self.auto_register = True
        self.known_subjects = {}

    def handle_config(self, src_conf):
        """
        Validates AvroSerializer properties.

        Args:
            src_conf(dict): source configuration

        Returns:
            SubjectNameStrategy: Key subject name strategy implementation.
            SubjectNameStrategy: Value subject name strategy implementation.

        """
        conf = deepcopy(src_conf)

        self.auto_register = src_conf.get('auto.register.schema', True)

        if not isinstance(self.auto_register, bool):
            raise ValueError("auto.register.schema must be a boolean value")

        key_subject_name_strategy = conf.pop('key.subject.name.strategy',
                                             TopicNameStrategy())
        value_subject_name_strategy = conf.pop('value.subject.name.strategy',
                                               TopicNameStrategy())

        if not isinstance(key_subject_name_strategy, SubjectNameStrategy):
            raise ValueError("key.subject.name.strategy must implement SubjectNameStrategy")

        if not isinstance(value_subject_name_strategy, SubjectNameStrategy):
            raise ValueError("value.subject.name.strategy must implement SubjectNameStrategy")

        return key_subject_name_strategy, value_subject_name_strategy

    @staticmethod
    def _prepare_schema(schema):
        """
        Prepares schema string to be parsed by FastAvro.

        Args:
            schema (Schema): Avro schema in JSON string format.

        Returns:
             str: JSON formatted string
        """
        schema_str = schema.schema.strip()

        # FastAvro does not support parsing schemas in their canonical form
        if schema_str[0] != "{":
            schema_str = '{"type":' + schema_str + '}}'
        return json.loads(schema_str)

    def __hash__(self):
        return hash(self.parsed_schema)

    def __call__(self, value, ctx):
        """
        Encode datum to Avro binary format.

        Arguments:
            value (object): Avro object to encode
            ctx (SerializationContext): Serialization context object.

        Raises:
            SerializerError if any error occurs encoding datum with the writer
            schema.

        Returns:
            bytes: Encoded Record|Primitive

        """
        if value is None:
            return None

        if ctx.field == MessageField.VALUE:
            subject = self.value_subject_func(value, ctx)
        else:
            subject = self.key_subject_func(value, ctx)

        if self.auto_register:
            self.schema_id = self.registry.register_schema(subject, self.schema)

        with ContextStringIO() as fo:
            # Write the magic byte and schema ID in network byte order (big endian)
            fo.write(struct.pack('>bI', _MAGIC_BYTE, self.schema_id))
            # write the record to the rest of the buffer
            schemaless_writer(fo, self.parsed_schema, value)

            return fo.getvalue()


class AvroDeserializer(Deserializer):
    """
    AvroDeserializer decodes objects from their Avro binary format in accordance
    with the schema provided at instantiation.

    Unless configured otherwise the AvroSerializer will automatically register
    its schema with the provided registry instance.

    Args:
        conf (dict): Configuration dictionary
        schema_registry_client (Registryclient): Confluent Schema Registry client
        schema (Schema): Parsed Avro Schema object

    .. _Schema Registry specification
        https://avro.apache.org/docs/current/spec.html for more details.

    .. _Schema Resolution
        https://avro.apache.org/docs/1.8.2/spec.html#Schema+Resolution

    """

    def __init__(self, conf, schema_registry_client, schema):
        self.registry = schema_registry_client
        self.writer_schemas = {}
        self.reader_schema = parse_schema(self._prepare_schema(schema))

    @staticmethod
    def _prepare_schema(schema):
        """
        Prepares schema to be parsed by FastAvro.

        Args:
            schema_str (string): Avro schema in JSON string format.

        Returns:
             str: JSON formatted string
        """
        schema_str = schema.schema.strip()

        # FastAvro does not support parsing schemas in their canonical form
        if schema_str[0] != "{":
            schema_str = '{"type":' + schema_str + '}'

        return json.loads(schema_str, encoding='UTF8')

    def __call__(self, value, ctx):
        """
        Decode Avro binary to object.

        Arguments:
            value (bytes): Avro binary encoded bytes
            ctx (SerializationContext): Serialization context object.

        Raises:
            SerializerError if an error occurs ready data.

        Returns:
            object: Decoded object

        """
        if value is None:
            return None

        record = self._decode(value)

        return record

    def _decode(self, value):
        """
        Decode a message from kafka that has been encoded for use with
        the schema registry.

        Arguments:
            value (bytes | str): Avro binary to be decoded.

        Returns:
            object: Decoded object

        """
        if len(value) <= 5:
            raise SerializationError("message is too small to decode {}".format(value))

        with ContextStringIO(value) as payload:
            magic, schema_id = struct.unpack('>bI', payload.read(5))
            if magic != _MAGIC_BYTE:
                raise SerializationError("message does not start with magic byte")

            parsed = self.writer_schemas.get(schema_id, None)
            if parsed is None:
                schema = self._prepare_schema(self.registry.get_schema(schema_id))
                parsed = parse_schema(schema)
                self.writer_schemas[schema_id] = parsed

            return schemaless_reader(payload,
                                     parsed,
                                     self.reader_schema)
