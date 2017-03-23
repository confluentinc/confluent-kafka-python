"""
    Avro schema registry module: Deals with encoding and decoding of messages with avro schemas

"""
import sys

from confluent_kafka import Producer, Consumer

VALID_LEVELS = ['NONE', 'FULL', 'FORWARD', 'BACKWARD']


def loads(schema_str):
    """ Parse a schema given a schema string """
    if sys.version_info[0] < 3:
        return schema.parse(schema_str)
    else:
        return schema.Parse(schema_str)


def load(fp):
    """ Parse a schema from a file path """
    with open(fp) as f:
        return loads(f.read())


# avro.schema.RecordSchema and avro.schema.PrimitiveSchema classes are not hashable. Hence defining them explicitely as a quick fix
def _hash_func(self):
    return hash(str(self))


try:
    from avro import schema

    schema.RecordSchema.__hash__ = _hash_func
    schema.PrimitiveSchema.__hash__ = _hash_func
except ImportError:
    pass


class ClientError(Exception):
    """ Error thrown by Schema Registry clients """

    def __init__(self, message, http_code=None):
        self.message = message
        self.http_code = http_code
        super(ClientError, self).__init__(self.__str__())

    def __repr__(self):
        return "ClientError(error={error})".format(error=self.message)

    def __str__(self):
        return self.message


from confluent_kafka.avro.cached_schema_registry_client import CachedSchemaRegistryClient
from confluent_kafka.avro.serializer import (SerializerError,
                                             KeySerializerError,
                                             ValueSerializerError)
from confluent_kafka.avro.serializer.message_serializer import MessageSerializer


class AvroProducer(Producer):
    """
        Kafka Producer client which does avro schema encoding to messages.
        Handles schema registration, Message serialization.

        Constructor takes below parameters

        @:param: config: dict object with config parameters containing url for schema registry (schema.registry.url).
        @:param: default_key_schema: Optional avro schema for key
        @:param: default_value_schema: Optional avro schema for value
    """

    def __init__(self, config, default_key_schema=None,
                 default_value_schema=None, schema_registry=None):
        schema_registry_url = config.pop("schema.registry.url", None)
        if schema_registry is None:
            if schema_registry_url is None:
                raise ValueError("Missing parameter: schema.registry.url")
            schema_registry = CachedSchemaRegistryClient(url=schema_registry_url)
        elif schema_registry_url is not None:
            raise ValueError("Cannot pass schema_registry along with schema.registry.url config")

        super(AvroProducer, self).__init__(config)
        self._serializer = MessageSerializer(schema_registry)
        self._key_schema = default_key_schema
        self._value_schema = default_value_schema

    def produce(self, **kwargs):
        """
            Sends message to kafka by encoding with specified avro schema
            @:param: topic: topic name
            @:param: value: An object to serialize
            @:param: value_schema : Avro schema for value
            @:param: key: An object to serialize
            @:param: key_schema : Avro schema for key
            @:exception: SerializerError
        """
        # get schemas from  kwargs if defined
        key_schema = kwargs.pop('key_schema', self._key_schema)
        value_schema = kwargs.pop('value_schema', self._value_schema)
        topic = kwargs.pop('topic', None)
        if not topic:
            raise ClientError("Topic name not specified.")
        value = kwargs.pop('value', None)
        key = kwargs.pop('key', None)

        if value:
            if value_schema:
                value = self._serializer.encode_record_with_schema(topic, value_schema, value)
            else:
                raise ValueSerializerError("Avro schema required for values")

        if key:
            if key_schema:
                key = self._serializer.encode_record_with_schema(topic, key_schema, key, True)
            else:
                raise KeySerializerError("Avro schema required for key")


        super(AvroProducer, self).produce(topic, value, key, **kwargs)


class AvroConsumer(Consumer):
    """
    Kafka Consumer client which does avro schema decoding of messages.
    Handles message deserialization.

    Constructor takes below parameters

    @:param: config: dict object with config parameters containing url for schema registry (schema.registry.url).
    """
    def __init__(self, config, schema_registry=None):
        schema_registry_url = config.pop("schema.registry.url", None)
        if schema_registry is None:
            if schema_registry_url is None:
                raise ValueError("Missing parameter: schema.registry.url")
            schema_registry = CachedSchemaRegistryClient(url=schema_registry_url)
        elif schema_registry_url is not None:
            raise ValueError("Cannot pass schema_registry along with schema.registry.url config")

        super(AvroConsumer, self).__init__(config)
        self._serializer = MessageSerializer(schema_registry)

    def poll(self, timeout=None):
        """
        This is an overriden method from confluent_kafka.Consumer class. This handles message
        deserialization using avro schema

        @:param timeout
        @:return message object with deserialized key and value as dict objects
        """
        if timeout is None:
            timeout = -1
        message = super(AvroConsumer, self).poll(timeout)
        if message is None:
            return None
        if not message.value() and not message.key():
            return message
        if not message.error():
            if message.value() is not None:
                decoded_value = self._serializer.decode_message(message.value())
                message.set_value(decoded_value)
            if message.key() is not None:
                decoded_key = self._serializer.decode_message(message.key())
                message.set_key(decoded_key)
        return message
