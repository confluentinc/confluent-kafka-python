"""
    Avro schema registry module: Deals with encoding and decoding of messages with avro schemas

"""
import sys

from confluent_kafka import Producer

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
from confluent_kafka.avro.serializer import SerializerError
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
                 default_value_schema=None):
        if ('schema.registry.url' not in config.keys()):
            raise ValueError("Missing parameter: schema.registry.url")
        schem_registry_url = config["schema.registry.url"]
        del config["schema.registry.url"]

        super(AvroProducer, self).__init__(config)
        self._serializer = MessageSerializer(CachedSchemaRegistryClient(url=schem_registry_url))
        self._key_schema = default_key_schema
        self._value_schema = default_value_schema

    def produce(self, **kwargs):
        """
            Sends message to kafka by encoding with specified avro schema
            @:param: topic: topic name
            @:param: value: A dictionary object
            @:param: value_schema : Avro schema for value
            @:param: key: A dictionary object
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
                raise SerializerError("Avro schema required for value")

        if key:
            if key_schema:
                key = self._serializer.encode_record_with_schema(topic, key_schema, key, True)
            else:
                raise SerializerError("Avro schema required for key")

        super(AvroProducer, self).produce(topic, value, key, **kwargs)
