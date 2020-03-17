import json

from fastavro._schema import PRIMITIVES
from fastavro.schema import schema_name, parse_schema


def SchemaProvider(schema_str, schema_type='AVRO'):
    """
    Factory function for instantiating a particular type of schema.

    Args:
        schema_str (str): Schema definition

    Keyword Args:
        schema_type (str): Schema type

    Returns:
         Schema: Schema

    """
    if schema_type == 'AVRO':
        return AvroSchema(schema_str)
    raise TypeError("Unrecognized schema_type")


class Schema(object):
    """
    Schema base class.

    Attributes:
        schema_type (str): Declares the Schema type

    Args:
        schema_str (str): Schema string representation
        registration_id (int, optional): Schema Registration ID. Defaults to -1(unregistered)

    Keyword Arguments:
        schema (str): Schema type to return. Defaults to ``AVRO``

    Raises:
        SchemaParseException if the schema can't be parsed.

    """
    __slots__ = ['schema_str', 'schema', 'name',
                 'dependencies', 'subjects']

    schema_type = None

    def __init__(self, schema):
        self.subjects = {}

    def __eq__(self, other):
        return str(self) == str(other)

    def __repr__(self):
        return self.schema_str

    def __str__(self):
        return self.schema_str

    # dicts aren't hashable
    def __hash__(self):
        return hash(str(self))


class AvroSchema(Schema):
    """
    Avro Schema definitions and accompanying Schema Registry metadata.

    Attributes:
        schema_type (str): Declares the Schema type as AVRO

    Args:
        schema (str): Avro Schema definition, must be JSON

    Raises:
        SchemaParseException if schema can't be parsed

    """
    __slots__ = ['_hash']

    schema_type = 'AVRO'

    def __init__(self, schema):
        # Setup subjects and registration id
        super(AvroSchema, self).__init__(schema)

        # Normalize schema string
        schema_dict = self._prepare_schema(schema)
        self.schema_str = json.dumps(schema_dict, sort_keys=True)

        self.schema = parse_schema(schema_dict)
        if self.schema['type'] in PRIMITIVES:
            self.name = self.schema['type']
        else:
            self.name = schema_name(self.schema, None)[1]

        self._hash = hash(self.schema_str)
        self.subjects = {}

    @staticmethod
    def _prepare_schema(schema_str):
        """

        Args:
            schema_str (string): Avro schema in JSON string format.

        Returns:
             str: JSON formatted string
        """
        schema_str = schema_str.strip()

        # FastAvro does not support parsing schemas in their canonical form
        if schema_str[0] != "{":
            schema_str = '{{"type": {} }}'.format(schema_str)
        return json.loads(schema_str)
