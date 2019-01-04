from avro.schema import PrimitiveSchema

# Python 2 considers int an instance of str
try:
    string_type = basestring  # noqa
except NameError:
    string_type = str


class GenericAvroRecord(dict):
    __slots__ = ['schema']

    def __init__(self, schema, datum=None):
        self.schema = schema
        if datum is not None:
            self.update(datum)

    def put(self, key, value):
        self[key] = value


def get_schema(datum):
    if isinstance(datum, GenericAvroRecord):
        return datum.schema
    elif (datum is None):
        return PrimitiveSchema("null")
    elif isinstance(datum, basestring):
        return PrimitiveSchema('string')
    elif isinstance(datum, bool):
        return PrimitiveSchema('boolean')
    elif isinstance(datum, int):
        return PrimitiveSchema('int')
    elif isinstance(datum, float):
        return PrimitiveSchema('float')
    else:
        raise ValueError("Unsupported Avro type {}.".format(type(datum)))