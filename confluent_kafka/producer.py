from .cimpl import ProducerImpl
from confluent_kafka.serializer import Serializer


class Producer(ProducerImpl):
    def __init__(self, *args, **kwargs):
        self.conf = {
            'key_serializer': Serializer.verify(kwargs.pop("key_serializer", None), True),
            'value_serializer': Serializer.verify(kwargs.pop("value_serializer", None)),
        }
        super(Producer, self).__init__(*args, **kwargs)

    @staticmethod
    def _encode(s, topic, payload):
        if s is None or payload is None:
            return payload
        return s.serialize(topic, payload)

    def produce(self, topic, value=None, key=None, *args, **kwargs):
        # Bypass the remaining logic if there are no serializers configured
        if (self.conf['key_serializer'] is None and self.conf['value_serializer']) is None:
            return super(Producer, self).produce(topic, value, key, *args, **kwargs)

        value_data = self._encode(self.conf['key_serializer'], topic,
                                  kwargs.pop('value', value))
        key_data = self._encode(self.conf['key_serializer'], topic,
                                kwargs.pop('key', key))

        super(Producer, self).produce(topic, value_data, key_data, *args[3:], **kwargs)
