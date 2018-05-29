from .cimpl import ConsumerImpl
from confluent_kafka.serializer import Deserializer


class Consumer(ConsumerImpl):
    def __init__(self, *args, **kwargs):
        self.conf = {
            'key_deserializer': Deserializer.verify(kwargs.pop("key_deserializer", None), True),
            'value_deserializer': Deserializer.verify(kwargs.pop("value_deserializer", None)),
        }
        super(Consumer, self).__init__(*args, **kwargs)

    @staticmethod
    def _decode(s, topic, payload):
        if s is None or payload is None:
            return payload
        return s.deserialize(topic, payload)

    def poll(self, timeout=-1):
        msg = super(Consumer, self).poll(timeout)

        # Bypass the remaining logic if there are no serializers configured
        if msg is not None:
            if self.conf['key_deserializer'] is None and self.conf['value_deserializer'] is None:
                return msg

            if not msg.value() and not msg.key():
                return msg
            if not msg.error():
                if msg.value() is not None:
                    msg.set_value(self._decode(self.conf['key_deserializer'], msg.topic(), msg.value()))
                if msg.key() is not None:
                    msg.set_key(self._decode(self.conf['value_deserializer'], msg.topic(), msg.key()))
            return msg
        return msg
