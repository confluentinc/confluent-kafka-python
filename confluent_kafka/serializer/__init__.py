from abc import ABCMeta, abstractmethod


class Serializer(object):
    __meta__ = ABCMeta

    def __init__(self, **config):
        pass

    @abstractmethod
    def serialize(self, topic, value):
        pass

    @classmethod
    def verify(cls, obj, is_key=False):
        if obj is None:
            return None

        if isinstance(obj, Serializer):
            return obj

        raise ValueError(
            '{}_serializer must register with or extend Serialize abstract base class'.format('key' if is_key
                                                                                              else 'value'))

    def close(self):
        pass


class Deserializer(object):
    __meta__ = ABCMeta

    def __init__(self, **config):
        pass

    @abstractmethod
    def deserialize(self, topic, value):
        pass

    @classmethod
    def verify(cls, obj, is_key=False):
        if obj is None:
            return None

        if isinstance(obj, Deserializer):
            return obj

        raise ValueError(
            '{}_deserializer must implement Deserializer abstract base class'.format('key' if is_key
                                                                                     else 'value'))

    def close(self):
        pass
