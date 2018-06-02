__all__ = ['cimpl', 'admin', 'avro', 'kafkatest']
from .cimpl import (Consumer,  # noqa
                    KafkaError,
                    KafkaException,
                    Message,
                    Producer,
                    TopicPartition,
                    libversion,
                    version,
                    TIMESTAMP_NOT_AVAILABLE,
                    TIMESTAMP_CREATE_TIME,
                    TIMESTAMP_LOG_APPEND_TIME,
                    OFFSET_BEGINNING,
                    OFFSET_END,
                    OFFSET_STORED,
                    OFFSET_INVALID)

__version__ = version()[0]


class ThrottleEvent (object):
    """
    ThrottleEvent contains details about a throttled request.

    This class is typically not user instantiated.

    :ivar broker_name str: The hostname of the broker which throttled the request
    :ivar broker_id int: The broker id
    :ivar throttle_time float: The amount of time (in seconds) the broker throttled (delayed) the request
    """
    def __init__(self, broker_name,
                 broker_id,
                 throttle_time):

        self.broker_name = broker_name
        self.broker_id = broker_id
        self.throttle_time = throttle_time

    def __str__(self):
        return "{}/{} throttled for {} ms".format(self.broker_name, self.broker_id, int(self.throttle_time * 1000))
