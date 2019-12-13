from confluent_kafka import Producer, KafkaError


class ConfluentException(Exception):
    """
        Converts KafkaError into an exception for handling by an application.

        :param KafkaError kafka_error: The KafkaError to convert.
        :param str msg: Optional Error message override.
    """
    def __init__(self, kafka_error, msg=None):
        self.code = kafka_error.code()

        if msg is None:
            msg = kafka_error.str()

        self.message = msg


class DeliveryReporter(object):
    __slots__ = ["success"]

    def __init__(self):
        self.success = False

    def __call__(self, err, msg):
        """
        Handles per message delivery reports indicating success or failed delivery attempts.

        :param err: KafkaError indicating message delivery failed.
        :param msg: The message delivered to the broker.
        """
        if err:
            self.error(err)

        self.success = True
        print("Message {} delivered!".format(msg.offset()))

    def error(self, err):
        """
        Handler for global/generic and message delivery errors.

        Raises ConfluentException for `KafkaError.__ALL_BROKERS_DOWN` if seen before a successful delivery.
        Often times this is the result of a misconfiguration.
        """
        if not self.success and err.code() is KafkaError._ALL_BROKERS_DOWN:
            raise ConfluentException(err)

        print('Error: %s' % err)


if __name__ == "__main__":
    """
    This example is designed to demonstrate how a user might report broker connection failures.

    https://github.com/confluentinc/confluent-kafka-python/issues/705
    """

    dr = DeliveryReporter()

    p = Producer({'bootstrap.servers': 'incorrect.address',
                  'socket.timeout.ms': 10,
                  'retries': 0,
                  'error_cb': dr.error})
    p.poll(1.0)
