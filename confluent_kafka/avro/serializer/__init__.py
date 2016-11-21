class SerializerError(Exception):
    """Generic error from serializer package"""

    def __init__(self, message):
        self.message = message

        def __repr__(self):
            return 'SerializerError(error={error})'.format(error=self.message)

        def __str__(self):
            return self.message
