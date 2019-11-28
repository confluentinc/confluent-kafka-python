The confluent_kafka API
=======================

A reliable, performant and feature rich Python client for Apache Kafka v0.8 and above.

Clients
   - :ref:`Consumer <pythonclient_consumer>`
   - :ref:`Producer <pythonclient_producer>`
   - :ref:`AdminClient <pythonclient_adminclient>`


Supporting classes
    - :ref:`Message <pythonclient_message>`
    - :ref:`TopicPartition <pythonclient_topicpartition>`
    - :ref:`KafkaError <pythonclient_kafkaerror>`
    - :ref:`KafkaException <pythonclient_kafkaexception>`
    - :ref:`ThrottleEvent <pythonclient_throttleevent>`
    - :ref:`Avro <pythonclient_avro>`


:ref:`genindex`


.. _pythonclient_consumer:

Consumer
========

.. autoclass:: confluent_kafka.Consumer
   :members:

.. _pythonclient_producer:

Producer
========

.. autoclass:: confluent_kafka.Producer
   :members:

.. _pythonclient_adminclient:

AdminClient
===========

.. automodule:: confluent_kafka.admin
   :members:

.. _pythonclient_avro:

Avro
====

.. automodule:: confluent_kafka.avro
   :members:

Supporting Classes
==================

.. _pythonclient_message:

*******
Message
*******

.. autoclass:: confluent_kafka.Message
   :members:

.. _pythonclient_topicpartition:

**************
TopicPartition
**************

.. autoclass:: confluent_kafka.TopicPartition
   :members:


.. _pythonclient_kafkaerror:

**********
KafkaError
**********

.. autoclass:: confluent_kafka.KafkaError
   :members:


.. _pythonclient_kafkaexception:

**************
KafkaException
**************

.. autoclass:: confluent_kafka.KafkaException
   :members:


******
Offset
******

Logical offset constants:

 * :py:const:`OFFSET_BEGINNING` - Beginning of partition (oldest offset)
 * :py:const:`OFFSET_END` - End of partition (next offset)
 * :py:const:`OFFSET_STORED` - Use stored/committed offset
 * :py:const:`OFFSET_INVALID` - Invalid/Default offset


.. _pythonclient_throttleevent:

*************
ThrottleEvent
*************

.. autoclass:: confluent_kafka.ThrottleEvent
   :members:


.. _pythonclient_configuration:

Configuration
=============

Configuration of producer and consumer instances is performed by
providing a dict of configuration properties to the instance constructor, e.g.::

  conf = {'bootstrap.servers': 'mybroker.com',
          'group.id': 'mygroup', 'session.timeout.ms': 6000,
          'on_commit': my_commit_callback,
          'auto.offset.reset': 'earliest'}
  consumer = confluent_kafka.Consumer(conf)

The supported configuration values are dictated by the underlying
librdkafka C library. For the full range of configuration properties
please consult librdkafka's documentation:
https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md

The Python bindings also provide some additional configuration properties:

* ``default.topic.config``: value is a dict of client topic-level configuration
  properties that are applied to all used topics for the instance. **DEPRECATED:**
  topic configuration should now be specified in the global top-level configuration.

* ``error_cb(kafka.KafkaError)``: Callback for generic/global error events, these errors are typically to be considered informational since the client will automatically try to recover. This callback is served upon calling
  ``client.poll()`` or ``producer.flush()``.

* ``throttle_cb(confluent_kafka.ThrottleEvent)``: Callback for throttled request reporting.
  This callback is served upon calling ``client.poll()`` or ``producer.flush()``.

* ``stats_cb(json_str)``: Callback for statistics data. This callback is triggered by poll() or flush
  every ``statistics.interval.ms`` (needs to be configured separately).
  Function argument ``json_str`` is a str instance of a JSON document containing statistics data.
  This callback is served upon calling ``client.poll()`` or ``producer.flush()``. See
  https://github.com/edenhill/librdkafka/wiki/Statistics" for more information.

* ``on_delivery(kafka.KafkaError, kafka.Message)`` (**Producer**): value is a Python function reference
  that is called once for each produced message to indicate the final
  delivery result (success or failure).
  This property may also be set per-message by passing ``callback=callable``
  (or ``on_delivery=callable``) to the confluent_kafka.Producer.produce() function.
  Currently message headers are not supported on the message returned to the
  callback. The ``msg.headers()`` will return None even if the original message
  had headers set. This callback is served upon calling ``producer.poll()`` or ``producer.flush()``.

* ``on_commit(kafka.KafkaError, list(kafka.TopicPartition))`` (**Consumer**): Callback used to indicate
  success or failure of asynchronous and automatic commit requests. This callback is served upon calling
  ``consumer.poll()``. Is not triggered for synchronous commits. Callback arguments: *KafkaError* is the
  commit error, or None on success. *list(TopicPartition)* is the list of partitions with their committed
  offsets or per-partition errors.

* ``logger=logging.Handler`` kwarg: forward logs from the Kafka client to the
  provided ``logging.Handler`` instance.
  To avoid spontaneous calls from non-Python threads the log messages
  will only be forwarded when ``client.poll()`` or ``producer.flush()`` are called.
  For example::

    mylogger = logging.getLogger()
    mylogger.addHandler(logging.StreamHandler())
    producer = confluent_kafka.Producer({'bootstrap.servers': 'mybroker.com'}, logger=mylogger)
