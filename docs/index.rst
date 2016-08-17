Welcome to Confluent's Apache Kafka Python client documentation
===============================================================

Indices and tables
==================

* :ref:`genindex`

:mod:`confluent_kafka` --- Confluent's Apache Kafka Python client
*****************************************************************

.. automodule:: confluent_kafka
   :synopsis: Confluent's Apache Kafka Python client.
   :members:

********
Consumer
********

.. autoclass:: confluent_kafka.Consumer
   :members:

********
Producer
********

.. autoclass:: confluent_kafka.Producer
   :members:

*******
Message
*******

.. autoclass:: confluent_kafka.Message
   :members:

**************
TopicPartition
**************

.. autoclass:: confluent_kafka.TopicPartition
   :members:

**********
KafkaError
**********

.. autoclass:: confluent_kafka.KafkaError
   :members:

**************
KafkaException
**************

.. autoclass:: confluent_kafka.KafkaException
   :members:



Configuration
=============
Configuration of producer and consumer instances is performed by
providing a dict of configuration properties to the instance constructor, e.g.::

  conf = {'bootstrap.servers': 'mybroker.com',
          'group.id': 'mygroup', 'session.timeout.ms': 6000,
          'on_commit': my_commit_callback,
          'default.topic.config': {'auto.offset.reset': 'smallest'}}
  consumer = confluent_kafka.Consumer(**conf)

The supported configuration values are dictated by the underlying
librdkafka C library. For the full range of configuration properties
please consult librdkafka's documentation:
https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md

The Python bindings also provide some additional configuration properties:

* ``default.topic.config``: value is a dict of topic-level configuration
  properties that are applied to all used topics for the instance.

* ``on_delivery`` (**Producer**): value is a Python function reference
  that is called once for each produced message to indicate the final
  delivery result (success or failure).
  This property may also be set per-message by passing ``callback=callable``
  (or ``on_delivery=callable``) to the confluent_kafka.Producer.produce() function.

* ``on_commit`` (**Consumer**): Callback used to indicate success or failure
  of commit requests.

Changelog
=========

Version 3.0.1
^^^^^^^^^^^^^

* `PR-3 <https://github.com/confluentinc/confluent-kafka-python/pull/3>`_ - Add /usr/local/lib to library_dirs in setup
* `PR-4 <https://github.com/confluentinc/confluent-kafka-python/pull/4>`_ - Py3: use bytes for Message payload and key
* `PR-5 <https://github.com/confluentinc/confluent-kafka-python/pull/5>`_ - Removed hard coded c extentions lib/include paths
* `PR-9 <https://github.com/confluentinc/confluent-kafka-python/pull/9>`_ - Use consistent syntax highlighting (e.g. prefix commands with `$`)
* `PR-17 <https://github.com/confluentinc/confluent-kafka-python/pull/17>`_ - Version bump to 0.9.1.2


