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

******
Offset
******

Logical offset constants:

 * :py:const:`OFFSET_BEGINNING` - Beginning of partition (oldest offset)
 * :py:const:`OFFSET_END` - End of partition (next offset)
 * :py:const:`OFFSET_STORED` - Use stored/committed offset
 * :py:const:`OFFSET_INVALID` - Invalid/Default offset



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

* ``error_cb(kafka.KafkaError)``: Callback for generic/global error events. This callback is served by
  poll().

* ``stats_cb(json_str)``: Callback for statistics data. This callback is triggered by poll()
  every ``statistics.interval.ms`` (needs to be configured separately).
  Function argument ``json_str`` is a str instance of a JSON document containing statistics data.

* ``on_delivery(kafka.KafkaError, kafka.Message)`` (**Producer**): value is a Python function reference
  that is called once for each produced message to indicate the final
  delivery result (success or failure).
  This property may also be set per-message by passing ``callback=callable``
  (or ``on_delivery=callable``) to the confluent_kafka.Producer.produce() function.

* ``on_commit(kafka.KafkaError, list(kafka.TopicPartition))`` (**Consumer**): Callback used to indicate success or failure
  of commit requests.

Changelog
=========

Version 0.11.0
^^^^^^^^^^^^^^

 * Handle null/None values during deserialization
 * Allow to pass custom schema registry instance.
 * None conf values are now converted to NULL rather than the string "None" (#133)
 * Fix memory leaks when certain exceptions were raised.
 * Handle delivery.report.only.error in Python (#84)
 * Proper use of Message error string on Producer (#129)
 * Now Flake8 clean

Version 0.9.4
^^^^^^^^^^^^^

 * Unlock GIL for Consumer_close's rd_kafka_destroy()
 * Unlock GIL on Producer's librdkafka destroy call (#107)
 * Add optional timeout argument to Producer.flush() (#105)
 * Added offset constants
 * Added Consumer.get_watermark_offsets() (#31)
 * Added Consumer.assignment() API
 * Add timestamp= arg to produce()
 * replace from .cimpl import * with explicit names. (#87)
 * Dont delete unset tlskey (closes #78)
 * AvroConsumer for handling schema registry (#80)
 * Fix open issue #73 -- TopicPartition_str0 broken on Mac OS X (#83)
 * Producer client for handling avro schemas (#40, @roopahc, @criccomini)
 * enable.auto.commit behavior consequences on close() and commit() (#77)
 * Consumer, Producer, TopicPartition classes are now sub-classable
 * commit() without msg/offset args would call C commit() twice (#71)
 * Consumer: set up callstate on dealloc to allow callbacks (#66)
 * Added statistics callback support (#43)
 * Add timestamp() to Messages

Version 0.9.2
^^^^^^^^^^^^^

 * on_commit: handle NULL offsets list (on error)
 * Fix 32-bit arch build warnings
 * Destroy rd_kafka_t handle on consumer.close() (#30)
 * Handle None error_cb and dr_cb
 * Added CallState to track per-thread C call state (fixes #19)
 * Make sure to GC on_commit callable


Version 0.9.1.2
^^^^^^^^^^^^^^^

* `PR-3 <https://github.com/confluentinc/confluent-kafka-python/pull/3>`_ - Add /usr/local/lib to library_dirs in setup
* `PR-4 <https://github.com/confluentinc/confluent-kafka-python/pull/4>`_ - Py3: use bytes for Message payload and key
* `PR-5 <https://github.com/confluentinc/confluent-kafka-python/pull/5>`_ - Removed hard coded c extentions lib/include paths
* `PR-9 <https://github.com/confluentinc/confluent-kafka-python/pull/9>`_ - Use consistent syntax highlighting (e.g. prefix commands with `$`)
* `PR-17 <https://github.com/confluentinc/confluent-kafka-python/pull/17>`_ - Version bump to 0.9.1.2


