confluent_kafka API
===================

A reliable, performant and feature-rich Python client for Apache Kafka v0.8 and above.

Configuration
   - :ref:`Configuration Guide <pythonclient_configuration>`

Client API
   - :ref:`AdminClient <pythonclient_adminclient>`
   - :ref:`Consumer <pythonclient_consumer>`
   - :ref:`DeserializingConsumer <serde_consumer>` (new API subject to change)
   - :ref:`AvroConsumer <avro_consumer>` (legacy)
   - :ref:`Producer <pythonclient_producer>`
   - :ref:`SerializingProducer <serde_producer>` (new API subject to change)
   - :ref:`AvroProducer <avro_producer>` (legacy)
   - :ref:`SchemaRegistry <schemaregistry_client>`


Serialization API
   - Serializer
      - :ref:`AvroDeserializer <schemaregistry_avro_deserializer>`
      - :ref:`DoubleDeserializer <serde_deserializer_double>`
      - :ref:`JSONDeserializer <schemaregistry_json_deserializer>`
      - :ref:`IntegerDeserializer <serde_deserializer_integer>`
      - :ref:`ProtobufDeserializer <schemaregistry_protobuf_deserializer>`
      - :ref:`StringDeserializer <serde_deserializer_string>`

   - Deserializer
      - :ref:`AvroSerializer <schemaregistry_avro_serializer>`
      - :ref:`DoubleSerializer <serde_serializer_double>`
      - :ref:`JSONSerializer <schemaregistry_json_serializer>`
      - :ref:`IntegerSerializer <serde_serializer_integer>`
      - :ref:`ProtobufSerializer <schemaregistry_protobuf_serializer>`
      - :ref:`StringSerializer <serde_serializer_string>`


Supporting classes
    - :ref:`Message <pythonclient_message>`
    - :ref:`TopicPartition <pythonclient_topicpartition>`
    - :ref:`ThrottleEvent <pythonclient_throttleevent>`

    - Errors
        - :ref:`KafkaError <pythonclient_kafkaerror>`
        - :ref:`KafkaException <pythonclient_kafkaexception>`
        - :ref:`ConsumeError <pyclient_error_consumer>`
        - :ref:`ProduceError <pyclient_error_producer>`
        - :ref:`SerializationError <serde_error>`
            - :ref:`KeySerializationError <serde_error_serializer_key>`
            - :ref:`ValueSerializationError <serde_error_serializer_value>`
            - :ref:`KeyDeserializationError <serde_error_deserializer_key>`
            - :ref:`ValueDeserializationError <serde_error_deserializer_value>`

    - Admin API
        - :ref:`NewTopic <pyclient_admin_newtopic>`
        - :ref:`NewPartitions <pyclient_admin_newpartitions>`
        - :ref:`ConfigSource <pythonclient_config_source>`
        - :ref:`ConfigEntry <pythonclient_config_entry>`
        - :ref:`ConfigResource <pythonclient_config_resource>`
        - :ref:`ResourceType <pythonclient_resource_type>`
        - :ref:`ResourcePatternType <pythonclient_resource_pattern_type>`
        - :ref:`AclOperation <pythonclient_acl_operation>`
        - :ref:`AclPermissionType <pythonclient_acl_permission_type>`
        - :ref:`AclBinding <pythonclient_acl_binding>`
        - :ref:`AclBindingFilter <pythonclient_acl_binding_filter>`

Guide to the :ref:`Transactional Producer API <pythonclient_transactional>`



Kafka Clients
=============

.. _pythonclient_adminclient:

***********
AdminClient
***********

.. automodule:: confluent_kafka.admin
   :members:

.. _pyclient_admin_newtopic:

**************
NewTopic
**************

.. autoclass:: confluent_kafka.admin.NewTopic
   :members:

.. _pyclient_admin_newpartitions:

**************
NewPartitions
**************

.. autoclass:: confluent_kafka.admin.NewPartitions
   :members:

.. _pythonclient_config_source:

**************
ConfigSource
**************

.. autoclass:: confluent_kafka.admin.ConfigSource
   :members:

.. _pythonclient_config_entry:

**************
ConfigEntry
**************

.. autoclass:: confluent_kafka.admin.ConfigEntry
   :members:

.. _pythonclient_config_resource:

**************
ConfigResource
**************

.. autoclass:: confluent_kafka.admin.ConfigResource
   :members:

.. _pythonclient_resource_type:

**************
ResourceType
**************

.. autoclass:: confluent_kafka.admin.ResourceType
   :members:

.. _pythonclient_resource_pattern_type:

*******************
ResourcePatternType
*******************

.. autoclass:: confluent_kafka.admin.ResourcePatternType
   :members:

.. _pythonclient_acl_operation:

**************
AclOperation
**************

.. autoclass:: confluent_kafka.admin.AclOperation
   :members:

.. _pythonclient_acl_permission_type:

*****************
AclPermissionType
*****************

.. autoclass:: confluent_kafka.admin.AclPermissionType
   :members:

.. _pythonclient_acl_binding:

**************
AclBinding
**************

.. autoclass:: confluent_kafka.admin.AclBinding
   :members:

.. _pythonclient_acl_binding_filter:

****************
AclBindingFilter
****************

.. autoclass:: confluent_kafka.admin.AclBindingFilter
   :members:

.. _pythonclient_consumer:

********
Consumer
********

.. autoclass:: confluent_kafka.Consumer
   :members:

.. _serde_consumer:

*********************
DeserializingConsumer
*********************

.. autoclass:: confluent_kafka.DeserializingConsumer
   :members:

   :inherited-members:

.. _pythonclient_producer:

********
Producer
********

.. autoclass:: confluent_kafka.Producer
   :members:

.. _serde_producer:

*******************
SerializingProducer
*******************

.. autoclass:: confluent_kafka.SerializingProducer
   :members:

   :inherited-members:

.. _schemaregistry_client:

********************
SchemaRegistryClient
********************

.. autoclass:: confluent_kafka.schema_registry.SchemaRegistryClient
   :members:

.. _avro_producer:

********************
AvroProducer(Legacy)
********************

.. autoclass:: confluent_kafka.avro.AvroProducer
   :members:

.. _avro_consumer:

********************
AvroConsumer(Legacy)
********************

.. autoclass:: confluent_kafka.avro.AvroConsumer
   :members:

Serialization API
=================

.. _serde_deserializer:

************
Deserializer
************

.. autoclass:: confluent_kafka.serialization.Deserializer
   :members:

   .. automethod:: __call__

.. _schemaregistry_avro_deserializer:

****************
AvroDeserializer
****************

.. autoclass:: confluent_kafka.schema_registry.avro.AvroDeserializer
   :members:

   .. automethod:: __call__

.. _serde_deserializer_double:

******************
DoubleDeserializer
******************

.. autoclass:: confluent_kafka.serialization.DoubleDeserializer
   :members:

   .. automethod:: __call__


.. _serde_deserializer_integer:

*******************
IntegerDeserializer
*******************

.. autoclass:: confluent_kafka.serialization.IntegerDeserializer
   :members:

   .. automethod:: __call__

.. _schemaregistry_json_deserializer:

****************
JSONDeserializer
****************

.. autoclass:: confluent_kafka.schema_registry.json_schema.JSONDeserializer
   :members:

   .. automethod:: __call__

.. _schemaregistry_protobuf_deserializer:

********************
ProtobufDeserializer
********************

.. autoclass:: confluent_kafka.schema_registry.protobuf.ProtobufDeserializer
   :members:

   .. automethod:: __call__

.. _serde_deserializer_string:

******************
StringDeserializer
******************

.. autoclass:: confluent_kafka.serialization.StringDeserializer
   :members:

   .. automethod:: __call__

.. _serde_serializer:

**********
Serializer
**********

.. autoclass:: confluent_kafka.serialization.Serializer
   :members:

   .. automethod:: __call__

.. _schemaregistry_avro_serializer:

**************
AvroSerializer
**************

.. autoclass:: confluent_kafka.schema_registry.avro.AvroSerializer
   :members:

   .. automethod:: __call__

.. _serde_serializer_double:

****************
DoubleSerializer
****************

.. autoclass:: confluent_kafka.serialization.DoubleSerializer
   :members:

   .. automethod:: __call__


.. _serde_serializer_integer:

*****************
IntegerSerializer
*****************

.. autoclass:: confluent_kafka.serialization.IntegerSerializer
   :members:

   .. automethod:: __call__

.. _schemaregistry_json_serializer:

**************
JSONSerializer
**************

.. autoclass:: confluent_kafka.schema_registry.json_schema.JSONSerializer
   :members:

   .. automethod:: __call__

.. _schemaregistry_protobuf_serializer:

******************
ProtobufSerializer
******************

.. autoclass:: confluent_kafka.schema_registry.protobuf.ProtobufSerializer
   :members:

   .. automethod:: __call__

.. _serde_serializer_string:

****************
StringSerializer
****************

.. autoclass:: confluent_kafka.serialization.StringSerializer
   :members:

   .. automethod:: __call__

.. _pythonclient_transactional:

Transactional Producer API
==========================

The transactional producer operates on top of the idempotent producer,
and provides full exactly-once semantics (EOS) for Apache Kafka when used
with the transaction aware consumer (``isolation.level=read_committed``).

A producer instance is configured for transactions by setting the
transactional.id to an identifier unique for the application. This
id will be used to fence stale transactions from previous instances of
the application, typically following an outage or crash.

After creating the transactional producer instance the transactional state
must be initialized by calling
:py:meth:`confluent_kafka.Producer.init_transactions()`.
This is a blocking call that will acquire a runtime producer id from the
transaction coordinator broker as well as abort any stale transactions and
fence any still running producer instances with the same ``transactional.id``.

Once transactions are initialized the application may begin a new
transaction by calling :py:meth:`confluent_kafka.Producer.begin_transaction()`.
A producer instance may only have one single on-going transaction.

Any messages produced after the transaction has been started will
belong to the ongoing transaction and will be committed or aborted
atomically.
It is not permitted to produce messages outside a transaction
boundary, e.g., before :py:meth:`confluent_kafka.Producer.begin_transaction()`
or after :py:meth:`confluent_kafka.commit_transaction()`,
:py:meth:`confluent_kafka.Producer.abort_transaction()`, or after the current
transaction has failed.

If consumed messages are used as input to the transaction, the consumer
instance must be configured with enable.auto.commit set to false.
To commit the consumed offsets along with the transaction pass the
list of consumed partitions and the last offset processed + 1 to
:py:meth:`confluent_kafka.Producer.send_offsets_to_transaction()` prior to
committing the transaction.
This allows an aborted transaction to be restarted using the previously
committed offsets.

To commit the produced messages, and any consumed offsets, to the
current transaction, call
:py:meth:`confluent_kafka.Producer.commit_transaction()`.
This call will block until the transaction has been fully committed or
failed (typically due to fencing by a newer producer instance).

Alternatively, if processing fails, or an abortable transaction error is
raised, the transaction needs to be aborted by calling
:py:meth:`confluent_kafka.Producer.abort_transaction()` which marks any produced
messages and offset commits as aborted.

After the current transaction has been committed or aborted a new
transaction may be started by calling
:py:meth:`confluent_kafka.Producer.begin_transaction()` again.

**Retriable errors**

Some error cases allow the attempted operation to be retried, this is
indicated by the error object having the retriable flag set which can
be detected by calling :py:meth:`confluent_kafka.KafkaError.retriable()` on
the KafkaError object.
When this flag is set the application may retry the operation immediately
or preferably after a shorter grace period (to avoid busy-looping).
Retriable errors include timeouts, broker transport failures, etc.

**Abortable errors**

An ongoing transaction may fail permanently due to various errors,
such as transaction coordinator becoming unavailable, write failures to the
Apache Kafka log, under-replicated partitions, etc.
At this point the producer application must abort the current transaction
using :py:meth:`confluent_kafka.Producer.abort_transaction()` and optionally
start a new transaction by calling
:py:meth:`confluent_kafka.Producer.begin_transaction()`.
Whether an error is abortable or not is detected by calling
:py:meth:`confluent_kafka.KafkaError.txn_requires_abort()`.

**Fatal errors**

While the underlying idempotent producer will typically only raise
fatal errors for unrecoverable cluster errors where the idempotency
guarantees can't be maintained, most of these are treated as abortable by
the transactional producer since transactions may be aborted and retried
in their entirety;
The transactional producer on the other hand introduces a set of additional
fatal errors which the application needs to handle by shutting down the
producer and terminate. There is no way for a producer instance to recover
from fatal errors.
Whether an error is fatal or not is detected by calling
:py:meth:`confluent_kafka.KafkaError.fatal()`.

**Handling of other errors**

For errors that have neither retriable, abortable or the fatal flag set
it is not always obvious how to handle them. While some of these errors
may be indicative of bugs in the application code, such as when
an invalid parameter is passed to a method, other errors might originate
from the broker and be passed thru as-is to the application.
The general recommendation is to treat these errors, that have
neither the retriable or abortable flags set, as fatal.

**Error handling example**

.. code-block:: python

    while True:
       try:
           producer.commit_transaction(10.0)
           break
       except KafkaException as e:
           if e.args[0].retriable():
              # retriable error, try again
              continue
           elif e.args[0].txn_requires_abort():
              # abort current transaction, begin a new transaction,
              # and rewind the consumer to start over.
              producer.abort_transaction()
              producer.begin_transaction()
              rewind_consumer_offsets...()
           else:
               # treat all other errors as fatal
               raise

.. _pythonclient_configuration:

Kafka Client Configuration
===========================

Configuration of producer and consumer instances is performed by
providing a dict of configuration properties to the instance constructor, e.g.

.. code-block:: python

  conf = {'bootstrap.servers': 'mybroker.com',
          'group.id': 'mygroup', 
          'session.timeout.ms': 6000,
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

* ``oauth_cb(config_str)``: Callback for retrieving OAuth Bearer token.
  Function argument ``config_str`` is a str from config: ``sasl.oauthbearer.config``.
  Return value of this callback is expected to be ``(token_str, expiry_time)`` tuple
  where ``expiry_time`` is the time in seconds since the epoch as a floating point number.
  This callback is useful only when ``sasl.mechanisms=OAUTHBEARER`` is set and
  is served to get the initial token before a successful broker connection can be made.
  The callback can be triggered by calling ``client.poll()`` or ``producer.flush()``.

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
  For example:

.. code-block:: python

    mylogger = logging.getLogger()
    mylogger.addHandler(logging.StreamHandler())
    producer = confluent_kafka.Producer({'bootstrap.servers': 'mybroker.com'}, logger=mylogger)

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

.. _serde_field:

************
MessageField
************

.. autoclass:: confluent_kafka.serialization.MessageField
   :members:

.. _serde_ctx:

********************
SerializationContext
********************

.. autoclass:: confluent_kafka.serialization.SerializationContext
   :members:

.. _schemaregistry_schema:

******
Schema
******

.. autoclass:: confluent_kafka.schema_registry.Schema
   :members:

.. _schemaregistry_registered_schema:

****************
RegisteredSchema
****************

.. autoclass:: confluent_kafka.schema_registry.RegisteredSchema
   :members:

.. _schemaregistry_error:

*******************
SchemaRegistryError
*******************

.. autoclass:: confluent_kafka.schema_registry.error.SchemaRegistryError
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

.. _pyclient_error_consumer:

************
ConsumeError
************

.. autoclass:: confluent_kafka.error.ConsumeError
   :members:

.. _pyclient_error_producer:

************
ProduceError
************

.. autoclass:: confluent_kafka.error.ProduceError
   :members:

.. _serde_error:

*******************
SerializationError
*******************

.. autoclass:: confluent_kafka.error.SerializationError
   :members:

.. _serde_error_serializer_key:

*********************
KeySerializationError
*********************

.. autoclass:: confluent_kafka.error.KeySerializationError
   :members:

.. _serde_error_serializer_value:

***********************
ValueSerializationError
***********************

.. autoclass:: confluent_kafka.error.ValueSerializationError
   :members:

.. _serde_error_deserializer_key:

***********************
KeyDeserializationError
***********************

.. autoclass:: confluent_kafka.error.KeyDeserializationError
   :members:

.. _serde_error_deserializer_value:

*************************
ValueDeserializationError
*************************

.. autoclass:: confluent_kafka.error.ValueDeserializationError
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
