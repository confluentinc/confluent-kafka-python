Starting with this **Preview** release, confluent-kafka-python supports the
KIP-932 *Queues for Kafka* share consumer through the
:py:class:`~confluent_kafka.ShareConsumer` class.

.. note::

   The share consumer is a **Preview** feature. Its public interfaces may
   change before General Availability and it is **not recommended for
   production use**. See `Current Limitations`_ below.

********
Overview
********

Share groups (`KIP-932 <https://cwiki.apache.org/confluence/display/KAFKA/KIP-932%3A+Queues+for+Kafka>`_)
bring queue-like semantics to Apache Kafka.

-  **What is different from a consumer group:**

   A classic consumer group assigns each partition to exactly **one** member at
   a time. A **share group** lets **multiple** members consume from the **same**
   partitions cooperatively. The unit of progress is the individual **record**
   rather than the partition offset: each delivered record is *acquired* by a
   member, processed, and then *acknowledged*. Records that are not acknowledged
   in time, or that are explicitly released, become available again and may be
   redelivered (possibly to a different member). This lets you scale the number
   of consumers beyond the number of partitions and distribute work like a
   traditional queue.

-  **Distinct client:**

   A share consumer is a separate handle, :py:class:`~confluent_kafka.ShareConsumer`
   (and :py:class:`~confluent_kafka.DeserializingShareConsumer`), not the regular
   :py:class:`~confluent_kafka.Consumer`. Partition assignment is entirely
   broker-driven via the share-group heartbeat; there is **no** rebalance callback
   and **no** ``assign()`` step.

************
Requirements
************

-  **Broker:** share groups must be enabled on the broker. They are generally
   available in **Apache Kafka 4.2.0** (early access in 4.0.0, preview in 4.1.0).
-  **Client:** a confluent-kafka-python release that includes the Preview share
   consumer.

**********
Enablement
**********

Create a :py:class:`~confluent_kafka.ShareConsumer` with a configuration dict.
``group.id`` is required. The acknowledgement mode is selected with the
optional ``share.acknowledgement.mode`` property and defaults to ``implicit``.

.. code:: python

   from confluent_kafka import ShareConsumer

   conf = {
       'bootstrap.servers': 'localhost:9092',
       'group.id': 'my-share-group',
       # Optional, defaults to 'implicit'. Use 'explicit' to acknowledge
       # each record yourself (see Acknowledgement Modes).
       # 'share.acknowledgement.mode': 'explicit',
   }

   consumer = ShareConsumer(conf)
   consumer.subscribe(['my-topic'])

Several regular-consumer configuration properties do not apply to share
consumers (for example ``partition.assignment.strategy``,
``enable.auto.commit``, ``auto.offset.reset``, ``isolation.level``,
``enable.partition.eof``, ``group.instance.id`` and the per-partition fetch-queue
tuning). A few network defaults also differ. See the
:ref:`Configuration Guide <pythonclient_configuration>`, where such properties
are annotated, for the authoritative list and the share-consumer defaults.

******************
Available Features
******************

:py:class:`~confluent_kafka.ShareConsumer` provides:

-  **Subscription:** :py:func:`~confluent_kafka.ShareConsumer.subscribe`
   (exact topic names only; wildcard/regex subscriptions are not supported),
   :py:func:`~confluent_kafka.ShareConsumer.unsubscribe`,
   :py:func:`~confluent_kafka.ShareConsumer.subscription`.
-  **Batch polling:** :py:func:`~confluent_kafka.ShareConsumer.poll` returns a
   :py:class:`~confluent_kafka.Messages` batch (a read-only sequence supporting
   iteration, ``len()``, indexing and slicing, plus ``count()``, ``is_empty()``
   and ``records()``) rather than a single message. ``max.poll.records``
   (default 500) bounds the batch size.
-  **Acknowledgement:** :py:func:`~confluent_kafka.ShareConsumer.acknowledge`
   and :py:func:`~confluent_kafka.ShareConsumer.acknowledge_offset` with the
   :py:class:`~confluent_kafka.AcknowledgeType` types ``ACCEPT`` / ``RELEASE`` /
   ``REJECT``; :py:func:`Message.delivery_count() <confluent_kafka.Message.delivery_count>`
   for poison-record detection.
-  **Commit:** :py:func:`~confluent_kafka.ShareConsumer.commit_sync` (blocks and
   returns per-partition results) and
   :py:func:`~confluent_kafka.ShareConsumer.commit_async`, plus an
   acknowledgement-commit callback set with
   :py:func:`~confluent_kafka.ShareConsumer.set_acknowledgement_commit_callback`.
-  **Lifecycle:** :py:func:`~confluent_kafka.ShareConsumer.close`, context-manager
   support (``with ShareConsumer(conf) as sc:``), and
   :py:func:`~confluent_kafka.ShareConsumer.set_sasl_credentials`.

*********************
Acknowledgement Modes
*********************

The mode is fixed at construction by ``share.acknowledgement.mode``:

-  **implicit** (default): the application does **not** call ``acknowledge()``.
   All records returned by a poll are automatically accepted (``ACCEPT``) on the
   next :py:func:`~confluent_kafka.ShareConsumer.poll`,
   :py:func:`~confluent_kafka.ShareConsumer.commit_sync` or
   :py:func:`~confluent_kafka.ShareConsumer.commit_async`.
-  **explicit**: the application **must** acknowledge every record returned by a
   poll, using :py:func:`~confluent_kafka.ShareConsumer.acknowledge` (or
   :py:func:`~confluent_kafka.ShareConsumer.acknowledge_offset`), before the next
   poll. If any record from the previous batch is still unacknowledged, the next
   :py:func:`~confluent_kafka.ShareConsumer.poll` raises
   :py:exc:`~confluent_kafka.IllegalStateException`.

*********************
Acknowledgement Types
*********************

Each acquired record is acknowledged with one of
:py:class:`~confluent_kafka.AcknowledgeType`:

-  ``ACCEPT`` — processed successfully.
-  ``RELEASE`` — not processed; make the record available again for redelivery
   (use for transient failures that should be retried).
-  ``REJECT`` — do not deliver the record again (use for permanent failures, such
   as a "poison" record).

:py:func:`Message.delivery_count() <confluent_kafka.Message.delivery_count>`
reports how many times a record has been delivered, which can be used to
``REJECT`` a record after a threshold.

Acknowledgements are sent to the broker as part of the next poll, or flushed
explicitly with :py:func:`~confluent_kafka.ShareConsumer.commit_async`
(fire-and-forget) or :py:func:`~confluent_kafka.ShareConsumer.commit_sync`
(blocks for the broker replies and returns a ``dict`` mapping each
:py:class:`~confluent_kafka.TopicPartition` to an optional
:py:exc:`~confluent_kafka.KafkaException`; ``None`` means that partition's
acknowledgements succeeded).

The acknowledgement-commit callback registered with
:py:func:`~confluent_kafka.ShareConsumer.set_acknowledgement_commit_callback`
is invoked as ``callback(offsets, exception)``, where ``offsets`` is a
``dict`` mapping each :py:class:`~confluent_kafka.TopicPartition` to the
``set`` of acknowledged offsets and ``exception`` is a single
:py:exc:`~confluent_kafka.KafkaException` covering all the topic partitions in
``offsets`` (or ``None`` on success) — not a per-partition mapping like
``commit_sync``.

The callback runs on the application thread during
:py:func:`~confluent_kafka.ShareConsumer.poll`,
:py:func:`~confluent_kafka.ShareConsumer.commit_sync`,
:py:func:`~confluent_kafka.ShareConsumer.commit_async` or
:py:func:`~confluent_kafka.ShareConsumer.close` (never on a background thread),
so keep calling the consumer to observe acknowledgement results.

Calling any ShareConsumer method from within the callback raises
:py:exc:`~confluent_kafka.IllegalStateException`.

**************
Usage Examples
**************

The snippets below are guidelines that illustrate the consume/acknowledge loop;
they are not complete programs. Full runnable examples are in the
`examples folder <https://github.com/confluentinc/confluent-kafka-python/tree/master/examples>`_.

Implicit acknowledgement (default)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

In implicit mode the application just processes the records; they are accepted
automatically on the next call.

.. code:: python

   from confluent_kafka import ShareConsumer

   consumer = ShareConsumer({
       'bootstrap.servers': 'localhost:9092',
       'group.id': 'my-share-group',
   })
   consumer.subscribe(['my-topic'])

   try:
       while running:
           messages = consumer.poll(timeout=1.0)  # a Messages batch, possibly empty
           for msg in messages:
               if msg.error():
                   # Record-level error; just inspect msg.error() (see Error Handling).
                   continue
               process(msg)  # auto-accepted on the next poll
   finally:
       consumer.close()

Explicit acknowledgement with commit
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

In explicit mode every successfully received record must be acknowledged before
the next poll. ``RELEASE`` a transient failure so it is redelivered; ``REJECT``
a permanent one.

.. code:: python

   from confluent_kafka import ShareConsumer, AcknowledgeType

   consumer = ShareConsumer({
       'bootstrap.servers': 'localhost:9092',
       'group.id': 'my-share-group',
       'share.acknowledgement.mode': 'explicit',
   })
   consumer.subscribe(['my-topic'])

   try:
       while running:
           messages = consumer.poll(timeout=1.0)
           for msg in messages:
               if msg.error():
                   # Already RELEASE/REJECT-ed internally; the app may
                   # re-acknowledge it if required.
                   continue
               try:
                   process(msg)
                   consumer.acknowledge(msg, AcknowledgeType.ACCEPT)
               except TransientError:
                   consumer.acknowledge(msg, AcknowledgeType.RELEASE)
               except Exception:
                   consumer.acknowledge(msg, AcknowledgeType.REJECT)

           # Flush acknowledgements. commit_sync returns a per-partition result;
           # a None value means that partition succeeded.
           results = consumer.commit_sync(timeout=10.0)
           for tp, exc in results.items():
               if exc is not None:
                   print(f"commit failed for {tp.topic} [{tp.partition}]: {exc}")
   finally:
       consumer.close()

Use :py:func:`~confluent_kafka.ShareConsumer.commit_async` instead of
``commit_sync`` to dispatch the acknowledgements without blocking; the
per-partition result is then delivered to the callback registered with
:py:func:`~confluent_kafka.ShareConsumer.set_acknowledgement_commit_callback`.

Context manager
^^^^^^^^^^^^^^^

:py:class:`~confluent_kafka.ShareConsumer` supports the context-manager protocol,
which closes the consumer automatically:

.. code:: python

   with ShareConsumer(conf) as consumer:
       consumer.subscribe(['my-topic'])
       while running:
           for msg in consumer.poll(timeout=1.0):
               ...

Deserializing share consumer
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

:py:class:`~confluent_kafka.DeserializingShareConsumer` adds
``key.deserializer`` / ``value.deserializer`` support, including Schema Registry
deserializers. It overrides :py:func:`~confluent_kafka.ShareConsumer.poll` to
deserialize each record in place.

.. code:: python

   from confluent_kafka import DeserializingShareConsumer
   from confluent_kafka.serialization import StringDeserializer

   consumer = DeserializingShareConsumer({
       'bootstrap.servers': 'localhost:9092',
       'group.id': 'my-share-group',
       'key.deserializer': StringDeserializer(),
       'value.deserializer': StringDeserializer(),
   })
   consumer.subscribe(['my-topic'])

   for msg in consumer.poll(timeout=1.0):
       if msg.error():
           ...  # deserialization or broker/record error (see below)
       else:
           use(msg.key(), msg.value())  # already deserialized

A deserialization failure does **not** discard the rest of the batch: the
affected record is returned with its **raw bytes preserved** and its
:py:func:`Message.error() <confluent_kafka.Message.error>` set to a
``_KEY_DESERIALIZATION`` or ``_VALUE_DESERIALIZATION`` error, so the application
can detect it with the same ``if msg.error():`` check it uses for broker errors.

Unlike broker / record-level errors, a deserialization failure is **not
acknowledged for you** today. In **implicit** mode the failed record is therefore
auto-accepted (``ACCEPT``) on the next poll — treated as processed and not
redelivered. If you need different handling, use **explicit** mode and
acknowledge it appropriately (for example ``RELEASE`` to retry or ``REJECT`` to
drop).

**************
Error Handling
**************

The share consumer surfaces errors at three levels:

-  **API-level (call-level) errors** are raised as exceptions:

   -  :py:exc:`~confluent_kafka.ConcurrentModificationException` if the
      handle is used concurrently from more than one thread (the share consumer
      is **not thread-safe**; see `Thread Safety`_).
   -  :py:exc:`~confluent_kafka.IllegalStateException` if a method is called
      in an invalid state — for example polling while not subscribed, polling in
      explicit mode before all previous records are acknowledged, acknowledging in
      implicit mode, using the consumer after it is closed, calling any method
      from within the acknowledgement-commit callback, or otherwise calling the
      consumer APIs in any wrong state.
   -  :py:exc:`~confluent_kafka.KafkaException` for other call-level failures.

-  **Record-level errors** are reported on the message via
   :py:func:`Message.error() <confluent_kafka.Message.error>`, with the topic,
   partition and offset intact. The application should check ``msg.error()`` on
   each record before treating it as data. For these records librdkafka has
   already applied the acknowledgement internally based on the error type
   (``RELEASE`` for decompression failures, ``REJECT`` for corrupt/unsupported
   batches); the application can re-acknowledge them if required.

-  **Acknowledgement errors** are reported through the commit result, not
   raised: the :py:func:`~confluent_kafka.ShareConsumer.commit_sync` return value
   maps each :py:class:`~confluent_kafka.TopicPartition` to an optional
   :py:exc:`~confluent_kafka.KafkaException` (``None`` on success), while the
   acknowledgement-commit callback used by
   :py:func:`~confluent_kafka.ShareConsumer.commit_async` reports a single
   :py:exc:`~confluent_kafka.KafkaException` for the commit (see
   `Acknowledgement Types`_ for the callback signature).

*************
Thread Safety
*************

The share consumer is **not thread-safe by design** — a single
:py:class:`~confluent_kafka.ShareConsumer` instance must not be used concurrently
from multiple threads. This follows the share-consumer design in
`KIP-932 <https://cwiki.apache.org/confluence/display/KAFKA/KIP-932%3A+Queues+for+Kafka>`_,
where the share consumer, like the classic consumer, is single-threaded and the
application owns the threading model (typically one instance per thread).
Concurrent use is detected on a best-effort basis and raises
:py:exc:`~confluent_kafka.ConcurrentModificationException`.

*******************
Current Limitations
*******************

This Preview differs from the Apache Kafka Java share consumer in a number of
ways:

-  **Record limit is a soft bound.** ``max.poll.records`` (default 500) does not
   strictly bound the number of records returned per poll (the strict-limit /
   acquire-mode work of KIP-1206 is not implemented).
-  **Acknowledgement types are limited to ACCEPT / RELEASE / REJECT.** There is
   no acquisition-lock renewal (KIP-1222), so a long-running handler cannot
   extend a record's lock and the record may be redelivered.
-  **No wakeup API.** A blocking poll/commit can only be ended by its timeout.
-  **No share-group admin operations** (describe / list / alter / delete share
   groups and offsets) and **no share-consumer client-metrics APIs**.
-  **No auto topic creation.** Share topics are resolved by topic id;
   ``allow.auto.create.topics`` has no effect.
-  **close() takes no timeout argument**; close is bounded internally to roughly
   ``socket.timeout.ms``.
-  **No async share consumer.** The share consumer is available on the
   synchronous client only; an asynchronous share consumer is not available yet.
