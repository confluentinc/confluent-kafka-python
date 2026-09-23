Starting with v2.16.0, ``confluent_kafka`` declares itself GIL-safe
(`PEP 703 <https://peps.python.org/pep-0703/>`_) and can run under
free-threaded (No-GIL) CPython builds without the GIL being re-enabled.


***********************
Thread-safety reference
***********************

.. list-table::
   :header-rows: 1
   :widths: 25 20 55

   * - Class
     - Safe to share across threads?
     - Notes
   * - :py:class:`~confluent_kafka.Producer`
     - Yes
     - Sharing a single instance across threads and running one producer
       per thread are both supported.
   * - :py:class:`~confluent_kafka.admin.AdminClient`
     - Yes
     - Same model as ``Producer``.
   * - :py:class:`~confluent_kafka.Consumer`
     - Yes (serialized, not recommended)
     - See `Consumer: cross-thread access`_.
   * - :py:class:`~confluent_kafka.Message`
     - Yes
     - Safe to read and mutate from multiple threads on the same instance.
   * - :py:class:`~confluent_kafka.ShareConsumer` /
       :py:class:`~confluent_kafka.DeserializingShareConsumer` (Preview)
     - No
     - Not thread-safe. Concurrent use raises
       :py:exc:`~confluent_kafka.ConcurrentModificationException`. See the
       `Share Consumer guide <docs/kip-932-share-consumer.md>`_.

``AIOProducer`` wraps ``Producer`` and is safe to call concurrently from
multiple tasks on the same event loop.

``AIOConsumer`` runs blocking ``Consumer`` calls on a thread pool, so the
same cross-thread serialization described for ``Consumer`` applies to
concurrent tasks calling it, even from the same event loop. It also has its
own callback rules and worker-pool sizing considerations -- see
`AIOConsumer considerations`_ below.

*************************
Recommended usage pattern
*************************

For ``Producer``, either pattern works: run one producer per thread, or
share a single instance across threads with each thread producing to a
different partition to minimize internal lock contention.

For ``Consumer``, run one instance per thread; see
`Consumer: cross-thread access`_ for why.

*****************************
Consumer: cross-thread access
*****************************

librdkafka's consumer is not thread-safe, so sharing a single ``Consumer``
instance across threads is not recommended.

To maintain backward compatibility, cross-thread access is now serialized
instead. A call from a second thread waits for the in-progress call on the
first thread to finish before proceeding, rather than running concurrently
(which could corrupt state) or being rejected outright. This wait has no
timeout, so in the worst case -- for example, a caller blocked in
``poll(-1)`` -- another caller can starve, waiting indefinitely.

Reentrant calls from the *same* logical caller (for example, a rebalance
callback calling back into the ``Consumer`` that invoked it) are still
admitted immediately.

The net effect: sharing one ``Consumer`` instance across threads no longer
crashes or corrupts state, but it does not parallelize either. We recommend
running one consumer per thread.

****************************
AIOConsumer considerations
****************************

Worker-pool sizing
   ``AIOConsumer`` runs blocking ``Consumer`` calls on a thread pool, so its
   size matters. The default ``max_workers`` was raised from 2 to 100 as
   part of this work. Size it for how many callers you expect to have in
   flight at once, not just for reentrancy. A call that collides with
   another caller now waits for its turn instead of being rejected, and
   that wait ties up a worker the whole time. If a new, unrelated caller
   collides while a reentrant callback chain is still running, it's
   competing with that chain for the same worker -- with too few workers,
   they can end up starving each other indefinitely.

Callbacks
   Inside a callback, do not ``gather()`` or leave a ``create_task()``
   running past the end of the callback. Make calls into the consumer one
   at a time and ``await`` each one before the callback returns.

*******************************
Extras on free-threaded Python
*******************************

The following extras have a caveat on free-threaded Python, as they have
one or more dependencies that do not support free threading.

.. list-table::
   :header-rows: 1
   :widths: 12 28 60

   * - Extra
     - Deps without free-threaded support
     - Impact
   * - ``avro``
     - ``fastavro``
     - Importing it silently re-enables the GIL for the rest of the
       process. Applications requiring Avro (de)serialization should
       remain on the standard (GIL) build.
   * - ``rules``
     - ``tink``, ``google-re2``, ``grpcio``
     - Applications requiring client-side field-level encryption or
       data-quality rules should remain on the standard (GIL) build.
   * - ``json-fast``
     - ``orjson``
     - Omit it. The client automatically falls back to the
       standard-library JSON codec, so no application change is needed.
   * - ``protobuf``
     - ``protobuf``
     - On free threaded interpreter ``pip`` automatically falls back to
       the pure-Python backend, which is safe but slower.

The above reflects upstream status as of this writing and can go stale --
these projects may ship free-threaded wheels or declare GIL-safety at any
time. Verify current status yourself rather than relying on this list
long-term.
