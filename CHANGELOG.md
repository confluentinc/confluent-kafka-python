# Confluent's Python client for Apache Kafka

## v1.9.2

v1.9.2 is a maintenance release with the following fixes and enhancements:

 - Support for setting principal and SASL extensions in oauth_cb
   and handle failures (@Manicben, #1402)
 - Wheel for macOS M1/arm64
 - KIP-140 Admin API ACL fix:
   When requesting multiple create_acls or delete_acls operations,
   if the provided ACL bindings or ACL binding filters are not
   unique, an exception will be thrown immediately rather than later
   when the responses are read. (#1370).
 - KIP-140 Admin API ACL fix:
   Better documentation of the describe and delete ACLs behavior
   when using the MATCH resource patter type in a filter. (#1373).
 - Avro serialization examples:
   added a parameter for using a generic or specific Avro schema. (#1381).

confluent-kafka-python is based on librdkafka v1.9.2, see the
[librdkafka release notes](https://github.com/edenhill/librdkafka/releases/tag/v1.9.2)
for a complete list of changes, enhancements, fixes and upgrade considerations.


## v1.9.1

There was no 1.9.1 release of the Python Client.


## v1.9.0

This is a feature release:

 - OAUTHBEARER OIDC support
 - KIP-140 Admin API ACL support

### Fixes

 - The warnings for `use.deprecated.format` (introduced in v1.8.2)
   had its logic reversed, which result in warning logs to be emitted when
   the property was correctly configured, and the log message itself also
   contained text that had it backwards.
   The warning is now only emitted when `use.deprecated.format` is set
   to the old legacy encoding (`True`). #1265
 - Use `str(Schema)` rather than `Schema.to_json` to prevent fastavro
   from raising exception `TypeError: unhashable type: 'mappingproxy'`.
   (@ffissore, #1156, #1197)
 - Fix the argument order in the constructor signature for
   AvroDeserializer/Serializer: the argument order in the constructor
   signature for AvroDeserializer/Serializer was altered in v1.6.1, but
   the example is not changed yet. (@DLT1412, #1263)
 - Fix the json deserialization errors from `_schema_loads` for
   valid primitive declarations. (@dylrich, #989)

confluent-kafka-python is based on librdkafka v1.9.0, see the
[librdkafka release notes](https://github.com/edenhill/librdkafka/releases/tag/v1.9.0)
for a complete list of changes, enhancements, fixes and upgrade considerations.


## v1.8.2

v1.8.2 is a maintenance release with the following fixes and enhancements:

 - **IMPORTANT**: Added mandatory `use.deprecated.format` to
   `ProtobufSerializer` and `ProtobufDeserializer`.
   See **Upgrade considerations** below for more information.
 - **Python 2.7 binary wheels are no longer provided.**
   Users still on Python 2.7 will need to build confluent-kafka from source
   and install librdkafka separately, see [README.md](README.md#Prerequisites)
   for build instructions.
 - Added `use.latest.version` and `skip.known.types` (Protobuf) to
   the Serializer classes. (Robert Yokota, #1133).
 - `list_topics()` and `list_groups()` added to AdminClient.
 - Added support for headers in the SerializationContext (Laurent Domenech-Cabaud)
 - Fix crash in header parsing (Armin Ronacher, #1165)
 - Added long package description in setuptools (Bowrna, #1172).
 - Documentation fixes by Aviram Hassan and Ryan Slominski.
 - Don't raise AttributeError exception when CachedSchemaRegistryClient
   constructor raises a valid exception.

confluent-kafka-python is based on librdkafka v1.8.2, see the
[librdkafka release notes](https://github.com/edenhill/librdkafka/releases/tag/v1.8.2)
for a complete list of changes, enhancements, fixes and upgrade considerations.

**Note**: There were no v1.8.0 and v1.8.1 releases.


## Upgrade considerations

### Protobuf serialization format changes

Prior to this version the confluent-kafka-python client had a bug where
nested protobuf schemas indexes were incorrectly serialized, causing
incompatibility with other Schema-Registry protobuf consumers and producers.

This has now been fixed, but since the old defect serialization and the new
correct serialization are mutually incompatible the user of
confluent-kafka-python will need to make an explicit choice which
serialization format to use during a transitory phase while old producers and
consumers are upgraded.

The `ProtobufSerializer` and `ProtobufDeserializer` constructors now
both take a (for the time being) configuration dictionary that requires
the `use.deprecated.format` configuration property to be explicitly set.

Producers should be upgraded first and as long as there are old (<=v1.7.0)
Python consumers reading from topics being produced to, the new (>=v1.8.2)
Python producer must be configured with `use.deprecated.format` set to `True`.

When all existing messages in the topic have been consumed by older consumers
the consumers should be upgraded and both new producers and the new consumers
must set `use.deprecated.format` to `False`.


The requirement to explicitly set `use.deprecated.format` will be removed
in a future version and the setting will then default to `False` (new format).






## v1.7.0

v1.7.0 is a maintenance release with the following fixes and enhancements:

- Add error_cb to confluent_cloud.py example (#1096).
- Clarify that doc output varies based on method (@slominskir, #1098).
- Docs say Schema when they mean SchemaReference (@slominskir, #1092).
- Add documentation for NewTopic and NewPartitions (#1101).

confluent-kafka-python is based on librdkafka v1.7.0, see the
[librdkafka release notes](https://github.com/edenhill/librdkafka/releases/tag/v1.7.0)
for a complete list of changes, enhancements, fixes and upgrade considerations.


## v1.6.1

v1.6.1 is a feature release:

 - KIP-429 - Incremental consumer rebalancing support.
 - OAUTHBEARER support.

### Fixes

 - Add `return_record_name=True` to AvroDeserializer (@slominskir, #1028)
 - Fix deprecated `schema.Parse` call (@casperlehmann, #1006).
 - Make reader schema optional in AvroDeserializer (@97nitt, #1000).
 - Add `**kwargs` to legacy AvroProducer and AvroConsumer constructors to
   support all Consumer and Producer base class constructor arguments, such
   as `logger` (@venthur, #699).
 - Add bool for permanent schema delete (@slominskir, #1029).
 - The avro package is no longer required for Schema-Registry support (@jaysonsantos, #950).
 - Only write to schema cache once, improving performance (@fimmtiu, #724).
 - Improve Schema-Registry error reporting (@jacopofar, #673).
 - `producer.flush()` could return a non-zero value without hitting the
   specified timeout.


confluent-kafka-python is based on librdkafka v1.6.1, see the
[librdkafka release notes](https://github.com/edenhill/librdkafka/releases/tag/v1.6.1)
for a complete list of changes, enhancements, fixes and upgrade considerations.


## v1.6.0

v1.6.0 is a feature release with the following features, fixes and enhancements:

 - Bundles librdkafka v1.6.0 which adds support for Incremental rebalancing,
   Sticky producer partitioning, Transactional producer scalabilty improvements,
   and much much more. See link to release notes below.
 - Rename asyncio.py example to avoid circular import (#945)
 - The Linux wheels are now built with manylinux2010 (rather than manylinux1)
   since OpenSSL v1.1.1 no longer builds on CentOS 5. Older Linux distros may
   thus no longer be supported, such as CentOS 5.
 - The in-wheel OpenSSL version has been updated to 1.1.1i.
 - Added `Message.latency()` to retrieve the per-message produce latency.
 - Added trove classifiers.
 - Consumer destructor will no longer trigger consumer_close(),
   `consumer.close()` must now be explicitly called if the application
   wants to leave the consumer group properly and commit final offsets.
 - Fix `PY_SSIZE_T_CLEAN` warning
 - Move confluent_kafka/ to src/ to avoid pytest/tox picking up the local dir
 - Added `producer.purge()` to purge messages in-queue/flight (@peteryin21, #548)
 - Added `AdminClient.list_groups()` API (@messense, #948)
 - Rename asyncio.py example to avoid circular import (#945)

confluent-kafka-python is based on librdkafka v1.6.0, see the
[librdkafka release notes](https://github.com/edenhill/librdkafka/releases/tag/v1.6.0)
for a complete list of changes, enhancements, fixes and upgrade considerations.


## v1.5.2

v1.5.2 is a maintenance release with the following fixes and enhancements:

 - Add producer purge method with optional blocking argument (@peteryin21, #548)
 - Add AdminClient.list_groups API (@messense, #948)
 - Rename asyncio.py example to avoid circular import (#945)
 - Upgrade bundled OpenSSL to v1.1.1h (from v1.0.2u)
 - The Consumer destructor will no longer trigger `consumer.close()`
   callbacks, `consumer.close()` must now be explicitly called to cleanly
   close down the consumer and leave the group.
 - Fix `PY_SSIZE_T_CLEAN` warning in calls to produce().
 - Restructure source tree to avoid undesired local imports of confluent_kafka
   when running pytest.

confluent-kafka-python is based on librdkafka v1.5.2, see the
[librdkafka release notes](https://github.com/edenhill/librdkafka/releases/tag/v1.5.2)
for a complete list of changes, enhancements, fixes and upgrade considerations.


**Note: There was no v1.5.1 release**


## v1.5.0

v1.5.0 is a maintenance release with the following fixes and enhancements:

 - Bundles librdkafka v1.5.0 - see release notes for all enhancements and fixes.
 - Documentation fixes
 - [Dockerfile examples](examples/docker)
 - [List offsets example](examples/list_offsets.py)

confluent-kafka-python is based on librdkafka v1.5.0, see the
[librdkafka release notes](https://github.com/edenhill/librdkafka/releases/tag/v1.5.0)
for a complete list of changes, enhancements, fixes and upgrade considerations.

