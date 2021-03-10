# Confluent's Python client for Apache Kafka

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

confluent-kafka-python is based on librdkafka v1.6.2, see the
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

