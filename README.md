<<<<<<< HEAD
# Confluent Python Client for Apache Kafka

[![Try Confluent Cloud - The Data Streaming Platform](https://images.ctfassets.net/8vofjvai1hpv/10bgcSfn5MzmvS4nNqr94J/af43dd2336e3f9e0c0ca4feef4398f6f/confluent-banner-v2.svg)](https://confluent.cloud/signup?utm_source=github&utm_medium=banner&utm_campaign=tm.plg.cflt-oss-repos&utm_term=confluent-kafka-python)
=======
> [!WARNING]
> Due to an error in which we included dependency changes to a recent patch release, Confluent recommends users to **refrain from upgrading to 2.6.2** of Confluent Kafka. Confluent will release a new minor version, 2.7.0, where the dependency changes will be appropriately included. Users who have already upgraded to 2.6.2 and made the required dependency changes are free to remain on that version and are recommended to upgrade to 2.7.0 when that version is available. Upon the release of 2.7.0, the 2.6.2 version will be marked deprecated.
We apologize for the inconvenience and appreciate the feedback that we have gotten from the community.
>>>>>>> 7b378e7 (add accidentally removed md files)

Confluent's Python Client for Apache Kafka<sup>TM</sup>
=======================================================

<<<<<<< HEAD
**confluent-kafka-python** provides a high-level `Producer`, `Consumer` and `AdminClient` compatible with all [Apache Kafka™](http://kafka.apache.org/) brokers >= v0.8, [Confluent Cloud](https://www.confluent.io/confluent-cloud/) and [Confluent Platform](https://www.confluent.io/product/compare/).

**Recommended for Production:** While this client works with any Kafka deployment, it's optimized for and fully supported with [Confluent Cloud](https://www.confluent.io/confluent-cloud/) (fully managed) and [Confluent Platform](https://www.confluent.io/product/compare/) (self-managed), which provide enterprise-grade security, monitoring, and support.

## Why Choose Confluent's Python Client?

Unlike the basic Apache Kafka Python client, `confluent-kafka-python` provides:

- **Production-Ready Performance**: Built on `librdkafka` (C library) for maximum throughput and minimal latency, significantly outperforming pure Python implementations.
- **Enterprise Features**: Schema Registry integration, transactions, exactly-once semantics, and advanced serialization support out of the box.
- **AsyncIO Support**: Native async/await support for modern Python applications - not available in the Apache Kafka client.
- **Comprehensive Serialization**: Built-in Avro, Protobuf, and JSON Schema support with automatic schema evolution handling.
- **Professional Support**: Backed by Confluent's engineering team with enterprise SLAs and 24/7 support options.
- **Active Development**: Continuously updated with the latest Kafka features and performance optimizations.
- **Battle-Tested**: Used by thousands of organizations in production, from startups to Fortune 500 companies.

**Performance Note:** The Apache Kafka Python client (`kafka-python`) is a pure Python implementation that, while functional, has significant performance limitations for high-throughput production use cases. `confluent-kafka-python` leverages the same high-performance C library (`librdkafka`) used by Confluent's other clients, providing enterprise-grade performance and reliability.

## Key Features

- **High Performance & Reliability**: Built on [`librdkafka`](https://github.com/confluentinc/librdkafka), the battle-tested C client for Apache Kafka, ensuring maximum throughput, low latency, and stability. The client is supported by Confluent and is trusted in mission-critical production environments.
- **Comprehensive Kafka Support**: Full support for the Kafka protocol, transactions, and administration APIs.
- **Experimental; AsyncIO Producer**: An experimental fully asynchronous producer (`AIOProducer`) for seamless integration with modern Python applications using `asyncio`.
- **Seamless Schema Registry Integration**: Synchronous and asynchronous clients for Confluent Schema Registry to handle schema management and serialization (Avro, Protobuf, JSON Schema).
- **Improved Error Handling**: Detailed, context-aware error messages and exceptions to speed up debugging and troubleshooting.
- **[Confluent Cloud] Automatic Zone Detection**: Producers automatically connect to brokers in the same availability zone, reducing latency and data transfer costs without requiring manual configuration.
- **[Confluent Cloud] Simplified Configuration Profiles**: Pre-defined configuration profiles optimized for common use cases like high throughput or low latency, simplifying client setup.
- **Enterprise Support**: Backed by Confluent's expert support team with SLAs and 24/7 assistance for production deployments.

## Usage

For a step-by-step guide on using the client, see [Getting Started with Apache Kafka and Python](https://developer.confluent.io/get-started/python/).

### Choosing Your Kafka Deployment

- **[Confluent Cloud](https://www.confluent.io/confluent-cloud/)** - Fully managed service with automatic scaling, security, and monitoring. Best for teams wanting to focus on applications rather than infrastructure.
- **[Confluent Platform](https://www.confluent.io/product/compare/)** - Self-managed deployment with enterprise features, support, and tooling. Ideal for on-premises or hybrid cloud requirements.
- **Apache Kafka** - Open source deployment. Requires manual setup, monitoring, and maintenance.

Additional examples can be found in the [examples](examples) directory or the [confluentinc/examples](https://github.com/confluentinc/examples/tree/master/clients/cloud/python) GitHub repo, which include demonstrations of:

=======
**confluent-kafka-python** provides a high-level Producer, Consumer and AdminClient compatible with all
[Apache Kafka<sup>TM<sup>](http://kafka.apache.org/) brokers >= v0.8, [Confluent Cloud](https://www.confluent.io/confluent-cloud/)
and [Confluent Platform](https://www.confluent.io/product/compare/). The client is:

- **Reliable** - It's a wrapper around [librdkafka](https://github.com/edenhill/librdkafka) (provided automatically via binary wheels) which is widely deployed in a diverse set of production scenarios. It's tested using [the same set of system tests](https://github.com/confluentinc/confluent-kafka-python/tree/master/src/confluent_kafka/kafkatest) as the Java client [and more](https://github.com/confluentinc/confluent-kafka-python/tree/master/tests). It's supported by [Confluent](https://confluent.io).

- **Performant** - Performance is a key design consideration. Maximum throughput is on par with the Java client for larger message sizes (where the overhead of the Python interpreter has less impact). Latency is on par with the Java client.

- **Future proof** - Confluent, founded by the
creators of Kafka, is building a [streaming platform](https://www.confluent.io/product/compare/)
with Apache Kafka at its core. It's high priority for us that client features keep
pace with core Apache Kafka and components of the [Confluent Platform](https://www.confluent.io/product/compare/).


## Usage

For a step-by-step guide on using the client see [Getting Started with Apache Kafka and Python](https://developer.confluent.io/get-started/python/).

Aditional examples can be found in the [examples](examples) directory or the [confluentinc/examples](https://github.com/confluentinc/examples/tree/master/clients/cloud/python) github repo, which include demonstration of:
>>>>>>> 7b378e7 (add accidentally removed md files)
- Exactly once data processing using the transactional API.
- Integration with asyncio.
- (De)serializing Protobuf, JSON, and Avro data with Confluent Schema Registry integration.
- [Confluent Cloud](https://www.confluent.io/confluent-cloud/) configuration.

<<<<<<< HEAD
Also see the [Python client docs](https://docs.confluent.io/kafka-clients/python/current/overview.html) and the [API reference](https://docs.confluent.io/kafka-clients/python/current/).

Finally, the [tests](tests) are useful as a reference for example usage.
### AsyncIO Producer (experimental)

Use the AsyncIO `Producer` inside async applications to avoid blocking the event loop.

```python
import asyncio
from confluent_kafka.experimental.aio import AIOProducer

async def main():
    p = AIOProducer({"bootstrap.servers": "mybroker"})
    try:
        # produce() returns a Future; first await the coroutine to get the Future,
        # then await the Future to get the delivered Message.
        delivery_future = await p.produce("mytopic", value=b"hello")
        delivered_msg = await delivery_future
        # Optionally flush any remaining buffered messages before shutdown
        await p.flush()
    finally:
        await p.close()

asyncio.run(main())
```

Notes:

- Batched async produce buffers messages; delivery callbacks, stats, errors, and logger run on the event loop.
- Per-message headers are not supported in the batched async path. If headers are required, use the synchronous `Producer.produce(...)` (you can offload to a thread in async apps).

For a more detailed example that includes both an async producer and consumer, see
[`examples/asyncio_example.py`](examples/asyncio_example.py).

**Architecture:** For implementation details and component architecture, see the [AIOProducer Architecture Overview](aio_producer_simple_diagram.md).

#### When to use AsyncIO vs synchronous Producer

- **Use AsyncIO `Producer`** when your code runs under an event loop (FastAPI/Starlette, aiohttp, Sanic, asyncio workers) and must not block.
- **Use synchronous `Producer`** for scripts, batch jobs, and highest-throughput pipelines where you control threads/processes and can call `poll()`/`flush()` directly.
- **In async servers**, prefer AsyncIO `Producer`; if you need headers, call sync `produce()` via `run_in_executor` for that path.

#### AsyncIO with Schema Registry

The AsyncIO producer and consumer integrate seamlessly with async Schema Registry serializers. See the [Schema Registry Integration](#schema-registry-integration) section below for full details.

### Basic Producer example
=======
Also refer to the [API documentation](http://docs.confluent.io/current/clients/confluent-kafka-python/index.html).

Finally, the [tests](tests) are useful as a reference for example usage.

### Basic Producer Example
>>>>>>> 7b378e7 (add accidentally removed md files)

```python
from confluent_kafka import Producer

p = Producer({'bootstrap.servers': 'mybroker1,mybroker2'})

def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))

for data in some_data_source:
    # Trigger any available delivery report callbacks from previous produce() calls
    p.poll(0)

    # Asynchronously produce a message. The delivery report callback will
    # be triggered from the call to poll() above, or flush() below, when the
    # message has been successfully delivered or failed permanently.
    p.produce('mytopic', data.encode('utf-8'), callback=delivery_report)

# Wait for any outstanding messages to be delivered and delivery report
# callbacks to be triggered.
p.flush()
```

For a discussion on the poll based producer API, refer to the
[Integrating Apache Kafka With Python Asyncio Web Applications](https://www.confluent.io/blog/kafka-python-asyncio-integration/)
blog post.
<<<<<<< HEAD
### Schema Registry Integration

This client provides full integration with Schema Registry for schema management and message serialization, and is compatible with both [Confluent Platform](https://docs.confluent.io/platform/current/schema-registry/index.html) and [Confluent Cloud](https://docs.confluent.io/cloud/current/sr/index.html). Both synchronous and asynchronous clients are available.

#### Learn more

- [Getting Started with Apache Kafka and Python](https://developer.confluent.io/get-started/python/) – step-by-step course with videos and labs.
- [Schema Registry Basics](https://developer.confluent.io/learn-kafka/stream-processing/sr-schema-registry/) – short tutorial explaining subjects, compatibility, and serialization patterns.

#### Synchronous Client & Serializers

Use the synchronous `SchemaRegistryClient` with the standard `Producer` and `Consumer`.

```python
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField

# Configure Schema Registry Client
schema_registry_conf = {'url': 'http://localhost:8081'}  # Confluent Platform
# For Confluent Cloud, add: 'basic.auth.user.info': '<sr-api-key>:<sr-api-secret>'
# See: https://docs.confluent.io/cloud/current/sr/index.html
schema_registry_client = SchemaRegistryClient(schema_registry_conf)

# 2. Configure AvroSerializer
avro_serializer = AvroSerializer(schema_registry_client,
                                 user_schema_str,
                                 lambda user, ctx: user.to_dict())

# 3. Configure Producer
producer_conf = {
    'bootstrap.servers': 'localhost:9092',
    'key.serializer': StringSerializer('utf_8'),
    'value.serializer': avro_serializer
}
producer = Producer(producer_conf)

# 4. Produce messages
producer.produce('my-topic', key='user1', value=some_user_object)
producer.flush()
```

#### Asynchronous Client & Serializers (AsyncIO)

Use the `AsyncSchemaRegistryClient` and `Async` serializers with `AIOProducer` and `AIOConsumer`. The configuration is the same as the synchronous client.

```python
from confluent_kafka.experimental.aio import AIOProducer
from confluent_kafka.schema_registry import AsyncSchemaRegistryClient
from confluent_kafka.schema_registry._async.avro import AsyncAvroSerializer

# Setup async Schema Registry client and serializer
# (See configuration options in the synchronous example above)
schema_registry_conf = {'url': 'http://localhost:8081'}
schema_client = AsyncSchemaRegistryClient(schema_registry_conf)
serializer = await AsyncAvroSerializer(schema_client, schema_str=avro_schema)

# Use with AsyncIO producer
producer = AIOProducer({"bootstrap.servers": "localhost:9092"})
serialized_value = await serializer(data, SerializationContext("topic", MessageField.VALUE))
delivery_future = await producer.produce("topic", value=serialized_value)
```

Available async serializers: `AsyncAvroSerializer`, `AsyncJSONSerializer`, `AsyncProtobufSerializer` (and corresponding deserializers).

See also:

- Example: [`examples/asyncio_avro_producer.py`](examples/asyncio_avro_producer.py)

#### Import paths

```python
from confluent_kafka.schema_registry._async.avro import AsyncAvroSerializer, AsyncAvroDeserializer
from confluent_kafka.schema_registry._async.json_schema import AsyncJSONSerializer, AsyncJSONDeserializer
from confluent_kafka.schema_registry._async.protobuf import AsyncProtobufSerializer, AsyncProtobufDeserializer
```

**Client-Side Field Level Encryption (CSFLE):** To use Data Contracts rules (including CSFLE), install the `rules` extra (see Install section), and refer to the encryption examples in [`examples/README.md`](examples/README.md). For CSFLE-specific guidance, see the [Confluent Cloud CSFLE documentation](https://docs.confluent.io/cloud/current/security/encrypt/csfle/overview.html).

**Note:** The async Schema Registry interface mirrors the synchronous client exactly - same configuration options, same calling patterns, no unexpected gotchas or limitations. Simply add `await` to method calls and use the `Async` prefixed classes.

#### Troubleshooting

- **401/403 Unauthorized when using Confluent Cloud:** Verify your `basic.auth.user.info` (SR API key/secret) is correct and that the Schema Registry URL is for your specific cluster. Ensure you are using an SR API key, not a Kafka API key.
- **Schema not found:** Check that your `subject.name.strategy` configuration matches how your schemas are registered in Schema Registry, and that the topic and message field (key/value) pairing is correct.
### Basic Consumer example
=======


### Basic Consumer Example
>>>>>>> 7b378e7 (add accidentally removed md files)

```python
from confluent_kafka import Consumer

c = Consumer({
    'bootstrap.servers': 'mybroker',
    'group.id': 'mygroup',
    'auto.offset.reset': 'earliest'
})

c.subscribe(['mytopic'])

while True:
    msg = c.poll(1.0)

    if msg is None:
        continue
    if msg.error():
        print("Consumer error: {}".format(msg.error()))
        continue

    print('Received message: {}'.format(msg.value().decode('utf-8')))

c.close()
```
<<<<<<< HEAD
### Basic AdminClient example
=======


### Basic AdminClient Example
>>>>>>> 7b378e7 (add accidentally removed md files)

Create topics:

```python
from confluent_kafka.admin import AdminClient, NewTopic

a = AdminClient({'bootstrap.servers': 'mybroker'})

new_topics = [NewTopic(topic, num_partitions=3, replication_factor=1) for topic in ["topic1", "topic2"]]
# Note: In a multi-cluster production scenario, it is more typical to use a replication_factor of 3 for durability.

# Call create_topics to asynchronously create topics. A dict
# of <topic,future> is returned.
fs = a.create_topics(new_topics)

# Wait for each operation to finish.
for topic, f in fs.items():
    try:
        f.result()  # The result itself is None
        print("Topic {} created".format(topic))
    except Exception as e:
        print("Failed to create topic {}: {}".format(topic, e))
```
<<<<<<< HEAD
## Thread safety

The `Producer`, `Consumer`, and `AdminClient` are all thread safe.
## Install

```bash
# Basic installation
pip install confluent-kafka

# With Schema Registry support
pip install "confluent-kafka[avro,schemaregistry]"     # Avro
pip install "confluent-kafka[json,schemaregistry]"     # JSON Schema  
pip install "confluent-kafka[protobuf,schemaregistry]" # Protobuf

# With Data Contract rules (includes CSFLE support)
pip install "confluent-kafka[avro,schemaregistry,rules]"
```

**Note:** Pre-built Linux wheels do not include SASL Kerberos/GSSAPI support. For Kerberos, see the source installation instructions in [INSTALL.md](INSTALL.md).
=======


## Thread Safety

The `Producer`, `Consumer` and `AdminClient` are all thread safe.


## Install

**Install self-contained binary wheels**

```bash
pip install confluent-kafka
```

**NOTE:** The pre-built Linux wheels do NOT contain SASL Kerberos/GSSAPI support.
          If you need SASL Kerberos/GSSAPI support you must install librdkafka and
          its dependencies using the repositories below and then build
          confluent-kafka using the instructions in the
          "Install from source" section below.

>>>>>>> 7b378e7 (add accidentally removed md files)
To use Schema Registry with the Avro serializer/deserializer:

```bash
pip install "confluent-kafka[avro,schemaregistry]"
```

To use Schema Registry with the JSON serializer/deserializer:

```bash
pip install "confluent-kafka[json,schemaregistry]"
```

To use Schema Registry with the Protobuf serializer/deserializer:

```bash
pip install "confluent-kafka[protobuf,schemaregistry]"
```

When using Data Contract rules (including CSFLE) add the `rules`extra, e.g.:

```bash
pip install "confluent-kafka[avro,schemaregistry,rules]"
```

**Install from source**

For source install, see the *Install from source* section in [INSTALL.md](INSTALL.md).

<<<<<<< HEAD
## Broker compatibility

The Python client (as well as the underlying C library librdkafka) supports
all broker versions >= 0.8.
=======

## Broker Compatibility

The Python client (as well as the underlying C library librdkafka) supports
all broker versions &gt;= 0.8.
>>>>>>> 7b378e7 (add accidentally removed md files)
But due to the nature of the Kafka protocol in broker versions 0.8 and 0.9 it
is not safe for a client to assume what protocol version is actually supported
by the broker, thus you will need to hint the Python client what protocol
version it may use. This is done through two configuration settings:

<<<<<<< HEAD
- `broker.version.fallback=YOUR_BROKER_VERSION` (default 0.9.0.1)
- `api.version.request=true|false` (default true)

When using a Kafka 0.10 broker or later you don't need to do anything
=======
 * `broker.version.fallback=YOUR_BROKER_VERSION` (default 0.9.0.1)
 * `api.version.request=true|false` (default true)

When using a Kafka 0.10 broker or later you don't need to do anything
(`api.version.request=true` is the default).
If you use Kafka broker 0.9 or 0.8 you must set
`api.version.request=false` and set
`broker.version.fallback` to your broker version,
e.g `broker.version.fallback=0.9.0.1`.

More info here:
https://github.com/edenhill/librdkafka/wiki/Broker-version-compatibility


## SSL certificates

If you're connecting to a Kafka cluster through SSL you will need to configure
the client with `'security.protocol': 'SSL'` (or `'SASL_SSL'` if SASL
authentication is used).

The client will use CA certificates to verify the broker's certificate.
The embedded OpenSSL library will look for CA certificates in `/usr/lib/ssl/certs/`
or `/usr/lib/ssl/cacert.pem`. CA certificates are typically provided by the
Linux distribution's `ca-certificates` package which needs to be installed
through `apt`, `yum`, et.al.

If your system stores CA certificates in another location you will need to
configure the client with `'ssl.ca.location': '/path/to/cacert.pem'`.

Alternatively, the CA certificates can be provided by the [certifi](https://pypi.org/project/certifi/)
Python package. To use certifi, add an `import certifi` line and configure the
client's CA location with `'ssl.ca.location': certifi.where()`.


## License

[Apache License v2.0](http://www.apache.org/licenses/LICENSE-2.0)

KAFKA is a registered trademark of The Apache Software Foundation and has been licensed for use
by confluent-kafka-python. confluent-kafka-python has no affiliation with and is not endorsed by
The Apache Software Foundation.


## Developer Notes

Instructions on building and testing confluent-kafka-python can be found [here](DEVELOPER.md).


## Confluent Cloud

For a step-by-step guide on using the Python client with Confluent Cloud see [Getting Started with Apache Kafka and Python](https://developer.confluent.io/get-started/python/) on [Confluent Developer](https://developer.confluent.io/). 
>>>>>>> 7b378e7 (add accidentally removed md files)
