Confluent's Python Client for Apache Kafka<sup>TM</sup>
=======================================================

**confluent-kafka-python** provides a high-level Producer, Consumer and AdminClient compatible with all
[Apache Kafka<sup>TM<sup>](http://kafka.apache.org/) brokers >= v0.8, [Confluent Cloud](https://www.confluent.io/confluent-cloud/)
and the [Confluent Platform](https://www.confluent.io/product/compare/). The client is:

- **Reliable** - It's a wrapper around [librdkafka](https://github.com/edenhill/librdkafka) (provided automatically via binary wheels) which is widely deployed in a diverse set of production scenarios. It's tested using [the same set of system tests](https://github.com/confluentinc/confluent-kafka-python/tree/master/src/confluent_kafka/kafkatest) as the Java client [and more](https://github.com/confluentinc/confluent-kafka-python/tree/master/tests). It's supported by [Confluent](https://confluent.io).

- **Performant** - Performance is a key design consideration. Maximum throughput is on par with the Java client for larger message sizes (where the overhead of the Python interpreter has less impact). Latency is on par with the Java client.

- **Future proof** - Confluent, founded by the
creators of Kafka, is building a [streaming platform](https://www.confluent.io/product/compare/)
with Apache Kafka at its core. It's high priority for us that client features keep
pace with core Apache Kafka and components of the [Confluent Platform](https://www.confluent.io/product/compare/).


See the [API documentation](http://docs.confluent.io/current/clients/confluent-kafka-python/index.html) for more info.

For a step-by-step guide on using the client see [Getting Started with Apache Kafka and Python](https://developer.confluent.io/get-started/python/).


Usage
=====

Below are some examples of typical usage. For more examples, see the [examples](examples) directory or the [confluentinc/examples](https://github.com/confluentinc/examples/tree/master/clients/cloud/python) github repo for a [Confluent Cloud](https://www.confluent.io/confluent-cloud/) example.


**Producer**

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

    # Asynchronously produce a message, the delivery report callback
    # will be triggered from poll() above, or flush() below, when the message has
    # been successfully delivered or failed permanently.
    p.produce('mytopic', data.encode('utf-8'), callback=delivery_report)

# Wait for any outstanding messages to be delivered and delivery report
# callbacks to be triggered.
p.flush()
```


**High-level Consumer**

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

**AvroProducer**

```python
from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer


value_schema_str = """
{
   "namespace": "my.test",
   "name": "value",
   "type": "record",
   "fields" : [
     {
       "name" : "name",
       "type" : "string"
     }
   ]
}
"""

key_schema_str = """
{
   "namespace": "my.test",
   "name": "key",
   "type": "record",
   "fields" : [
     {
       "name" : "name",
       "type" : "string"
     }
   ]
}
"""

value_schema = avro.loads(value_schema_str)
key_schema = avro.loads(key_schema_str)
value = {"name": "Value"}
key = {"name": "Key"}


def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))


avroProducer = AvroProducer({
    'bootstrap.servers': 'mybroker,mybroker2',
    'on_delivery': delivery_report,
    'schema.registry.url': 'http://schema_registry_host:port'
    }, default_key_schema=key_schema, default_value_schema=value_schema)

avroProducer.produce(topic='my_topic', value=value, key=key)
avroProducer.flush()
```

**AvroConsumer**

```python
from confluent_kafka.avro import AvroConsumer
from confluent_kafka.avro.serializer import SerializerError


c = AvroConsumer({
    'bootstrap.servers': 'mybroker,mybroker2',
    'group.id': 'groupid',
    'schema.registry.url': 'http://127.0.0.1:8081'})

c.subscribe(['my_topic'])

while True:
    try:
        msg = c.poll(10)

    except SerializerError as e:
        print("Message deserialization failed for {}: {}".format(msg, e))
        break

    if msg is None:
        continue

    if msg.error():
        print("AvroConsumer error: {}".format(msg.error()))
        continue

    print(msg.value())

c.close()
```

**AdminClient**

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



Thread Safety
-------------

The `Producer`, `Consumer` and `AdminClient` are all thread safe.


Install
=======

**Install self-contained binary wheels**

    $ pip install confluent-kafka

**NOTE:** The pre-built Linux wheels do NOT contain SASL Kerberos/GSSAPI support.
          If you need SASL Kerberos/GSSAPI support you must install librdkafka and
          its dependencies using the repositories below and then build
          confluent-kafka using the instructions in the
          "Install from source" section below.

**Install AvroProducer and AvroConsumer**

    $ pip install "confluent-kafka[avro]"

**Install from source**

For source install, see the *Install from source* section in [INSTALL.md](INSTALL.md).


Broker Compatibility
====================
The Python client (as well as the underlying C library librdkafka) supports
all broker versions &gt;= 0.8.
But due to the nature of the Kafka protocol in broker versions 0.8 and 0.9 it
is not safe for a client to assume what protocol version is actually supported
by the broker, thus you will need to hint the Python client what protocol
version it may use. This is done through two configuration settings:

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


SSL certificates
================
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



License
=======

[Apache License v2.0](http://www.apache.org/licenses/LICENSE-2.0)

KAFKA is a registered trademark of The Apache Software Foundation and has been licensed for use
by confluent-kafka-python. confluent-kafka-python has no affiliation with and is not endorsed by
The Apache Software Foundation.

Developer Notes
===============

Instructions on building and testing confluent-kafka-python can be found [here](DEVELOPER.md).

Confluent Cloud
===============

For a step-by-step guide on using the Python client with Confluent Cloud see [Getting Started with Apache Kafka and Python](https://developer.confluent.io/get-started/python/) on [Confluent Developer](https://developer.confluent.io/). 
