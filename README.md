Confluent's Python Client for Apache Kafka<sup>TM</sup>
=======================================================

**confluent-kafka-python** is Confluent's Python client for [Apache Kafka](http://kafka.apache.org/) and the
[Confluent Platform](https://www.confluent.io/product/compare/).

Features:

- **High performance** - confluent-kafka-python is a lightweight wrapper around
[librdkafka](https://github.com/edenhill/librdkafka), a finely tuned C
client.

- **Reliability** - There are a lot of details to get right when writing an Apache Kafka
client. We get them right in one place (librdkafka) and leverage this work
across all of our clients (also [confluent-kafka-go](https://github.com/confluentinc/confluent-kafka-go)
and [confluent-kafka-dotnet](https://github.com/confluentinc/confluent-kafka-dotnet)).

- **Supported** - Commercial support is offered by
[Confluent](https://confluent.io/).

- **Future proof** - Confluent, founded by the
creators of Kafka, is building a [streaming platform](https://www.confluent.io/product/compare/)
with Apache Kafka at its core. It's high priority for us that client features keep
pace with core Apache Kafka and components of the [Confluent Platform](https://www.confluent.io/product/compare/).

The Python bindings provides a high-level Producer and Consumer with support
for the balanced consumer groups of Apache Kafka 0.9.

See the [API documentation](http://docs.confluent.io/current/clients/confluent-kafka-python/index.html) for more info.

**License**: [Apache License v2.0](http://www.apache.org/licenses/LICENSE-2.0)


Usage
=====

**Producer:**

```python
from confluent_kafka import Producer


p = Producer({'bootstrap.servers': 'mybroker,mybroker2'})

for data in some_data_source:
    p.produce('mytopic', data.encode('utf-8'))

p.flush()
```


**High-level Consumer:**

```python
from confluent_kafka import Consumer, KafkaError


c = Consumer({
    'bootstrap.servers': 'mybroker',
    'group.id': 'mygroup',
    'default.topic.config': {
        'auto.offset.reset': 'smallest'
    }
})

c.subscribe(['mytopic'])

while True:
    msg = c.poll()

    if msg.error():
        if msg.error().code() == KafkaError._PARTITION_EOF:
            continue
        else:
            print(msg.error())
            break

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

avroProducer = AvroProducer({
    'bootstrap.servers': 'mybroker,mybroker2',
    'schema.registry.url': 'http://schem_registry_host:port'
    }, default_key_schema=key_schema, default_value_schema=value_schema)

avroProducer.produce(topic='my_topic', value=value, key=key)
avroProducer.flush()
```

**AvroConsumer**

```python
from confluent_kafka import KafkaError
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
        if msg.error().code() == KafkaError._PARTITION_EOF:
            continue
        else:
            print(msg.error())
            break

    print(msg.value())

c.close()
```

See [examples](examples) for more examples.


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


Prerequisites
=============

 * Python >= 2.7 or Python 3.x
 * [librdkafka](https://github.com/edenhill/librdkafka) >= 0.9.5 (latest release is embedded in wheels)

librdkafka is embedded in the manylinux wheels, for other platforms or
when a specific version of librdkafka is desired, following these guidelines:

  * For **Debian/Ubuntu**** based systems, add this APT repo and then do `sudo apt-get install librdkafka-dev python-dev`:
http://docs.confluent.io/current/installation.html#installation-apt

 * For **RedHat** and **RPM**-based distros, add this YUM repo and then do `sudo yum install librdkafka-devel python-devel`:
http://docs.confluent.io/current/installation.html#rpm-packages-via-yum

 * On **OSX**, use **homebrew** and do `brew install librdkafka`


Install
=======

**Install from PyPi:**

    $ pip install confluent-kafka

    # for AvroProducer or AvroConsumer
    $ pip install confluent-kafka[avro]


**Install from source / tarball:**

    $ pip install .

    # for AvroProducer or AvroConsumer
    $ pip install .[avro]


Build
=====

    $ python setup.py build

If librdkafka is installed in a non-standard location provide the include and library directories with:

    $ C_INCLUDE_PATH=/path/to/include LIBRARY_PATH=/path/to/lib python setup.py ...


Tests
=====


**Run unit-tests:**

In order to run full test suite, simply execute:

    $ tox -r

**NOTE**: Requires `tox` (please install with `pip install tox`), several supported versions of Python on your path, and `librdkafka` [installed](tools/bootstrap-librdkafka.sh) into `tmp-build`.


**Run integration tests:**

To run the integration tests, uncomment the following line from `tox.ini` and add the paths to your Kafka and Confluent Schema Registry instances. You can also run the integration tests outside of `tox` by running this command from the source root.

    examples/integration_test.py <kafka-broker> [<test-topic>] [<schema-registry>]

**WARNING**: These tests require an active Kafka cluster and will create new topics.




Generate Documentation
======================
Install sphinx and sphinx_rtd_theme packages:

    $ pip install sphinx sphinx_rtd_theme

Build HTML docs:

    $ make docs

or:

    $ python setup.py build_sphinx

Documentation will be generated in `docs/_build/`.
