Confluent's Apache Kafka client for Python
==========================================

Confluent's Kafka client for Python wraps the librdkafka C library, providing
full Kafka protocol support with great performance and reliability.

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

c = Consumer({'bootstrap.servers': 'mybroker', 'group.id': 'mygroup',
              'default.topic.config': {'auto.offset.reset': 'smallest'}})
c.subscribe(['mytopic'])
running = True
while running:
    msg = c.poll()
    if not msg.error():
        print('Received message: %s' % msg.value().decode('utf-8'))
    elif msg.error().code() != KafkaError._PARTITION_EOF:
        print(msg.error())
        running = False
c.close()
```

**AvroProducer**
```
from confluent_kafka import avro 
from confluent_kafka.avro import AvroProducer

value_schema = avro.load('ValueSchema.avsc')
key_schema = avro.load('KeySchema.avsc')
value = {"name": "Value"}
key = {"name": "Key"}

avroProducer = AvroProducer({'bootstrap.servers': 'mybroker,mybroker2', 'schema.registry.url': 'http://schem_registry_host:port'}, default_key_schema=key_schema, default_value_schema=value_schema)
avroProducer.produce(topic='my_topic', value=value, key=key)
```

**AvroConsumer**
```
import sys
import traceback
from confluent_kafka import KafkaError
from confluent_kafka.avro import AvroConsumer
from confluent_kafka.avro.serializer import SerializerError

c = AvroConsumer({'bootstrap.servers': 'mybroker,mybroker2', 'group.id': 'groupid', 'schema.registry.url': 'http://127.0.0.1:8081'})
c.subscribe(['my_topic'])
running = True
while running:
    try:
        msg = c.poll(10)
        if msg:
            if not msg.error():
                print(msg.value())
            elif msg.error().code() != KafkaError._PARTITION_EOF:
                print(msg.error())
                running = False
    except SerializerError as e:
        print("Message deserialization failed for %s: %s" % (msg, e))
        running = False
        
c.close()
```

See [examples](examples) for more examples.


Broker compatibility
====================
The Python client (as well as the underlying C library librdkafka) supports
all broker versions &gt;= 0.8.
But due to the nature of the Kafka protocol in broker versions 0.8 and 0.9 it
is not safe for a client to assume what protocol version is actually supported
by the broker, thus you will need to hint the Python client what protocol
version it may use. This is done through two configuration settings:

 * `broker.version.fallback=YOUR_BROKER_VERSION` (default 0.9.0.1)
 * `api.version.request=true|false` (default false)

When using a Kafka 0.10 broker or later you only need to set
`api.version.request=true`.
If you use Kafka broker 0.9 or 0.8 you should leave
`api.version.request=false` (default) and set
`broker.version.fallback` to your broker version,
e.g `broker.version.fallback=0.9.0.1`.

More info here:
https://github.com/edenhill/librdkafka/wiki/Broker-version-compatibility


Prerequisites
=============

 * Python >= 2.7 or Python 3.x
 * [librdkafka](https://github.com/edenhill/librdkafka) >= 0.9.1


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

To run the integration tests, uncomment the following line from `tox.ini` and add the paths to your Kafka and Confluent Schema Registry instances. If no Schema Registry path is provided then no AVRO tests will by run. You can also run the integration tests outside of `tox` by running this command from the source root.

    examples/integration_test.py <kafka-broker> [<test-topic>] [<schema-registry>]

**WARNING**: These tests require an active Kafka cluster and will create new topics.




Generate documentation
======================
Install sphinx and sphinx_rtd_theme packages and then:

    $ make docs

or:

    $ python setup.py build_sphinx

Documentation will be generated in `docs/_build/`.
