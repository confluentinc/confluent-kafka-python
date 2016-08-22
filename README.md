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


**Install from source / tarball:**

    $ pip install .


Build
=====

    $ python setup.py build

If librdkafka is installed in a non-standard location provide the include and library directories with:

    $ C_INCLUDE_PATH=/path/to/include LIBRARY_PATH=/path/to/lib python setup.py ...


Tests
=====


**Run unit-tests:**

    $ py.test

**NOTE**: Requires `py.test`, install by `pip install pytest`


**Run integration tests:**

    $ examples/integration_test.py <kafka-broker>

**WARNING**: These tests require an active Kafka cluster and will make use of a topic named 'test'.




Generate documentation
======================
Install sphinx and sphinx_rtd_theme packages and then:

    $ make docs

or:

    $ python setup.py build_sphinx

Documentation will be generated in `docs/_build/`.
