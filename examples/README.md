The scripts in this directory provide code examples using Confluent's Python client:

* [adminapi.py](adminapi.py): Collection of Kafka Admin API operations
* [asyncio_example.py](asyncio_example.py): AsyncIO webserver with Kafka producer
* [consumer.py](consumer.py): Reads messages from a Kafka topic
* [producer.py](producer.py): Reads lines from stdin and sends them to Kafka
* [eos-transactions.py](eos-transactions.py): Transactional producer with exactly once semantics (EOS)
* [avro_producer.py](avro_producer.py): Simple example demonstrating use of AvroSerializer
* [avro_consumer.py](avro_consumer.py): Simple example demonstrating use of AvroDeserializer
* [json_producer.py](json_producer.py): Simple example demonstrating use of JSONSerializer
* [json_consumer.py](json_consumer.py): Simple example demonstrating use of JSONDeserializer
* [protobuf_producer.py](protobuf_producer.py): Simple example demonstrating use of ProtobufSerializer
* [protobuf_consumer.py](protobuf_consumer.py): Simple example demonstrating use of ProtobufDeserializer
* [sasl_producer.py](sasl_producer.py):  Example demonstrating SASL Authentication
* [list_offsets.py](list_offsets.py): List committed offsets and consumer lag for group and topics
* [oauth_producer.py](oauth_producer.py): Example demonstrating OAuth Authentication (client credentials)

Additional examples for [Confluent Cloud](https://www.confluent.io/confluent-cloud/):

* [confluent_cloud.py](confluent_cloud.py): produces messages to Confluent Cloud and then reads them back again
* [confluentinc/examples](https://github.com/confluentinc/examples/tree/master/clients/cloud/python): integrates Confluent Cloud and Confluent Cloud Schema Registry

## venv setup

It's usually a good idea to install Python dependencies in a virtual environment to avoid
conflicts between projects.

To setup a venv with the latest release version of confluent-kafka and dependencies of all examples installed:

```
$ python3 -m venv venv_examples
$ source venv_examples/bin/activate
$ cd examples
$ pip install -r requirements.txt
```

To setup a venv that uses the current source tree version of confluent_kafka, you
need to have a C compiler and librdkafka installed
([from a package](https://github.com/edenhill/librdkafka#installing-prebuilt-packages), or
[from source](https://github.com/edenhill/librdkafka#build-from-source)). Then:

```
$ python3 -m venv venv_examples
$ source venv_examples/bin/activate
$ python setup.py develop
$ cd examples
$ pip install -r requirements.txt
```

When you're finished with the venv:

```
$ deactivate
```