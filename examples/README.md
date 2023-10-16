The scripts in this directory provide various examples of using Confluent's Python client for Kafka:

* [adminapi.py](adminapi.py): Various AdminClient operations.
* [asyncio_example.py](asyncio_example.py): AsyncIO webserver with Kafka producer.
* [consumer.py](consumer.py): Read messages from a Kafka topic.
* [producer.py](producer.py): Read lines from stdin and send them to a Kafka topic.
* [eos-transactions.py](eos-transactions.py): Transactional producer with exactly once semantics (EOS).
* [avro_producer.py](avro_producer.py): Produce Avro serialized data using AvroSerializer.
* [avro_consumer.py](avro_consumer.py): Read Avro serialized data using AvroDeserializer.
* [json_producer.py](json_producer.py): Produce JSON serialized data using JSONSerializer.
* [json_consumer.py](json_consumer.py): Read JSON serialized data using JSONDeserializer.
* [protobuf_producer.py](protobuf_producer.py): Produce Protobuf serialized data using ProtobufSerializer.
* [protobuf_consumer.py](protobuf_consumer.py): Read Protobuf serialized data using ProtobufDeserializer.
* [sasl_producer.py](sasl_producer.py):  Demonstrates SASL Authentication.
* [get_watermark_offsets.py](get_watermark_offsets.py): Consumer method for listing committed offsets and consumer lag for group and topics.
* [oauth_producer.py](oauth_producer.py): Demonstrates OAuth Authentication (client credentials).

Additional examples for [Confluent Cloud](https://www.confluent.io/confluent-cloud/):

* [confluent_cloud.py](confluent_cloud.py): Produce messages to Confluent Cloud and then read them back again.
* [confluentinc/examples](https://github.com/confluentinc/examples/tree/master/clients/cloud/python): Integration with Confluent Cloud and Confluent Cloud Schema Registry

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
