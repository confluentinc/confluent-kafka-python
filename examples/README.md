# Confluent Kafka Python Examples

The scripts in this directory provide various examples of using Confluent's Python client for Kafka:

## Basic Producer/Consumer Examples

* [producer.py](producer.py): Read lines from stdin and send them to a Kafka topic.
* [consumer.py](consumer.py): Read messages from a Kafka topic.

## AsyncIO Examples

* [asyncio_example.py](asyncio_example.py): Comprehensive AsyncIO example demonstrating both AIOProducer and AIOConsumer with transactional operations, batched async produce, proper event loop integration, signal handling, and async callback patterns.

### Web Framework Integration

The AsyncIO producer works seamlessly with popular Python web frameworks:

**FastAPI/Starlette:**

```python
from fastapi import FastAPI
from confluent_kafka.aio import AIOProducer

app = FastAPI()
producer = None

@app.on_event("startup")
async def startup():
    global producer
    producer = AIOProducer({"bootstrap.servers": "localhost:9092"})

@app.post("/events")
async def create_event(data: dict):
    delivery_future = await producer.produce("events", value=str(data))
    message = await delivery_future
    return {"offset": message.offset()}
```

**aiohttp:**

```python
from aiohttp import web
from confluent_kafka.aio import AIOProducer

async def init_app():
    app = web.Application()
    app['producer'] = AIOProducer({"bootstrap.servers": "localhost:9092"})
    return app

async def handle_event(request):
    producer = request.app['producer']
    delivery_future = await producer.produce("events", value="data")
    message = await delivery_future
    return web.json_response({"offset": message.offset()})
```

For more details, see [Integrating Apache Kafka With Python Asyncio Web Applications](https://www.confluent.io/blog/kafka-python-asyncio-integration/).

## Transactional Examples

* [eos_transactions.py](eos_transactions.py): Transactional producer with exactly once semantics (EOS).

## Schema Registry & Serialization Examples

### Avro Serialization

* [avro_producer.py](avro_producer.py): Produce Avro serialized data using AvroSerializer.
* [avro_consumer.py](avro_consumer.py): Read Avro serialized data using AvroDeserializer.
* [avro_producer_encryption.py](avro_producer_encryption.py): Produce Avro data with client-side field level encryption (CSFLE).
* [avro_consumer_encryption.py](avro_consumer_encryption.py): Consume Avro data with client-side field level encryption (CSFLE).

### JSON Serialization

* [json_producer.py](json_producer.py): Produce JSON serialized data using JSONSerializer.
* [json_consumer.py](json_consumer.py): Read JSON serialized data using JSONDeserializer.
* [json_producer_encryption.py](json_producer_encryption.py): Produce JSON data with client-side field level encryption (CSFLE).
* [json_consumer_encryption.py](json_consumer_encryption.py): Consume JSON data with client-side field level encryption (CSFLE).

### Protobuf Serialization

* [protobuf_producer.py](protobuf_producer.py): Produce Protobuf serialized data using ProtobufSerializer.
* [protobuf_consumer.py](protobuf_consumer.py): Read Protobuf serialized data using ProtobufDeserializer.
* [protobuf_producer_encryption.py](protobuf_producer_encryption.py): Produce Protobuf data with client-side field level encryption (CSFLE).
* [protobuf_consumer_encryption.py](protobuf_consumer_encryption.py): Consume Protobuf data with client-side field level encryption (CSFLE).

## Authentication Examples

* [sasl_producer.py](sasl_producer.py): Demonstrates SASL Authentication.
* [oauth_producer.py](oauth_producer.py): Demonstrates OAuth Authentication (client credentials).
* [oauth_oidc_ccloud_producer.py](oauth_oidc_ccloud_producer.py): Demonstrates OAuth OIDC authentication with Confluent Cloud.
* [oauth_schema_registry.py](oauth_schema_registry.py): Demonstrates OAuth authentication with Schema Registry.

## Admin API Examples

* [adminapi.py](adminapi.py): Various AdminClient operations (topics, configs, ACLs).
* [adminapi_logger.py](adminapi_logger.py): AdminClient operations with custom logger configuration.
* [get_watermark_offsets.py](get_watermark_offsets.py): Consumer method for listing committed offsets and consumer lag for group and topics.

## Confluent Cloud Examples

* [confluent_cloud.py](confluent_cloud.py): Produce messages to Confluent Cloud and then read them back again.
* [confluentinc/examples](https://github.com/confluentinc/examples/tree/master/clients/cloud/python): Integration with Confluent Cloud and Confluent Cloud Schema Registry.

**Confluent Cloud Resources:**

* [Getting Started with Python](https://developer.confluent.io/get-started/python/) - Step-by-step tutorial.
* [Confluent Cloud Console](https://confluent.cloud/) - Sign up and manage clusters.
* [Python Client Configuration for Confluent Cloud](https://docs.confluent.io/cloud/current/client-apps/config-client.html#python-client)
* [Confluent Developer](https://developer.confluent.io/) - Tutorials, guides, and code examples.

## Running the Examples

Most examples require a running Kafka cluster. You can use:

* Local Kafka installation
* Docker Compose (see `docker/` subdirectory)
* [Confluent Cloud](https://confluent.cloud/) - see [Getting Started with Python](https://developer.confluent.io/get-started/python/) guide.

### Basic Usage Pattern

```bash
# Most examples follow this pattern:
python3 <example_name>.py <bootstrap_servers> [additional_args]

# For example:
python3 producer.py localhost:9092
python3 consumer.py localhost:9092
python3 asyncio_example.py localhost:9092 my-topic
```

### Examples with Schema Registry

For Avro, JSON, and Protobuf examples, you'll also need a Schema Registry:

```bash
python3 avro_producer.py localhost:9092 http://localhost:8081
```

**Schema Registry Resources:**

* [Schema Registry Overview](https://docs.confluent.io/platform/current/schema-registry/index.html)
* [Using Schema Registry with Python](https://docs.confluent.io/kafka-clients/python/current/overview.html#schema-registry)
* [Confluent Cloud Schema Registry](https://docs.confluent.io/cloud/current/sr/index.html)

Check each example's source code for specific command-line arguments and configuration requirements.

## venv setup

It's usually a good idea to install Python dependencies in a virtual environment to avoid
conflicts between projects.

To setup a venv with the latest release version of confluent-kafka and dependencies of all examples installed:

```bash
python3 -m venv venv_examples
source venv_examples/bin/activate
python3 -m pip install confluent_kafka
python3 -m pip  install -r requirements/requirements-examples.txt
```

To setup a venv that uses the current source tree version of confluent_kafka, you
need to have a C compiler and librdkafka installed
([from a package](https://github.com/edenhill/librdkafka#installing-prebuilt-packages), or
[from source](https://github.com/edenhill/librdkafka#build-from-source)). Then:

```bash
python3 -m venv venv_examples
source venv_examples/bin/activate
python3 -m pip install .[examples]
```

When you're finished with the venv:

```bash
deactivate
```
