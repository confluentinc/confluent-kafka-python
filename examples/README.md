The scripts in this directory provide code examples using Confluent's Python client:

* [adminapi.py](adminapi.py): collection of Kafka Admin API operations
* [avro-cli.py](avro-cli.py): produces Avro messages with Confluent Schema Registry and then reads them back again 
* [consumer.py](consumer.py): reads messages from a Kafka topic
* [producer.py](producer.py): reads lines from stdin and sends them to Kafka
* [eos-transactions.py](eos-transactions.py): transactional producer with exactly once semantics (EOS)

Additional examples for [Confluent Cloud](https://www.confluent.io/confluent-cloud/):

* [confluent_cloud.py](confluent_cloud.py): produces messages to Confluent Cloud and then reads them back again
* [confluentinc/examples](https://github.com/confluentinc/examples/tree/master/clients/cloud/python): integrates Confluent Cloud and Confluent Cloud Schema Registry
