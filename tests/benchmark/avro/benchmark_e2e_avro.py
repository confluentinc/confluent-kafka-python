import cProfile
import os
from uuid import uuid4

from confluent_kafka.cimpl import NewTopic
from trivup.clusters.KafkaCluster import KafkaCluster

from confluent_kafka import avro, Producer, Consumer
from confluent_kafka.admin import AdminClient
from confluent_kafka.schema_registry import CachedSchemaRegistryClient
from confluent_kafka.serialization import AvroSerializer

homedir = os.path.dirname(__file__)

basic_schema = """
{
    "name": "basic",
    "type": "record",
    "doc": "basic schema for tests",
    "namespace": "python.test.basic",
    "fields": [
        {
            "name": "number",
            "doc": "age",
            "type": [
                "long",
                "null"
            ]
        },
        {
            "name": "name",
            "doc": "a name",
            "type": [
                "string"
            ]
        }
    ]
}
"""


def create_topic(topic_name, conf):
    """
    Creates a topic.

    :param str topic_name: topic name
    :param dict conf: additions/overrides to topic configuration.
    :returns: The topic's name
    :rtype: str
    """
    admin = AdminClient(conf)

    future_topic = admin.create_topics(
        [NewTopic(topic_name, num_partitions=3, replication_factor=1)])

    future_topic.get(topic_name).result()

    return topic_name


def benchmark_single_subject_produce(conf, topic):
    """

    :param conf:
    :param topic:
    :return:
    """
    basic = avro.loads(basic_schema)
    profile = cProfile.Profile()
    producer = avro.AvroProducer(conf, default_value_schema=basic, default_key_schema=basic)
    profile.enable()
    for x in range(0, 1000000):
        try:
            producer.produce(topic=topic, value={'name': 'abc'}, key={'name': 'abc'})
            producer.poll(0)
        except BufferError:
            print("applying back pressure")
            producer.poll(1.0)
        continue
    profile.disable()
    producer.flush()
    profile.dump_stats("avro_producer.prof")


def benchmark_generic_serde_produce(conf, topic):
    """

    :param conf:
    :param topic:
    :return:
    """
    basic = avro.loads(basic_schema)
    sr_conf = {key.replace("schema.registry.", ""): value
               for key, value in conf.items() if key.startswith("schema.registry")}
    app_conf = {key: value
                for key, value in conf.items() if not key.startswith("schema.registry")}

    serializer = AvroSerializer(CachedSchemaRegistryClient(sr_conf), schema=basic)

    producer = Producer(app_conf, key_serializer=serializer, value_serializer=serializer)

    profile = cProfile.Profile()
    profile.enable()
    for x in range(0, 1000000):
        try:
            producer.produce(topic=topic, value={'name': 'abc'}, key={'name': 'abc'})
            producer.poll(0)
        except BufferError:
            print("applying back pressure")
            producer.poll(1.0)
        continue
    profile.disable()
    producer.flush()
    profile.dump_stats("serde_producer.prof")


def benchmark_consume(conf, topic):
    """

    :param conf:
    :param topic:
    :return:
    """
    consumer = avro.AvroConsumer(conf)
    profile = cProfile.Profile()
    msg_cnt = 0
    consumer.subscribe([topic], on_assign=lambda *args: profile.enable())
    while msg_cnt < 1000000:
        msg = consumer.poll(1.0)
        if msg is None or msg.error() is not None:
            print("err or none")
            continue
        msg_cnt += 1
    profile.disable()
    consumer.close()
    profile.dump_stats("avro_consumer.prof")


def benchmark_serde_batch_consume(conf, topic):
    """

    :param conf:
    :param topic:
    :return:
    """

    basic = avro.loads(basic_schema)
    sr_conf = {key.replace("schema.registry.", ""): value
               for key, value in conf.items() if key.startswith("schema.registry")}
    app_conf = {key: value
                for key, value in conf.items() if not key.startswith("schema.registry")}

    serializer = AvroSerializer(CachedSchemaRegistryClient(sr_conf), schema=basic)

    consumer = Consumer(app_conf, key_serializer=serializer, value_serializer=serializer)
    profile = cProfile.Profile()
    msg_cnt = 0
    consumer.subscribe([topic], on_assign=lambda *args: profile.enable())
    while msg_cnt < 1000000:
        for msg in consumer.consume(1000000, 2.0):
            if msg is None or msg.error() is not None:
                print("err or none")
                continue
            msg_cnt += 1
    profile.disable()
    consumer.close()
    profile.dump_stats("serde_consumer_batch.prof")


if __name__ == "__main__":
    topic = "benchmark_{}".format(str(uuid4()))

    cluster = KafkaCluster(**{'with_sr': True})
    cluster.wait_operational()

    conf = cluster.client_conf()
    create_topic(topic, conf)

    conf.update({'linger.ms': 100,
                 'acks': 1,
                 'schema.registry.url': cluster.sr.get('url')})

    # benchmark_single_subject_produce(conf, topic)
    benchmark_generic_serde_produce(conf, topic)
    conf.update({'group.id': str(uuid4()),
                 'auto.offset.reset': 'earliest',
                 'enable.partition.eof': True})
    # benchmark_consume(conf, topic)
    benchmark_serde_batch_consume(conf, topic)

    cluster.stop(keeptypes=[])
