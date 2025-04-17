import asyncio

from confluent_kafka import TopicPartition

async def test_async_produce(kafka_cluster):
    """
    Tests basic Avro serializer to_dict and from_dict object hook functionality.

    Args:
        kafka_cluster (KafkaClusterFixture): cluster fixture

    """
    topic = kafka_cluster.create_topic_and_wait_propogation("async-produce")

    producer = kafka_cluster.async_producer()

    await asyncio.gather(*[
        producer.produce(topic, value=bytes(i))
        for i in range(1_000)
    ])

    consumer = kafka_cluster.async_consumer()
    consumer.assign([TopicPartition(topic, 0)])

    msgs = []
    async for msg in consumer:
        msgs.append(msg)

        if len(msgs) >= 1_000:
            break
