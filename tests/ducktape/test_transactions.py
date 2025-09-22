"""
Ducktape tests for transactions in async producer and consumer.
"""
import uuid
import asyncio
from ducktape.tests.test import Test

from confluent_kafka.aio._AIOProducer import AIOProducer
from tests.ducktape.consumer_strategy import AsyncConsumerStrategy, SyncConsumerStrategy
from tests.ducktape.services.kafka import KafkaClient
from confluent_kafka import Producer


class TransactionsTest(Test):
    def __init__(self, test_context):
        super(TransactionsTest, self).__init__(test_context=test_context)
        self.kafka = KafkaClient(test_context, bootstrap_servers="localhost:9092")

    def setUp(self):
        if not self.kafka.verify_connection():
            raise Exception("Cannot connect to Kafka at localhost:9092. Please ensure Kafka is running.")

    def _new_topic(self, partitions: int = 1) -> str:
        topic = f"tx-{uuid.uuid4()}"
        self.kafka.create_topic(topic, partitions=partitions, replication_factor=1)
        assert self.kafka.wait_for_topic(topic, max_wait_time=30)
        return topic

    def _create_transactional_producer(self, producer_type):
        """Create transactional producer based on type"""
        # TODO: use producer_strategy to simplify code once it's merged
        if producer_type == "sync":
            config = {
                'bootstrap.servers': self.kafka.bootstrap_servers(),
                'batch.size': 65536,
                'linger.ms': 1,
                'compression.type': 'lz4',
                'transactional.id': f'sync-tx-producer-{uuid.uuid4()}',
                'acks': 'all',
                'enable.idempotence': True
            }
            return Producer(config)
        else:  # async
            config = {
                'bootstrap.servers': self.kafka.bootstrap_servers(),
                'batch.size': 65536,
                'linger.ms': 1,
                'compression.type': 'lz4',
                'transactional.id': f'async-tx-producer-{uuid.uuid4()}',
                'acks': 'all',
                'enable.idempotence': True,
            }
            return AIOProducer(config, max_workers=10)

    def _create_transactional_consumer(self, consumer_type, group_id=None):
        """Create read_committed consumer based on type using strategy pattern"""
        if group_id is None:
            group_id = f'tx-consumer-{uuid.uuid4()}'

        # Overrides for transactional consumers
        overrides = {
            'isolation.level': 'read_committed',
            'enable.auto.commit': False,
            'auto.offset.reset': 'earliest',
        }

        if consumer_type == "sync":
            strategy = SyncConsumerStrategy(self.kafka.bootstrap_servers(), group_id, self.logger)
            return strategy.create_consumer(config_overrides=overrides)
        else:  # async
            strategy = AsyncConsumerStrategy(self.kafka.bootstrap_servers(), group_id, self.logger)
            return strategy.create_consumer(config_overrides=overrides)

    async def _transactional_produce(self, producer, topic: str, values: list[str], partition: int | None = None):
        """Produce values within an active transaction, polling to drive delivery."""
        for v in values:
            kwargs = {'topic': topic, 'value': v}
            if partition is not None:
                kwargs['partition'] = partition
            await producer.produce(**kwargs)
            await producer.poll(0.0)

    async def _seed_topic(self, topic: str, values: list[str]):
        """Seed a topic using transactional producer."""
        producer = self._create_transactional_producer("sync")
        producer.init_transactions()
        producer.begin_transaction()
        for v in values:
            producer.produce(topic, value=v)
            producer.poll(0.0)
        producer.commit_transaction()
        producer.flush()

    # =========== Functional tests (async) ===========

    def test_commit_transaction(self):
        """Committed transactional messages must be visible to read_committed consumer."""
        async def run():
            topic = self._new_topic()
            producer = self._create_transactional_producer("async")
            consumer = self._create_transactional_consumer("async")
            try:
                await producer.init_transactions()
                await producer.begin_transaction()
                await self._transactional_produce(producer, topic, [f'c{i}' for i in range(5)])
                await producer.commit_transaction()

                await consumer.subscribe([topic])
                seen = []
                for _ in range(20):
                    msg = await consumer.poll(timeout=1.0)
                    if not msg or msg.error():
                        continue
                    seen.append(msg.value().decode('utf-8'))
                    if len(seen) >= 5:
                        break
                assert len(seen) == 5, f"expected 5 committed messages, got {len(seen)}"
            finally:
                await consumer.close()
        asyncio.run(run())

    def test_abort_transaction_then_retry_commit(self):
        """Aborted messages must be invisible, and retrying with new transaction must only commit visible results."""
        async def run():
            topic = self._new_topic()
            producer = self._create_transactional_producer("async")
            consumer = self._create_transactional_consumer("async")
            try:
                await producer.init_transactions()

                # Abort case
                await producer.begin_transaction()
                await self._transactional_produce(producer, topic, [f'a{i}' for i in range(3)])
                await producer.abort_transaction()

                await consumer.subscribe([topic])
                aborted_seen = []
                for _ in range(10):
                    msg = await consumer.poll(timeout=1.0)
                    if not msg or msg.error():
                        continue
                    val = msg.value().decode('utf-8')
                    if val.startswith('a'):
                        aborted_seen.append(val)
                assert not aborted_seen, f"aborted messages should be invisible, saw {aborted_seen}"

                # Retry-commit flow
                await producer.begin_transaction()
                retry_vals = [f'r{i}' for i in range(3)]
                await self._transactional_produce(producer, topic, retry_vals)
                await producer.commit_transaction()

                # Verify only retry values appear
                seen = []
                for _ in range(20):
                    msg = await consumer.poll(timeout=1.0)
                    if not msg or msg.error():
                        continue
                    val = msg.value().decode('utf-8')
                    seen.append(val)
                    if all(rv in seen for rv in retry_vals):
                        break
                assert all(rv in seen for rv in retry_vals), f"expected retry values {retry_vals}, saw {seen}"
                assert all(not s.startswith('a') for s in seen), f"should not see aborted values, saw {seen}"
            finally:
                await consumer.close()
        asyncio.run(run())

    def test_send_offsets_to_transaction(self):
        """Offsets committed atomically with produced results using send_offsets_to_transaction."""
        async def run():
            input_topic = self._new_topic()
            output_topic = self._new_topic()

            # Seed input
            input_vals = [f'in{i}' for i in range(5)]
            await self._seed_topic(input_topic, input_vals)

            producer = self._create_transactional_producer("async")
            consumer = self._create_transactional_consumer("async")
            try:
                await consumer.subscribe([input_topic])

                # Consume a small batch from input
                consumed = []
                for _ in range(20):
                    msg = await consumer.poll(timeout=1.0)
                    if not msg or msg.error():
                        continue
                    consumed.append(msg)
                    if len(consumed) >= 3:
                        break
                assert consumed, "expected to consume at least 1 message from input"

                # Begin transaction: produce results and commit consumer offsets atomically
                await producer.init_transactions()
                await producer.begin_transaction()

                out_vals = [f'out:{m.value().decode("utf-8")}' for m in consumed]
                await self._transactional_produce(producer, output_topic, out_vals)

                assignment = await consumer.assignment()
                positions = await consumer.position(assignment)
                group_metadata = await consumer.consumer_group_metadata()

                await producer.send_offsets_to_transaction(positions, group_metadata)
                await producer.commit_transaction()

                # Verify output has results
                out_consumer = self._create_transactional_consumer("async")
                try:
                    await out_consumer.subscribe([output_topic])
                    seen = []
                    for _ in range(20):
                        msg = await out_consumer.poll(timeout=1.0)
                        if not msg or msg.error():
                            continue
                        seen.append(msg.value().decode('utf-8'))
                        if len(seen) >= len(out_vals):
                            break
                    assert set(seen) == set(out_vals), f"expected {out_vals}, saw {seen}"
                finally:
                    await out_consumer.close()

                # Verify committed offsets advanced to positions
                committed = await consumer.committed(assignment)
                for pos, comm in zip(positions, committed):
                    assert comm.offset >= pos.offset, f"committed {comm.offset} < position {pos.offset}"
            finally:
                await consumer.close()
        asyncio.run(run())

    def test_commit_multiple_topics_partitions(self):
        """Commit atomically across multiple topics/partitions."""
        async def run():
            topic_a = self._new_topic(partitions=2)
            topic_b = self._new_topic(partitions=1)

            producer = self._create_transactional_producer("async")
            consumer = self._create_transactional_consumer("async")
            try:
                await producer.init_transactions()
                await producer.begin_transaction()
                # Produce across A partitions and B
                await self._transactional_produce(producer, topic_a, ["a0-p0", "a1-p0"], partition=0)
                await self._transactional_produce(producer, topic_a, ["a0-p1", "a1-p1"], partition=1)
                await self._transactional_produce(producer, topic_b, ["b0", "b1"])  # default partition
                await producer.commit_transaction()

                await consumer.subscribe([topic_a, topic_b])
                expected = {"a0-p0", "a1-p0", "a0-p1", "a1-p1", "b0", "b1"}
                seen = set()
                for _ in range(30):
                    msg = await consumer.poll(timeout=1.0)
                    if not msg or msg.error():
                        continue
                    seen.add(msg.value().decode('utf-8'))
                    if seen == expected:
                        break
                assert seen == expected, f"expected {expected}, saw {seen}"
            finally:
                await consumer.close()
        asyncio.run(run())
