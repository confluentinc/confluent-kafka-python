  ---
  Scenario 1 — Implicit mode + commit_sync

  Use case
  1. Worker subscribes to a topic in implicit mode (the default).
  2. Worker polls a batch of records.
  3. Worker processes them — by polling, it implicitly accepts them.
  4. Worker calls commit_sync(); broker converts the polled batch to ACCEPT.
  5. commit_sync() returns a dict of TopicPartition → None on success (or KafkaError per partition).

  from confluent_kafka import ShareConsumer

  sc = ShareConsumer({
      'bootstrap.servers': 'localhost:9092',
      'group.id': 'orders-workers',
      'share.acknowledgement.mode': 'implicit',     # default
  })
  sc.subscribe(['orders'])

  for msg in sc.poll(timeout=10.0):
      process_order(msg.value())

  result = sc.commit_sync(timeout=10.0)
  # result -> {TopicPartition('orders', 0): None}

  ---
  Scenario 2 — Explicit mode + ACCEPT + commit_sync

  Use case
  1. Worker subscribes with share.acknowledgement.mode='explicit'.
  2. Worker polls a batch.
  3. For each record, after processing, worker calls acknowledge(msg, ACCEPT).
  4. Worker calls commit_sync(); broker accepts the explicitly-ack'd records.
  5. App controls every record's outcome — auditable, record-by-record.

  from confluent_kafka import ShareConsumer, AcknowledgeType

  sc = ShareConsumer({
      'bootstrap.servers': 'localhost:9092',
      'group.id': 'orders-workers',
      'share.acknowledgement.mode': 'explicit',
  })
  sc.subscribe(['orders'])

  for msg in sc.poll(timeout=10.0):
      process_order(msg.value())
      sc.acknowledge(msg, AcknowledgeType.ACCEPT)

  result = sc.commit_sync(timeout=10.0)
  # result -> {TopicPartition('orders', 0): None}

  ---
  Scenario 3 — RELEASE → redelivery (queue effect)

  Use case
  1. Worker polls a record in explicit mode.
  2. Processing hits a transient failure (downstream rate-limit, timeout).
  3. Worker calls acknowledge(msg, RELEASE) — "try again later."
  4. Worker calls commit_sync(); broker returns the record to Available.
  5. Worker polls again — same (topic, partition, offset) is redelivered. Broker tracks delivery count internally.

  from confluent_kafka import ShareConsumer, AcknowledgeType

  sc = ShareConsumer({
      'bootstrap.servers': 'localhost:9092',
      'group.id': 'orders-workers',
      'share.acknowledgement.mode': 'explicit',
  })
  sc.subscribe(['orders'])

  for msg in sc.poll(timeout=10.0):
      try:
          process_order(msg.value())
          sc.acknowledge(msg, AcknowledgeType.ACCEPT)
      except DownstreamUnavailable:
          sc.acknowledge(msg, AcknowledgeType.RELEASE)   # redeliver later

  sc.commit_sync(timeout=10.0)

  ---
  Scenario 4 — REJECT → archived (poison-pill)

  Use case
  1. Worker polls a record in explicit mode.
  2. Record is malformed (schema violation, business-rule failure).
  3. Worker calls acknowledge(msg, REJECT) — "never deliver again."
  4. Worker calls commit_sync(); broker moves the record to Archived state.
  5. A fresh consumer in the same group polls — the record never reappears. No DLQ wiring required.

  from confluent_kafka import ShareConsumer, AcknowledgeType

  sc = ShareConsumer({
      'bootstrap.servers': 'localhost:9092',
      'group.id': 'orders-workers',
      'share.acknowledgement.mode': 'explicit',
  })
  sc.subscribe(['orders'])

  for msg in sc.poll(timeout=10.0):
      try:
          order = parse_order(msg.value())
          process_order(order)
          sc.acknowledge(msg, AcknowledgeType.ACCEPT)
      except (SchemaError, BusinessRuleError):
          sc.acknowledge(msg, AcknowledgeType.REJECT)    # archived forever

  sc.commit_sync(timeout=10.0)

  ---
  Scenario 5 — Two consumers cooperatively drain one topic

  Use case (the headline KIP-932 capability)
  1. Two workers create ShareConsumers with the same group.id.
  2. Both subscribe to the same topic — even with a single partition.
  3. Producer enqueues N records.
  4. Broker delivers each record to exactly one worker in the group.
  5. Each worker acks the records it received; total processed = N, duplicates = 0.

  from confluent_kafka import ShareConsumer, AcknowledgeType

  def worker(name):
      sc = ShareConsumer({
          'bootstrap.servers': 'localhost:9092',
          'group.id': 'orders-workers',           # SAME group across workers
          'share.acknowledgement.mode': 'explicit',
      })
      sc.subscribe(['orders'])

      while True:
          for msg in sc.poll(timeout=1.0):
              print(f'[{name}] processed offset {msg.offset()}')
              sc.acknowledge(msg, AcknowledgeType.ACCEPT)
          sc.commit_async()

  # Run worker('A') and worker('B') in parallel (threads or processes).
  # Records split between them — no manual partition assignment.

  ---
  Scenario 6 — commit_async (non-blocking commit)

  Use case
  1. Worker is in a tight poll loop and can't afford to block on commits.
  2. Worker processes a batch, acks each record.
  3. Worker calls commit_async() — returns immediately, broker results land in a callback.
  4. Worker keeps polling without waiting for broker round-trip.
  5. Used in high-throughput pipelines where latency matters more than per-commit confirmation.

  from confluent_kafka import ShareConsumer, AcknowledgeType

  sc = ShareConsumer({
      'bootstrap.servers': 'localhost:9092',
      'group.id': 'orders-workers',
      'share.acknowledgement.mode': 'explicit',
  })
  sc.subscribe(['orders'])

  while True:
      for msg in sc.poll(timeout=1.0):
          process_order(msg.value())
          sc.acknowledge(msg, AcknowledgeType.ACCEPT)
      sc.commit_async()   # returns None immediately; broker ack happens in background