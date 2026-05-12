One unified demo: orders task-queue with 3 terminals

  Covers scenarios 
  2 (explicit + ACCEPT), 
  3 (RELEASE → redelivery), 
  4 (REJECT → archive), 
  5 (multi-consumer split), and 6 (commit_async + commit_sync result dict) 
  in one flow.
  
   ~30 sec wall-clock, ~3-4 min with narration.

  ---
  Setup (one-time, before demo)

  The local broker has group.share.record.lock.duration.ms=1000 (1s) which causes duplicate delivery. The producer script bumps it to 30s for our group on startup. Nothing for you to do here — it happens in the producer's first 200ms.

  ---
  The 3 terminals

  T1: python demo_producer.py
  T2: python demo_worker.py A
  T3: python demo_worker.py B
     (optional) T4: python demo_worker.py C

  Start workers first so they finish warmup before producer streams.

  ---
  Flow of steps

  1. Workers boot (T2, T3, T4). Each prints [A] ready once its share session is stable. They sit idle, polling.
  2. Producer fires (T1). Streams 20 tasks at 400 ms intervals to topic orders:
    - 18 normal task-N
    - 1 flaky flaky-12 (will be RELEASEd first time, ACCEPTed on retry)
    - 1 poison poison-7 (REJECTed; never redelivered)
  3. Workers race for each new task. Each task goes to exactly one worker.
  4. When a worker sees poison-* → acknowledge(msg, REJECT). Broker archives it.
  5. When a worker sees flaky-* first time → acknowledge(msg, RELEASE). Broker returns it to Available; it pops up in some worker (maybe a different one) a moment later → ACCEPT.
  6. Every worker calls commit_async() after each poll to keep the queue moving.
  7. You press Ctrl+C on each worker → final commit_sync(timeout=5) prints the per-partition result dict and an accepted=X rejected=Y tally.
  8. Audience sees: 19 accepted + 1 rejected = 20 tasks, 0 duplicates, 1 redelivery for flaky-12. Final commit_sync dict on each worker shows {TopicPartition('orders', 0): None}.

  ---
  Expected on-screen output

  T1 (producer)
  [producer] -> task-0
  [producer] -> task-1
  ...
  [producer] -> poison-7
  [producer] -> task-8
  ...
  [producer] -> flaky-12
  ...
  [producer] -> task-19
  [producer] done. 20 enqueued.

  T2 (worker A)
  [A] ready
  [A] task-0        ACCEPT
  [A] task-2        ACCEPT
  [A] flaky-12      RELEASE (transient)
  [A] task-15       ACCEPT
  [A] task-17       ACCEPT
  ^C
  [A] final commit_sync...
  [A] result: {TopicPartition('orders', 0): None}
  [A] accepted=8 rejected=0

  T3 (worker B)
  [B] ready
  [B] task-1        ACCEPT
  [B] task-3        ACCEPT
  [B] poison-7      REJECT (poison)
  [B] flaky-12      ACCEPT (retry)             <-- redelivery from A's RELEASE
  [B] task-19       ACCEPT
  ^C
  [B] result: {TopicPartition('orders', 0): None}
  [B] accepted=7 rejected=1

  ---
  demo_producer.py

  import time
  from confluent_kafka import Producer
  from confluent_kafka.admin import (
      AdminClient, AlterConfigOpType, ConfigEntry, ConfigResource, NewTopic,
  )

  TOPIC = 'orders'
  GROUP = 'orders-workers'
  BOOTSTRAP = 'localhost:9092'

  admin = AdminClient({'bootstrap.servers': BOOTSTRAP})

  # 1. Create topic (idempotent)
  for f in admin.create_topics([NewTopic(TOPIC, 1, 1)]).values():
      try: f.result(10)
      except Exception: pass

  # 2. Bump the group's lock duration to 30s so workers actually split
  res = ConfigResource(ConfigResource.Type.GROUP, GROUP)
  res.add_incremental_config(ConfigEntry(
      'share.record.lock.duration.ms', '30000',
      incremental_operation=AlterConfigOpType.SET))
  for f in admin.incremental_alter_configs([res]).values():
      f.result(10)

  # 3. Stream 20 tasks, 400ms apart
  tasks = [f'task-{i}' for i in range(20)]
  tasks[7]  = 'poison-7'
  tasks[12] = 'flaky-12'

  p = Producer({'bootstrap.servers': BOOTSTRAP})
  for t in tasks:
      p.produce(TOPIC, value=t.encode())
      p.poll(0)
      print(f'[producer] -> {t}', flush=True)
      time.sleep(0.4)
  p.flush(10)
  print('[producer] done. 20 enqueued.')

  ---
  demo_worker.py

  import os, sys, time
  from confluent_kafka import ShareConsumer, AcknowledgeType

  NAME      = sys.argv[1] if len(sys.argv) > 1 else 'X'
  TOPIC     = 'orders'
  GROUP     = 'orders-workers'
  BOOTSTRAP = 'localhost:9092'
  ATTEMPTS  = '/tmp/share_demo_attempts'
  os.makedirs(ATTEMPTS, exist_ok=True)

  sc = ShareConsumer({
      'bootstrap.servers': BOOTSTRAP,
      'group.id': GROUP,
      'share.acknowledgement.mode': 'explicit',
  })
  sc.subscribe([TOPIC])

  # Warmup -- empty polls until share session is Stable
  deadline = time.monotonic() + 8
  while time.monotonic() < deadline:
      sc.poll(timeout=1.0)
  print(f'[{NAME}] ready', flush=True)

  accepted = rejected = 0
  try:
      while True:
          for m in sc.poll(timeout=1.0):
              if m.error():
                  continue
              v = m.value().decode()
              time.sleep(0.15)                           # simulated work

              if v.startswith('poison'):
                  sc.acknowledge(m, AcknowledgeType.REJECT)
                  rejected += 1
                  print(f'[{NAME}] {v:12s}  REJECT (poison)', flush=True)

              elif v.startswith('flaky'):
                  marker = f'{ATTEMPTS}/{v}'
                  if os.path.exists(marker):             # second attempt
                      sc.acknowledge(m, AcknowledgeType.ACCEPT)
                      accepted += 1
                      print(f'[{NAME}] {v:12s}  ACCEPT (retry)', flush=True)
                  else:                                   # first attempt
                      open(marker, 'w').close()
                      sc.acknowledge(m, AcknowledgeType.RELEASE)
                      print(f'[{NAME}] {v:12s}  RELEASE (transient)', flush=True)

              else:
                  sc.acknowledge(m, AcknowledgeType.ACCEPT)
                  accepted += 1
                  print(f'[{NAME}] {v:12s}  ACCEPT', flush=True)
          sc.commit_async()                              # keep the queue hot

  except KeyboardInterrupt:
      print(f'[{NAME}] final commit_sync...', flush=True)
      result = sc.commit_sync(timeout=5.0)
      print(f'[{NAME}] result: {dict(result)}', flush=True)
      print(f'[{NAME}] accepted={accepted} rejected={rejected}', flush=True)
  finally:
      sc.close()

  ---
  What each line you'll demo maps to in the API

  ┌────────────────────────────────────────────────────────────────────┬───────────────────────────────────────┐
  │                          On-screen action                          │           API surface shown           │
  ├────────────────────────────────────────────────────────────────────┼───────────────────────────────────────┤
  │ sc = ShareConsumer({... 'share.acknowledgement.mode': 'explicit'}) │ scenario 2 setup                      │
  ├────────────────────────────────────────────────────────────────────┼───────────────────────────────────────┤
  │ sc.acknowledge(m, AcknowledgeType.ACCEPT)                          │ scenarios 2, 5                        │
  ├────────────────────────────────────────────────────────────────────┼───────────────────────────────────────┤
  │ sc.acknowledge(m, AcknowledgeType.RELEASE) + redelivery            │ scenario 3                            │
  ├────────────────────────────────────────────────────────────────────┼───────────────────────────────────────┤
  │ sc.acknowledge(m, AcknowledgeType.REJECT) + no redelivery          │ scenario 4                            │
  ├────────────────────────────────────────────────────────────────────┼───────────────────────────────────────┤
  │ Two/three workers, same group.id, no overlap                       │ scenario 5                            │
  ├────────────────────────────────────────────────────────────────────┼───────────────────────────────────────┤
  │ sc.commit_async() in the hot loop                                  │ scenario 6 (async)                    │
  ├────────────────────────────────────────────────────────────────────┼───────────────────────────────────────┤
  │ sc.commit_sync(timeout=5.0) on shutdown, prints dict               │ scenario 6 (sync) + result-dict shape │
  └────────────────────────────────────────────────────────────────────┴───────────────────────────────────────┘

  Only scenario 1 (implicit mode) isn't exercised — mention it verbally: "If you want even less code, set share.acknowledgement.mode='implicit' and skip all the acknowledge() calls — broker auto-accepts every polled record."

  ---