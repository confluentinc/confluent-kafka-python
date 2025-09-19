# AIOProducer Visual Class Diagram

## Overview
This document presents a visual class diagram for the `confluent_kafka.aio.producer` module using ASCII art boxes and arrows for better clarity.

## 🎯 Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────────────────────────────────┐
│                                    🔵 CORE ORCHESTRATION LAYER                                        │
└─────────────────────────────────────────────────────────────────────────────────────────────────────┘

                                    ╔══════════════════════════════╗
                                    ║         AIOProducer          ║
                                    ║ ──────────────────────────── ║
                                    ║ - _loop: EventLoop           ║
                                    ║ - _producer: Producer        ║
                                    ║ - _callback_manager          ║
                                    ║ - _kafka_executor            ║
                                    ║ - _batch_processor           ║
                                    ║ - _buffer_timeout_manager    ║
                                    ║ ──────────────────────────── ║
                                    ║ + async produce()            ║
                                    ║ + async flush()              ║
                                    ║ + async close()              ║
                                    ║ + get_callback_stats()       ║
                                    ║ + create_batches_preview()   ║
                                    ║ - _handle_user_callback()    ║
                                    ╚══════════════════════════════╝
                                              │
                                              │ creates & orchestrates
                                              ▼
        ┌─────────────────────────────────────┼─────────────────────────────────────┐
        │                                     │                                     │
        ▼                                     ▼                                     ▼

┌─────────────────────────────────────────────────────────────────────────────────────────────────────┐
│                           🟣 MESSAGE PROCESSING | 🟢 KAFKA OPERATIONS | 🟠 TIMEOUT                   │
└─────────────────────────────────────────────────────────────────────────────────────────────────────┘

╔════════════════════════╗        ╔═══════════════════════╗        ╔═════════════════════════╗
║ ProducerBatchProcessor ║        ║   KafkaBatchExecutor  ║        ║ BufferTimeoutManager    ║
║ ────────────────────── ║        ║ ───────────────────── ║        ║ ─────────────────────── ║
║ - _callback_manager    ║◄──────►║ - _producer: Producer ║        ║ - _batch_processor      ║
║ - _kafka_executor      ║        ║ - _executor: ThreadEx ║        ║ - _kafka_executor       ║
║ - _message_buffer[]    ║        ║ ───────────────────── ║        ║ - _timeout: float       ║
║ - _buffer_futures[]    ║        ║ + async execute_batch ║        ║ - _timeout_task: Task   ║
║ ────────────────────── ║        ║   (topic, messages)   ║        ║ ─────────────────────── ║
║ + add_message()        ║        ║ - _handle_partial_    ║        ║ + mark_activity()       ║
║ + create_batches()     ║        ║   failures()          ║        ║ + start_monitoring()    ║
║ + async flush_buffer() ║        ║                       ║        ║ + stop_monitoring()     ║
║ + get_buffer_size()    ║        ║                       ║        ║ - async _monitor_timeout║
║ - _group_by_topic()    ║        ║                       ║        ║ - async _flush_timeout  ║
╚════════════════════════╝        ╚═══════════════════════╝        ╚═════════════════════════╝
         │                                   │                              │        │
         │ uses                              │ executes batches             │        │
         ▼                                   ▼                              │        │
                                                                            │        │
┌─────────────────────────────────────────────────────────────────────────────────────────────────────┐
│                                🔴 UNIFIED CALLBACK MANAGEMENT LAYER                                  │
└─────────────────────────────────────────────────────────────────────────────────────────────────────┘
                                                                            │        │
         ╔═══════════════════════════╗                                     │        │
         ║     CallbackManager       ║◄────────────────────────────────────┘        │
         ║ ───────────────────────── ║                                              │
         ║ - _loop: EventLoop        ║                                              │
         ║ - _available: List[]      ║                                              │
         ║ - _in_use: Set{}          ║                                              │
         ║ - _created_count: int     ║                                              │
         ║ - _reuse_count: int       ║                                              │
         ║ ───────────────────────── ║                                              │
         ║ + handle_user_callback()  ║                                              │
         ║ + get_callback()          ║                                              │
         ║ + return_callback()       ║                                              │
         ║ + get_stats()             ║                                              │
         ╚═══════════════════════════╝                                              │
                    │                                                               │
                    │ manages pool of                                               │
                    ▼                                                               │
         ╔═══════════════════════════╗                                              │
         ║ ReusableMessageCallback   ║                                              │
         ║ ───────────────────────── ║                                              │
         ║ + future: Future          ║                                              │
         ║ + user_callback: callable ║                                              │
         ║ + pool: CallbackManager   ║                                              │
         ║ ───────────────────────── ║                                              │
         ║ + reset()                 ║                                              │
         ║ + __call__(err, msg)      ║                                              │
         ║ - _clear_and_return()     ║                                              │
         ╚═══════════════════════════╝                                              │
                    │                                                               │
                    │ auto-returns to pool                                          │
                    └───────────────────┐                                          │
                                        │                                          │
                                        ▼                                          │
┌─────────────────────────────────────────────────────────────────────────────────────────────────────┐
│                                  🟡 DATA STRUCTURES LAYER                                            │
└─────────────────────────────────────────────────────────────────────────────────────────────────────┘
                                        │                                          │
         ╔═══════════════════════════╗  │  ╔════════════════════════════╗         │
         ║      MessageBatch         ║  │  ║   create_message_batch()   ║         │
         ║    (namedtuple)           ║  │  ║        (factory)           ║         │
         ║ ───────────────────────── ║  │  ║ ────────────────────────── ║         │
         ║ + topic: str              ║  │  ║ + create_message_batch()   ║         │
         ║ + messages: tuple         ║◄─┼──║   → MessageBatch           ║         │
         ║ + futures: tuple          ║  │  ║                            ║         │
         ║ + callbacks: tuple        ║  │  ║ Ensures:                   ║         │
         ║ + size: property          ║  │  ║ • Type safety              ║         │
         ║ + info: property          ║  │  ║ • Immutability             ║         │
         ╚═══════════════════════════╝  │  ║ • Validation               ║         │
                    ▲                   │  ╚════════════════════════════╝         │
                    │ creates           │                                         │
                    └───────────────────┘                                         │
                                                                                  │
┌─────────────────────────────────────────────────────────────────────────────────────────────────────┐
│                                  ⚪ EXTERNAL DEPENDENCIES                                            │
└─────────────────────────────────────────────────────────────────────────────────────────────────────┘
                                                                                  │
    ╔═══════════════════════╗    ╔═══════════════════════╗    ╔═════════════════════════╗      │
    ║ confluent_kafka       ║    ║   ThreadPoolExecutor  ║    ║   asyncio.EventLoop     ║      │
    ║   .Producer           ║    ║     (external)        ║    ║      (external)         ║      │
    ║   (external)          ║    ║ ───────────────────── ║    ║ ─────────────────────── ║      │
    ║ ───────────────────── ║    ║ + submit()            ║    ║ + call_soon_threadsafe()║      │
    ║ + produce_batch()     ║    ║ + shutdown()          ║    ║ + create_task()         ║      │
    ║ + poll()              ║    ║                       ║    ║ + run_in_executor()     ║      │
    ║ + flush()             ║    ║                       ║    ║                         ║      │
    ╚═══════════════════════╝    ╚═══════════════════════╝    ╚═════════════════════════╝      │
             ▲                            ▲                            ▲                        │
             │                            │                            │                        │
             │ wraps & uses               │ executes in                │ schedules on           │
             └────────────────────────────┼────────────────────────────┼────────────────────────┘
                                          │                            │
                                          └────────────────────────────┘
```

## 🔄 Data Flow Diagram

```
User Code                    AIOProducer                 Internal Components
    │                           │                              │
    │ await produce()            │                              │
    ├──────────────────────────► │                              │
    │                           │ add_message()                │
    │                           ├─────────────────────────────►│ ProducerBatchProcessor
    │                           │                              │        │
    │                           │                              │        │ buffer fills up
    │                           │                              │        ▼
    │                           │                              │ create_batches()
    │                           │                              │        │
    │                           │                              │        ▼ MessageBatch
    │                           │                              │ execute_batch()
    │                           │                              │        │
    │                           │                              │        ▼ KafkaBatchExecutor
    │                           │                              │ ThreadPoolExecutor.submit()
    │                           │                              │        │
    │                           │                              │        ▼
    │                           │                              │ confluent_kafka.Producer
    │                           │                              │   .produce_batch()
    │                           │                              │        │
    │                           │                              │        │ C thread callback
    │                           │                              │        ▼
    │                           │                              │ ReusableMessageCallback
    │                           │                              │   .__call__()
    │                           │                              │        │
    │                           │                              │        ▼
    │                           │                              │ CallbackManager
    │                           │                              │   .handle_user_callback()
    │                           │                              │        │
    │                           │                              │        ├─ sync callback
    │                           │                              │        └─ async callback
    │                           │                              │           (via event loop)
    │                           │                              │
    │ Future resolves           │                              │
    │◄──────────────────────────┤                              │
    │                           │                              │
```

## 📊 Component Relationships

### 🔗 Strong Composition (Creates & Owns)
```
AIOProducer
├── creates ──► CallbackManager
├── creates ──► KafkaBatchExecutor  
├── creates ──► ProducerBatchProcessor
└── creates ──► BufferTimeoutManager

CallbackManager
└── manages pool of ──► ReusableMessageCallback[]
```

### 🔌 Dependency Injection (Loose Coupling)
```
ProducerBatchProcessor
├── injected ──► CallbackManager
└── injected ──► KafkaBatchExecutor

BufferTimeoutManager
├── injected ──► ProducerBatchProcessor
└── injected ──► KafkaBatchExecutor
```

### 📦 Data Creation & Usage
```
ProducerBatchProcessor ──creates──► MessageBatch
ProducerBatchProcessor ──uses────► create_message_batch()
BufferTimeoutManager ──creates──► MessageBatch (via processor)
ReusableMessageCallback ──returns to──► CallbackManager
```

## 🏗️ Architecture Benefits

```
┌─────────────────────────────────────────────────────────────────┐
│                        CLEAN ARCHITECTURE                       │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ✅ Single Responsibility    Each component has ONE job         │
│  ✅ Loose Coupling          Dependencies injected, not created  │
│  ✅ High Cohesion           Related code grouped together       │
│  ✅ Immutable Data          MessageBatch prevents corruption    │
│  ✅ Performance Optimized   Object pooling reduces allocations  │
│  ✅ Thread Safe             Proper C-thread to Python handling  │
│  ✅ Testable                Clean interfaces enable mocking     │
│  ✅ Maintainable            Changes isolated to single layer    │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

## 📈 Performance Characteristics

### Memory Management
```
┌─────────────────────────────────────────────────┐
│              OBJECT POOLING                     │
├─────────────────────────────────────────────────┤
│                                                 │
│  CallbackManager Pool:                          │
│  ┌─────────────────────────────────────┐        │
│  │ Available: [○○○○○○○○○○]             │        │
│  │ In Use:    {●●●}                    │        │
│  │ Created:   1000 total               │        │
│  │ Reused:    2500 times               │        │
│  │ Ratio:     2.5x reuse efficiency    │        │
│  └─────────────────────────────────────┘        │
│                                                 │
│  Benefits:                                      │
│  • Reduced GC pressure                         │
│  • Lower allocation overhead                   │
│  • Consistent performance                      │
│                                                 │
└─────────────────────────────────────────────────┘
```

### Thread Safety Model
```
┌─────────────────────────────────────────────────┐
│              THREAD INTERACTION                 │
├─────────────────────────────────────────────────┤
│                                                 │
│  Event Loop Thread (Python):                   │
│  ├─ Produces messages                           │
│  ├─ Manages buffer                              │
│  └─ Schedules async callbacks                   │
│                                                 │
│  ThreadPool (Python):                          │
│  ├─ Executes produce_batch()                    │
│  └─ Handles blocking operations                 │
│                                                 │
│  librdkafka (C threads):                       │
│  ├─ Network I/O                                │
│  ├─ Delivery confirmations                     │
│  └─ Calls Python callbacks                     │
│                                                 │
│  Synchronization:                               │
│  • call_soon_threadsafe() for async callbacks  │
│  • Future objects for result communication     │
│  • No locks needed (GIL + single writer)       │
│                                                 │
└─────────────────────────────────────────────────┘
```

## 🎯 Component Statistics

| Layer | Components | Lines of Code* | Responsibilities |
|-------|------------|---------------|-----------------|
| **🔵 Core** | 1 | ~200 | API, orchestration, lifecycle |
| **🟣 Processing** | 1 | ~150 | Batching, buffering, organization |
| **🟢 Kafka Ops** | 1 | ~80 | Thread pool execution, produce_batch |
| **🟠 Timeout** | 1 | ~100 | Activity monitoring, auto-flush |
| **🔴 Callbacks** | 2 | ~120 | Execution + pooling (unified) |
| **🟡 Data** | 2 | ~30 | Value objects, factory functions |
| **📊 Total** | **8** | **~680** | **Clean, focused responsibilities** |

*Approximate lines excluding comments and docstrings

---

*This visual architecture demonstrates a production-ready, scalable async Kafka producer with clear separation of concerns, optimal performance characteristics, and maintainable design patterns.*
