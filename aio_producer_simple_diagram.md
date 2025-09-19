# AIOProducer Simple Class Diagram

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   AIOProducer   │    │BufferTimeout    │    │ MessageBatch    │
│  (Orchestrator) │    │   Manager       │    │ (Value Object)  │
├─────────────────┤    ├─────────────────┤    ├─────────────────┤
│ • produce()     │───▶│ • timeout       │    │ • immutable     │
│ • flush()       │    │   monitoring    │    │ • type safe     │
│ • orchestrate   │    │ • mark_activity │    │ • clean data    │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         │                       │                       ▲
         ▼                       ▼                       │
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│ProducerBatch    │    │ProducerBatch    │    │CallbackManager  │
│   Manager       │───▶│   Executor      │    │ (Unified Mgmt)  │
├─────────────────┤    ├─────────────────┤    ├─────────────────┤
│ • create_batches│    │ • execute_batch │    │ • sync/async    │
│ • group topics  │    │ • thread pool   │    │ • object pool   │
│ • manage buffer │    │ • poll results  │    │ • event loop    │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         │                                             ▲
         ▼                                             │
┌─────────────────┐                          ┌─────────────────┐
│create_message   │─────────────────────────▶│ Reusable        │
│   _batch()      │                          │ Callbacks       │
├─────────────────┤                          ├─────────────────┤
│ • factory func  │                          │ • pooled objs   │
│ • type safety   │                          │ • auto-return   │
│ • validation    │                          │ • thread safe   │
└─────────────────┘                          └─────────────────┘
```

## Architecture Summary

**7 Components Total:**
- **1 Orchestrator**: AIOProducer (main API)
- **3 Core Services**: BatchProcessor, KafkaExecutor, TimeoutManager  
- **1 Unified Manager**: CallbackManager (merged handler + pool)
- **2 Data Objects**: MessageBatch + factory function

**Key Benefits:**
- ✅ Single responsibility per component
- ✅ Clean dependency injection  
- ✅ Unified callback management
- ✅ Immutable data structures
- ✅ Performance optimized pooling
