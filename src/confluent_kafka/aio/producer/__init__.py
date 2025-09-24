# Copyright 2025 Confluent Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Confluent Kafka AIOProducer Module

This module contains all the components for the async Kafka producer:

Core Components:
- AIOProducer: Main async producer with clean architecture
- ProducerBatchProcessor: Message batching and organization  
- KafkaBatchExecutor: Kafka operations and thread pool management
- BufferTimeoutManager: Timeout monitoring and automatic flushing

Data Structures:
- MessageBatch: Immutable value object for batch data

Architecture Benefits:
✅ Single Responsibility: Each component has one clear purpose
✅ Clean Interfaces: Well-defined boundaries between components  
✅ Immutable Data: MessageBatch objects prevent accidental mutations
✅ Better Testing: Components can be tested independently
✅ Maintainable: Clear separation makes changes safer
"""

from ._AIOProducer import AIOProducer

# Export the main public API
__all__ = ['AIOProducer']
