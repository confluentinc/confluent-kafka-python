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
Test package for confluent_kafka.aio.producer module

This package contains all tests for the async Kafka producer components:

Core Component Tests:
- test_AIOProducer.py: Main async producer API tests
- test_producer_batch_processor.py: Message batching and organization tests
- test_kafka_batch_executor.py: Kafka operations and thread pool tests

Callback Management Tests:
- test_callback_handler.py: Sync/async callback execution tests
- test_callback_pool.py: Performance optimization pooling tests

These tests verify the clean architecture and separation of concerns
implemented in the producer module.
"""
