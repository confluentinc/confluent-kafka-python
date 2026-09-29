# Copyright 2024 Confluent Inc.
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

# Deprecated: import from confluent_kafka.schema_registry.confluent.type instead.
#
# This package held the generated confluent.type.Decimal bindings until they moved to the
# canonical confluent/type path - the one the Java client registers and ProtobufSchema declares.
# What remains is generated from confluent/types/decimal.proto, a stub that declares nothing and
# publicly imports the canonical file, so Decimal stays importable under its old name and a
# descriptor built against the old import path still resolves.
