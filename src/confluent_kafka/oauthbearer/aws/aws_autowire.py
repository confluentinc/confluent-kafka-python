# Copyright 2026 Confluent Inc.
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

"""Frozen public entry-point for AWS IAM OAUTHBEARER autowire.

Placeholder for Phase 1; ``create_handler`` lands in Phase 4.

Mirrors .NET's ``AwsAutoWire.cs``. The C dispatcher in
``src/confluent_kafka/src/confluent_kafka.c`` will reach this module via
``PyImport_ImportModule("confluent_kafka.oauthbearer.aws.aws_autowire")``.
The function signature of ``create_handler`` is part of the cross-module
ABI and bumping it is a major-version change on ``confluent-kafka``.
"""
