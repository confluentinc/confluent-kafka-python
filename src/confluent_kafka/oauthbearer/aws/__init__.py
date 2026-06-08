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

"""AWS IAM OAUTHBEARER autowire subpackage.

The only publicly importable name in this subpackage is
:func:`confluent_kafka.oauthbearer.aws.aws_autowire.create_handler`, loaded by
core's C extension when the user sets
``sasl.oauthbearer.metadata.authentication.type=aws_iam``. All other modules
are private (underscore-prefixed) and not re-exported here.

Install with::

    pip install 'confluent-kafka[oauthbearer-aws]'
"""
