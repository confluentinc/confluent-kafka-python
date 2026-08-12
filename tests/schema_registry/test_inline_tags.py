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

"""
Tests for resolving Avro record fullnames when collecting inline ``confluent:tags``.

The tags are keyed by record fullname, and the key has to match the name the Avro
library reports; when it does not, the tags are silently dropped, and tags are what
drive client-side field encryption.
"""

from confluent_kafka.schema_registry.common.avro import get_inline_tags


def test_namespace_is_prepended_even_when_it_is_also_a_prefix():
    # The fullname of 'foobar' in namespace 'foo' is 'foo.foobar'. Treating the namespace
    # as an already-present prefix would key the tags under 'foobar'.
    tags = get_inline_tags(
        {
            'type': 'record',
            'namespace': 'foo',
            'name': 'foobar',
            'fields': [{'name': 'x', 'type': 'string', 'confluent:tags': ['PII']}],
        }
    )
    assert 'foo.foobar.x' in tags
    assert tags['foo.foobar.x'] == {'PII'}


def test_namespace_attribute_is_ignored_for_a_fullname():
    # Avro ignores 'namespace' when 'name' already contains a dot.
    tags = get_inline_tags(
        {
            'type': 'record',
            'namespace': 'x',
            'name': 'a.B',
            'fields': [{'name': 'y', 'type': 'string', 'confluent:tags': ['PII']}],
        }
    )
    assert 'a.B.y' in tags
    assert 'x.a.B.y' not in tags


def test_plain_namespace_and_name():
    tags = get_inline_tags(
        {
            'type': 'record',
            'namespace': 'ns1',
            'name': 'rec',
            'fields': [{'name': 'z', 'type': 'string', 'confluent:tags': ['PII']}],
        }
    )
    assert 'ns1.rec.z' in tags


def test_nested_record_inherits_the_enclosing_namespace():
    tags = get_inline_tags(
        {
            'type': 'record',
            'namespace': 'ns1',
            'name': 'outer',
            'fields': [
                {
                    'name': 'inner',
                    'type': {
                        'type': 'record',
                        'name': 'Inner',
                        'fields': [{'name': 'w', 'type': 'string', 'confluent:tags': ['PII']}],
                    },
                }
            ],
        }
    )
    assert 'ns1.Inner.w' in tags
