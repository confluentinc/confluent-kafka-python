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


def test_wrapped_union_resolves_by_fullname():
    """
    A wrapped union value carries the branch's fullname, while the subschema often carries
    only its simple name with the namespace inherited from the enclosing record. Comparing
    the two directly never matches, and the branch — with its rules and tags — is skipped.
    """
    from confluent_kafka.schema_registry.common.avro import _resolve_union

    union = [
        {'type': 'record', 'name': 'A', 'fields': [{'name': 'v', 'type': 'string'}]},
        {'type': 'record', 'name': 'B', 'fields': [{'name': 'v', 'type': 'string'}]},
    ]

    # fullname in the value, simple name in the subschema
    subschema, payload = _resolve_union(union, ('test.B', {'v': 'x'}))
    assert subschema is not None and subschema['name'] == 'B'
    assert payload == {'v': 'x'}

    # the typed-dict form resolves the same way, and keeps the marker on the payload
    subschema, payload = _resolve_union(union, {'-type': 'test.A', 'v': 'x'})
    assert subschema is not None and subschema['name'] == 'A'
    assert payload == {'-type': 'test.A', 'v': 'x'}

    # a simple name in the value still matches
    subschema, _ = _resolve_union(union, ('B', {'v': 'x'}))
    assert subschema is not None and subschema['name'] == 'B'

    # an unknown branch resolves to nothing
    subschema, _ = _resolve_union(union, ('test.C', {'v': 'x'}))
    assert subschema is None


def test_wrapped_union_prefers_an_exact_namespace_match():
    """
    When two branches share a simple name in different namespaces, the fullname decides.
    """
    from confluent_kafka.schema_registry.common.avro import _resolve_union

    union = [
        {'type': 'record', 'name': 'Rec', 'namespace': 'a', 'fields': []},
        {'type': 'record', 'name': 'Rec', 'namespace': 'b', 'fields': []},
    ]
    subschema, _ = _resolve_union(union, ('b.Rec', {}))
    assert subschema is not None and subschema['namespace'] == 'b'


def test_wrapped_union_does_not_match_a_declared_namespace_by_simple_name():
    """
    The simple-name fallback exists for a subschema whose namespace is inherited from the
    enclosing record, and so is not visible on the subschema. A subschema that declares its
    own namespace has a fullname, so only that fullname may match it.
    """
    from confluent_kafka.schema_registry.common.avro import _resolve_union

    declared = [
        {'type': 'record', 'name': 'Rec', 'namespace': 'a', 'fields': []},
        {'type': 'record', 'name': 'Rec', 'namespace': 'b', 'fields': []},
    ]
    # A namespace no branch declares names no branch, rather than the first same-named one.
    subschema, _ = _resolve_union(declared, ('c.Rec', {}))
    assert subschema is None

    # With one branch declaring a namespace and one inheriting it, a value naming the
    # inherited namespace resolves to the inheriting branch, not to the declared one.
    mixed = [
        {'type': 'record', 'name': 'Rec', 'namespace': 'a', 'fields': []},
        {'type': 'record', 'name': 'Rec', 'fields': [{'name': 'v', 'type': 'string'}]},
    ]
    subschema, payload = _resolve_union(mixed, ('b.Rec', {'v': 'x'}))
    assert subschema is not None and 'namespace' not in subschema
    assert payload == {'v': 'x'}


def test_wrapped_union_does_not_match_a_declared_namespace_by_bare_name():
    """
    A namespaced record is named ``namespace.name``; its bare name names it no more than it
    names a same-named record in another namespace. So a bare simple name matches neither of
    two namespaced branches, rather than selecting the first - the exact pass requires the
    fullname, just as the simple-name fallback does.
    """
    from confluent_kafka.schema_registry.common.avro import _resolve_union

    declared = [
        {'type': 'record', 'name': 'Rec', 'namespace': 'a', 'fields': []},
        {'type': 'record', 'name': 'Rec', 'namespace': 'b', 'fields': []},
    ]
    subschema, _ = _resolve_union(declared, ('Rec', {}))
    assert subschema is None

    # A record with no namespace is still matched by its bare name - that is its fullname.
    bare = [
        {'type': 'record', 'name': 'A', 'fields': []},
        {'type': 'record', 'name': 'B', 'fields': []},
    ]
    subschema, _ = _resolve_union(bare, ('B', {}))
    assert subschema is not None and subschema['name'] == 'B'
