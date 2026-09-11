#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
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

import base64

import pytest
from google.protobuf import descriptor_pb2
from google.protobuf.descriptor_pool import DescriptorPool

from confluent_kafka.schema_registry.common.protobuf import _init_pool, _str_to_proto


def _schema_str(dep: str, message: str, name: str) -> str:
    """A one-field schema importing ``dep``, base64-encoded the way the registry stores it."""
    fdp = descriptor_pb2.FileDescriptorProto()
    fdp.name = name
    fdp.package = "test"
    fdp.syntax = "proto3"
    fdp.dependency.append(dep)
    msg = fdp.message_type.add()
    msg.name = "M"
    field = msg.field.add()
    field.name, field.number, field.type, field.label = "f", 1, 11, 1
    field.type_name = ".confluent.type." + message
    return base64.standard_b64encode(fdp.SerializeToString()).decode("ascii")


def _load(dep: str, message: str, name: str):
    """The production path: registry text -> _str_to_proto -> pool.Add."""
    pool = DescriptorPool()
    _init_pool(pool)
    return pool.Add(_str_to_proto(name, _schema_str(dep, message, name)))


# The canonical import path is confluent/type/... - what the Java client registers and what
# ProtobufSchema declares - while the generated descriptors here were named confluent/types/...
# after the directory the Go client needs (`type` is a keyword there), which this client copied.
# A Java-registered schema used to fail with "Depends on file 'confluent/type/decimal.proto',
# but it has not been loaded". The descriptors are canonical now, and decimal's old path is a
# public-import stub registered alongside them: it declares nothing, so it re-exports
# confluent.type.Decimal without the second declaration a pool refuses ("duplicate symbol").
@pytest.mark.parametrize(
    "dep, message",
    [
        ("confluent/type/decimal.proto", "Decimal"),
        ("confluent/type/variant.proto", "Variant"),
        ("confluent/types/decimal.proto", "Decimal"),
    ],
)
def test_builtin_confluent_type_imports_resolve(dep, message):
    fd = _load(dep, message, "test_%s.proto" % dep.replace("/", "_"))
    assert fd.message_types_by_name["M"].fields[0].message_type.full_name == "confluent.type." + message


# Only decimal's old path is stubbed; anything else still has to fail, naming the import it
# could not find. Variant is in this list on purpose: it had not shipped under the old path, so
# nothing can be importing it, and pinning that makes adding a stub a deliberate act.
@pytest.mark.parametrize(
    "dep",
    [
        "confluent/type/nope.proto",
        "confluent/types/nope.proto",
        "confluent/types/variant.proto",
    ],
)
def test_an_unknown_builtin_still_fails(dep):
    with pytest.raises(Exception, match=dep):
        _load(dep, "Decimal", "test_%s.proto" % dep.replace("/", "_"))


# The stub itself: registered under the old name, declaring nothing, and re-exporting the symbol
# through a public import. Each of the three is what keeps it from conflicting with the canonical
# file while still resolving - a declaration here would raise "duplicate symbol".
def test_the_legacy_stub_declares_nothing_and_reexports():
    pool = DescriptorPool()
    _init_pool(pool)

    stub = pool.FindFileByName("confluent/types/decimal.proto")
    assert stub.message_types_by_name == {}
    assert [d.name for d in stub.public_dependencies] == ["confluent/type/decimal.proto"]
    assert pool.FindMessageTypeByName("confluent.type.Decimal").file.name == "confluent/type/decimal.proto"


# The module path that shipped before the move. v2.15.0rc2's confluent/types/decimal_pb2.py had
# exactly two public names - DESCRIPTOR and Decimal - and both have to keep resolving there.
def test_the_old_module_path_still_exports_everything_it_shipped():
    from confluent_kafka.schema_registry.confluent.types import decimal_pb2 as legacy

    assert sorted(n for n in vars(legacy) if not n.startswith("_") and not n.startswith("confluent_dot")) == [
        "DESCRIPTOR",
        "Decimal",
    ]
    assert legacy.Decimal(value=b"\x04\xd2", scale=2).DESCRIPTOR.full_name == "confluent.type.Decimal"


# What DESCRIPTOR describes did change, unavoidably: it is the stub's own file now, so it
# declares no message where the shipped one declared Decimal. Declaring it in both places is
# exactly what a pool refuses ("duplicate symbol 'confluent.type.Decimal'"), so this is the
# price of keeping the old path resolvable at all - pinned here as intended, not as drift.
# `Decimal.DESCRIPTOR` and the canonical file are where the message is found.
def test_the_old_descriptor_describes_the_stub_not_the_message():
    from confluent_kafka.schema_registry.confluent.types import decimal_pb2 as legacy

    assert legacy.DESCRIPTOR.name == "confluent/types/decimal.proto"
    assert dict(legacy.DESCRIPTOR.message_types_by_name) == {}
    assert [d.name for d in legacy.DESCRIPTOR.public_dependencies] == ["confluent/type/decimal.proto"]
