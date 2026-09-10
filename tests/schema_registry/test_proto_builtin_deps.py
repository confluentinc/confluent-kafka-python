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

import pytest
from google.protobuf import descriptor_pb2
from google.protobuf.descriptor_pool import DescriptorPool

from confluent_kafka.schema_registry.common.protobuf import _init_pool


def _schema_importing(dep: str, message: str, name: str) -> descriptor_pb2.FileDescriptorProto:
    """A one-field schema importing ``dep``, the shape the registry returns for a Protobuf
    schema whose only reference is a built-in."""
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
    return fdp


# The canonical import path is confluent/type/... - what the Java client registers and what
# ProtobufSchema declares. The generated descriptors here were named confluent/types/... after
# the directory the Go client needs (`type` is a keyword there), which Python copied, so a
# Java-registered schema failed with "Depends on file 'confluent/type/decimal.proto', but it
# has not been loaded".
@pytest.mark.parametrize("dep, message", [
    ("confluent/type/decimal.proto", "Decimal"),
    ("confluent/type/variant.proto", "Variant"),
])
def test_builtin_confluent_type_imports_resolve(dep, message):
    pool = DescriptorPool()
    _init_pool(pool)
    fd = pool.Add(_schema_importing(dep, message, "test_ok_%s.proto" % message.lower()))
    assert fd.message_types_by_name["M"].fields[0].message_type.full_name \
        == "confluent.type." + message


# A pool holds one file per symbol, so the plural spelling cannot be aliased alongside the
# canonical one - registering both raises "duplicate symbol 'confluent.type.Decimal'". It has
# to fail, and name the import it could not find.
@pytest.mark.parametrize("dep, message", [
    ("confluent/types/decimal.proto", "Decimal"),
    ("confluent/types/variant.proto", "Variant"),
])
def test_the_plural_spelling_is_not_registered(dep, message):
    pool = DescriptorPool()
    _init_pool(pool)
    with pytest.raises(Exception, match=dep):
        pool.Add(_schema_importing(dep, message, "test_no_%s.proto" % message.lower()))
