# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: dep.proto
"""Generated protocol buffer code."""
from google.protobuf.internal import builder as _builder
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import symbol_database as _symbol_database
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()


from . import test_pb2 as test__pb2


DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n\tdep.proto\x12\x04test\x1a\ntest.proto\"O\n\x11\x44\x65pendencyMessage\x12\x11\n\tis_active\x18\x01 \x01(\x08\x12\'\n\x0ctest_message\x18\x02 \x01(\x0b\x32\x11.test.TestMessageB\tZ\x07../testb\x06proto3')

_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, globals())
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'dep_pb2', globals())
if _descriptor._USE_C_DESCRIPTORS == False:

  DESCRIPTOR._options = None
  DESCRIPTOR._serialized_options = b'Z\007../test'
  _DEPENDENCYMESSAGE._serialized_start=31
  _DEPENDENCYMESSAGE._serialized_end=110
# @@protoc_insertion_point(module_scope)
