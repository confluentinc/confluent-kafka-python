# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: tests/integration/schema_registry/data/proto/exampleProtoCriteo.proto
"""Generated protocol buffer code."""
from google.protobuf.internal import builder as _builder
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import symbol_database as _symbol_database
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()


from tests.integration.schema_registry.data.proto import metadata_proto_pb2 as tests_dot_integration_dot_schema__registry_dot_data_dot_proto_dot_metadata__proto__pb2
from tests.integration.schema_registry.data.proto import common_proto_pb2 as tests_dot_integration_dot_schema__registry_dot_data_dot_proto_dot_common__proto__pb2


DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\nEtests/integration/schema_registry/data/proto/exampleProtoCriteo.proto\x12\x0b\x43riteo.Glup\x1a\x41tests/integration/schema_registry/data/proto/metadata_proto.proto\x1a?tests/integration/schema_registry/data/proto/common_proto.proto\"\x9b\x06\n\x08\x43lickCas\x12(\n\x0bglup_origin\x18\x01 \x01(\x0b\x32\x13.Criteo.Glup.Origin\x12)\n\tpartition\x18\x02 \x01(\x0b\x32\x16.Criteo.Glup.Partition\x12\x0b\n\x03uid\x18\x05 \x01(\t\x12:\n\nset_fields\x18\xda\x86\x03 \x03(\x0b\x32$.Criteo.Glup.ClickCas.SetFieldsEntry\x12R\n\x0f\x63ontrol_message\x18\xff\xff\x7f \x03(\x0b\x32%.Criteo.Glup.ControlMessage.WatermarkB\x10\x92\xb5\x18\x0c\n\n__metadata\x1a\x30\n\x0eSetFieldsEntry\x12\x0b\n\x03key\x18\x01 \x01(\x05\x12\r\n\x05value\x18\x02 \x01(\x08:\x02\x38\x01:\xc9\x03\x88\xb5\x18\x01\x82\xb5\x18\x04:\x02\x10\x01\x82\xb5\x18\x12\n\x10\n\x0eglup_click_cas\x82\xb5\x18\xea\x01*\xe7\x01\n\tclick_cas\x12G\n,/glup/datasets/click_cas/data/full/JSON_PAIL\x10\x02@dJ\x13\x46\x45\x44\x45RATED_JSON_PAIL\x12U\n3/glup/datasets/click_cas/data/full/PROTOBUF_PARQUET\x10\x04@2J\x1a\x46\x45\x44\x45RATED_PROTOBUF_PARQUET\x18\x04\"&com.criteo.glup.ClickCasProto$ClickCas2\x0b\x65nginejoins@\x01H\x86\x03\x82\xb5\x18\xb3\x01\x12\xb0\x01\x1a\xad\x01\n\x0b\x65nginejoins\x12\tclick_cas \x04Z9\x12\x30\n\x0eglup_click_cas\"\tclick_cas*\x13\x46\x45\x44\x45RATED_JSON_PAIL\xd2\x0f\x04\x08\x02\x10\x06ZP2G\n\tclick_cas\x12\tclick_cas*\x13\x46\x45\x44\x45RATED_JSON_PAIL2\x1a\x46\x45\x44\x45RATED_PROTOBUF_PARQUET\xd2\x0f\x04\x08\x02\x10\x06\x62\x04R\x02\x18\x04J\x04\x08\x46\x10JJ\x04\x08K\x10LR\x08obsoleteR\tobsolete2B\x11\n\x0f\x63om.criteo.glupb\x06proto3')

_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, globals())
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'tests.integration.schema_registry.data.proto.exampleProtoCriteo_pb2', globals())
if _descriptor._USE_C_DESCRIPTORS == False:

  DESCRIPTOR._options = None
  DESCRIPTOR._serialized_options = b'\n\017com.criteo.glup'
  _CLICKCAS_SETFIELDSENTRY._options = None
  _CLICKCAS_SETFIELDSENTRY._serialized_options = b'8\001'
  _CLICKCAS.fields_by_name['control_message']._options = None
  _CLICKCAS.fields_by_name['control_message']._serialized_options = b'\222\265\030\014\n\n__metadata'
  _CLICKCAS._options = None
  _CLICKCAS._serialized_options = b'\210\265\030\001\202\265\030\004:\002\020\001\202\265\030\022\n\020\n\016glup_click_cas\202\265\030\352\001*\347\001\n\tclick_cas\022G\n,/glup/datasets/click_cas/data/full/JSON_PAIL\020\002@dJ\023FEDERATED_JSON_PAIL\022U\n3/glup/datasets/click_cas/data/full/PROTOBUF_PARQUET\020\004@2J\032FEDERATED_PROTOBUF_PARQUET\030\004\"&com.criteo.glup.ClickCasProto$ClickCas2\013enginejoins@\001H\206\003\202\265\030\263\001\022\260\001\032\255\001\n\013enginejoins\022\tclick_cas \004Z9\0220\n\016glup_click_cas\"\tclick_cas*\023FEDERATED_JSON_PAIL\322\017\004\010\002\020\006ZP2G\n\tclick_cas\022\tclick_cas*\023FEDERATED_JSON_PAIL2\032FEDERATED_PROTOBUF_PARQUET\322\017\004\010\002\020\006b\004R\002\030\004'
  _CLICKCAS._serialized_start=219
  _CLICKCAS._serialized_end=1014
  _CLICKCAS_SETFIELDSENTRY._serialized_start=473
  _CLICKCAS_SETFIELDSENTRY._serialized_end=521
# @@protoc_insertion_point(module_scope)
