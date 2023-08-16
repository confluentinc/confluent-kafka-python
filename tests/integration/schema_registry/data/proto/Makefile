WORK_DIR:=$(strip $(shell dirname $(realpath $(lastword $(MAKEFILE_LIST)))))
PROTO_HOME=/usr/local/opt/include
SRC_DIR=$(realpath $(WORK_DIR)/../../../../..)

PROTOS := common_proto.proto DependencyTestProto.proto exampleProtoCriteo.proto $\
         metadata_proto.proto NestedTestProto.proto PublicTestProto.proto $\
         SInt32Value.proto SInt64Value.proto TestProto.proto

compile: $(PROTOS)
	for proto in $(PROTOS); do \
		(cd $(SRC_DIR) && protoc -I=$(PROTO_HOME) -I=$(SRC_DIR) --python_out=$(SRC_DIR) tests/integration/schema_registry/data/proto/$$proto ;) \
	done

clean:
	rm -f *_pb2.py
