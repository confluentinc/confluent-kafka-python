WORK_DIR:=$(strip $(shell dirname $(realpath $(lastword $(MAKEFILE_LIST)))))
PROTO_HOME=/usr/local/opt/include
SRC_DIR=$(WORK_DIR)
TARGET_DIR=$(WORK_DIR)/../gen

PROTOS := common_proto.proto DependencyTestProto.proto exampleProtoCriteo.proto $\
         metadata_proto.proto NestedTestProto.proto PublicTestProto.proto $\
         SInt32Value.proto SInt64Value.proto TestProto.proto

compile: $(PROTOS)
	for proto in $(PROTOS); do \
		protoc -I=$(PROTO_HOME) -I=$(SRC_DIR) --python_out=$(TARGET_DIR) $$proto ; \
	done

clean:
	rm -f $(TARGET_DIR)/*_pb2.py
