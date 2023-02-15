user_pb2.py: protobuf/user.proto
	cd protobuf && protoc -I=. --python_out=. ./user.proto;

clean:
	rm -f $(TARGET_DIR)/protobuf/*_pb2.py
