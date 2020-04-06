user_pb2.py: user.proto
	protoc -I=. --python_out=. ./user.proto;

clean:
	rm -f $(TARGET_DIR)/*_pb2.py
