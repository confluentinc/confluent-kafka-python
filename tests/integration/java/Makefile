KAFKA_JARS?=$(KAFKA_PATH)/libs

WORK_DIR:=$(strip $(shell dirname $(realpath $(lastword $(MAKEFILE_LIST)))))
CLASS_HOME=$(WORK_DIR)/target/classes
SRC_HOME=$(WORK_DIR)/src/main/java

CLASSES=JavaTestRunner.class

all: $(CLASSES)

JavaTestRunner.class: $(SRC_HOME)/JavaTestRunner.java
	javac -cp $(KAFKA_JARS)/kafka-clients-*.jar -d $(CLASS_HOME) $(SRC_HOME)/*.java

clean:
	rm -f $(CLASS_HOME)/*.class
