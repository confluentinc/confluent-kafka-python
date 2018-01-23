#!/bin/sh

# Optional ENV variables:
# * ADVERTISED_HOST: the external ip for the container, e.g. `docker-machine ip \`docker-machine active\``
# * ADVERTISED_PORT: the external port for Kafka, e.g. 9092
# * ZK_CHROOT: the zookeeper chroot that's used by Kafka (without / prefix), e.g. "kafka"
# * LOG_RETENTION_HOURS: the minimum age of a log file in hours to be eligible for deletion (default is 168, for 1 week)
# * LOG_RETENTION_BYTES: configure the size at which segments are pruned from the log, (default is 1073741824, for 1GB)
# * NUM_PARTITIONS: configure the default number of log partitions per topic

# hack to deal with appending shit to the config file
echo "\n" >>$KAFKA_HOME/config/server.properties

if [ ! -z "$BROKER_ID" ]; then
    echo "broker.id=$BROKER_ID" >> $KAFKA_HOME/config/server.properties
fi

# Set the external host and port
if [ ! -z "$ADVERTISED_LISTENERS" ]; then
    echo "advertised listener: $ADVERTISED_LISTENERS"
    echo "advertised.listeners=$ADVERTISED_LISTENERS" >> $KAFKA_HOME/config/server.properties
fi

if [ ! -z "$LISTENERS" ]; then
    echo "advertised listener: $LISTENERS"
    echo "listeners=$LISTENERS" >> $KAFKA_HOME/config/server.properties
fi

if [ ! -z "$ZOOKEEPER_URL" ]; then
    sed -r -i "s/(zookeeper.connect)=(.*)/\1=$ZOOKEEPER_URL/g" $KAFKA_HOME/config/server.properties
fi

# Allow specification of log retention policies
if [ ! -z "$LOG_RETENTION_HOURS" ]; then
    echo "log retention hours: $LOG_RETENTION_HOURS"
    sed -r -i "s/(log.retention.hours)=(.*)/\1=$LOG_RETENTION_HOURS/g" $KAFKA_HOME/config/server.properties
fi
if [ ! -z "$LOG_RETENTION_BYTES" ]; then
    echo "log retention bytes: $LOG_RETENTION_BYTES"
    sed -r -i "s/#(log.retention.bytes)=(.*)/\1=$LOG_RETENTION_BYTES/g" $KAFKA_HOME/config/server.properties
fi

# Configure the default number of log partitions per topic
if [ ! -z "$NUM_PARTITIONS" ]; then
    echo "default number of partition: $NUM_PARTITIONS"
    sed -r -i "s/(num.partitions)=(.*)/\1=$NUM_PARTITIONS/g" $KAFKA_HOME/config/server.properties
fi

if [ ! -z "$NUM_REPLICATION" ]; then
    echo "default replication: $NUM_REPLICATION"
    echo "default.replication.factor=$NUM_REPLICATION" >> $KAFKA_HOME/config/server.properties
fi

# Enable/disable auto creation of topics
if [ ! -z "$AUTO_CREATE_TOPICS" ]; then
    echo "auto.create.topics.enable: $AUTO_CREATE_TOPICS"
    echo "auto.create.topics.enable=$AUTO_CREATE_TOPICS" >> $KAFKA_HOME/config/server.properties
fi

# Run Kafka
exec $KAFKA_HOME/bin/kafka-server-start.sh $KAFKA_HOME/config/server.properties
