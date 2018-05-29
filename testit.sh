KAFKA_HOME=$HOME/CPKAFKA
KAFKA_VERSION=$1
RDKAFKA_VERSION=$2

# start cluster
$KAFKA_HOME/fresh.sh -r $KAFKA_VERSION 

# set throttle 
$KAFKA_HOME/confluent-$KAFKA_VERSION/bin/kafka-configs --zookeeper 127.0.0.1:2181 \
	--alter --add-config 'request_percentage=01' \
	--entity-name throttled_client \
	--entity-type clients

# run tests
tox -r 
