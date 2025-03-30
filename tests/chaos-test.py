import subprocess
import time
from confluent_kafka import Producer, Consumer
from confluent_kafka.admin import AdminClient, NewTopic
import random
import signal
import sys
import threading

consumer_stopped = False

def wait_for_containers_to_be_healthy(no_of_containers):
    # Run 'docker-compose up -d' in detached mode
    subprocess.run(["docker", "compose", "up", "-d"], check=True)
    while True:
        # Get the total number of containers defined in the compose file
        result = subprocess.run(
            ["docker", "ps", "--format", "{{.Names}}"],
            capture_output=True,
            text=True
        )
        all_container_names = result.stdout.splitlines()
        total_containers = len(all_container_names)
        if no_of_containers == total_containers:
            print("All containers are healthy and ready!")
            break
        print(f"Waiting for containers to be healthy... ({total_containers}/4)")
        time.sleep(5)  # Wait before checking again

def get_kafka_container():
    result = subprocess.run(
        ["docker", "ps", "--format", "{{.Names}} {{.ID}}"],
        capture_output=True,
        text=True
    )
    all_containers = result.stdout.splitlines()
    d = {}
    [d.update({item.split(' ')[0]: item.split(' ')[1]}) for item in all_containers if item.split(' ')[0] != "zookeeper"]
    return d

def stop_kafka_container(container_name):
    print(f"Stopping Kafka container: {container_name}")
    subprocess.Popen(["docker", "stop", container_name])

def start_kafka_container(container_name):
    subprocess.Popen(["docker", "start", container_name])

def start_stopped_kafka_container(stopped_container):
    print(f"Start Container: Current stopped containers: {stopped_container}")
    if len(stopped_container) > 0:
        starting_container = random.choice(stopped_container)
        stopped_container.remove(starting_container)
        start_kafka_container(starting_container)
        print(f"Started Kafka container: {starting_container}")

def stop_container(all_kafka_containers, stopped_container):
    random_kafka_container = random.choice(list(all_kafka_containers.keys()))
    stopping_container = all_kafka_containers[random_kafka_container]
    if stopping_container not in stopped_container:
        stop_kafka_container(stopping_container)
        stopped_container.append(stopping_container)
        print(f"Stopped Kafka container: {stopping_container}")
    else:
        print(f"Kafka container {stopping_container} is already stopped")
    print(f"Stop Container: Current stopped containers: {stopped_container}")

def stop_all_kafka_containers(all_kafka_containers, stopped_container):
    print("Stopping all Kafka containers")
    for container in all_kafka_containers.values():
        stop_kafka_container(container)
        if container not in stopped_container:
            stopped_container.append(container)

def reset_or_create_consumer(brokers, consumer, topic_name):
    time.sleep(0.05)
    stop_consumer = random.choice([True])
    if stop_consumer and consumer:
        consumer_stopped = True
        print("Closing Kafka consumer")
        consumer.close()
        consumer = None
    if not consumer:
        # Create a Kafka consumer
        consumer_stopped = False
        consumer = Consumer({
            "bootstrap.servers": brokers,
            "group.id": topic_name,
            "auto.offset.reset": "earliest",
        })
        # Subscribe to the topic
        consumer.subscribe([topic_name])
    return consumer

def start_all_kafka_containers(all_kafka_containers, stopped_container):
    print("Starting all Kafka containers")
    for container in all_kafka_containers.values():
        start_kafka_container(container)
        if container in stopped_container:
            stopped_container.remove(container)

def start_stop_broker_consumer(all_kafka_containers, stopped_container):
    frequency = 5
    control = random.randint(1, 100*frequency)
    control -= 1
    if control == 0:
        stop_container(all_kafka_containers, stopped_container)
    elif control == 25*frequency:
        start_stopped_kafka_container(stopped_container)
    if control == 50*frequency:
        stop_all_kafka_containers(all_kafka_containers, stopped_container)
    elif control == 75*frequency:
        start_all_kafka_containers(all_kafka_containers, stopped_container)
    return control % (25*frequency) == 0

def create_topics(brokers, topics, partitions=3, rep=1):
    a = AdminClient({'bootstrap.servers': brokers})
    new_topics = [NewTopic(topic, num_partitions=partitions, replication_factor=rep) for topic in topics]
    fs = a.create_topics(new_topics)
    for topic, f in fs.items():
        try:
            f.result()  # The result itself is None
            print("Topic {} created".format(topic))
        except Exception as e:
            print("Failed to create topic {}: {}".format(topic, e))


def main():
    brokers = sys.argv[1]
    topic_name = f"chaos-test-topic-{random.randint(1, 10000)}"
    stopped_container = []
    print(f"Using topic: {topic_name}")

    # Docker initializations
    wait_for_containers_to_be_healthy(int(sys.argv[2]))
    all_kafka_containers = get_kafka_container()

    # Create a Kafka topic
    create_topics(brokers, [topic_name], 12, 1)

    # Create a Kafka producer
    producer = Producer({
        "bootstrap.servers": brokers,
        "linger.ms": 0
    })

    # Create a Kafka consumer
    consumer = reset_or_create_consumer(brokers, None, topic_name)

    # Handle SIGINT
    def signal_handler(sig, frame):
        print("You pressed Ctrl+C! Exiting gracefully...")

        time.sleep(5)
        print(f"Stopped containers: {stopped_container}")
        while len(stopped_container) > 0:
            start_stopped_kafka_container(stopped_container)

        time.sleep(10)

        if consumer:
            print("Closing Kafka consumer")
            consumer.close()

        print("Flushing Kafka producer")
        producer.flush()

        print("Deleting Kafka topic")
        a = AdminClient({'bootstrap.servers': brokers})
        delete_topics = a.delete_topics([topic_name])
        for topic, f in delete_topics.items():
            try:
                f.result()  # The result itself is None
                print("Topic {} deleted".format(topic))
            except Exception as e:
                print("Failed to delete topic {}: {}".format(topic, e))

        exit(0)

    signal.signal(signal.SIGINT, signal_handler)

    while True:
        reset_consumer = start_stop_broker_consumer(all_kafka_containers, stopped_container)
        if reset_consumer and not consumer_stopped:
            def reset_consumer_thread():
                nonlocal consumer
                consumer = reset_or_create_consumer(brokers, consumer, topic_name)
            threading.Thread(target=reset_consumer_thread).start()

        if not consumer_stopped:
            # Consume a message
            message = consumer.poll(timeout=0.001)
            if message is None:
                print("No message received")
            else:
                print(f"Received message: {message.value()}")


        message_to_produce=f"value-{random.randint(1, 1000)}"
        if random.choice([True, False, False, False, False]):
            # Produce a message
            producer.produce(topic_name, value=message_to_produce)
            print(f"Producing message: {message_to_produce}")

        time.sleep(0.2)

if __name__ == "__main__":
    main()
