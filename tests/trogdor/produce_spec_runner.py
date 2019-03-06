# Copyright 2016 Confluent Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License")
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# One example of a produce Trogdor task, the workload is a JSON object following the ProduceBenchSpec.
#
# {
#     "id": "$TASK_ID",
#
#     "spec": {
#         "class": "org.apache.kafka.trogdor.workload.ExternalCommandSpec",
#         "command": ["python", "./tests/bin/ExternalCommandExample.py"],
#         "durationMs": 10000000,
#         "commandNode": "node0",
#         "workload":{
#             "class": "org.apache.kafka.trogdor.workload.ProduceBenchSpec",
#             "bootstrapServers": "localhost:9092",
#             "targetMessagesPerSec": 10000,
#             "maxMessages": 50000,
#             "activeTopics": {
#                 "foo[1-3]": {
#                     "numPartitions": 10,
#                     "replicationFactor": 1
#                 }
#             },
#             "inactiveTopics": {
#                 "foo[4-5]": {
#                     "numPartitions": 10,
#                     "replicationFactor": 1
#                 }
#             }
#         }
#     }
# }

# ProduceBenchSpec:
# "startMs": #start
# "bootstrapServers": #string#
# "targetMessagePerSec": #Java int#
# "maxMessages": #Java long#
# "keyGenerator": #Option, sequential generator by default# { "type": "constant" | "sequential" | "uniformRandom" | "null" }
# "valueGenerator"#Same as the keyGenerator#"
# "transactionGenerator":#Option, by default empty# {"type":"uniform"}
# "producerConf": #a JSON object#
# "commonClientConf": "a JSON object"
# "adminClientConf": "a JSON object"
# "activeTopics" : "a JSON object"
# "inactiveTopics": "a JSON object"
import threading
import time

from hdrh.histogram import HdrHistogram

from trogdor_utils import expand_topics, trogdor_log, update_trogdor_error, merge_topics, create_topics, \
    update_trogdor_status, create_admin_conn, partition_set, PayloadGenerator, SeqGenerator, ConstGenerator, \
    create_kafka_conf, create_producer_conn


def execute_produce_spec(spec):
    runner = ProduceSpecRunner(spec)
    runner.monitor()


class ProduceSpecRunner:
    def report_status(self):
        """ Report Histogram Latency"""
        exp_status = {"totalSent": self.latency_histogram.get_total_count(),
                      "totalError": self.nr_failed_messages,
                      "planQPS": self.qps,
                      "realQPS": self.real_qps,
                      "averageLatencyMs": self.latency_histogram.get_mean_value(),
                      "p50LatencyMs": self.latency_histogram.get_value_at_percentile(59),
                      "p95LatencyMs": self.latency_histogram.get_value_at_percentile(95),
                      "p99LatencyMs": self.latency_histogram.get_value_at_percentile(99),
                      "maxLatencyMs": self.latency_histogram.get_max_value()
                      }
        update_trogdor_status(exp_status)
    def message_on_delivery(self, err, msg, sent_time):
        self.nr_finished_messages += 1
        if err is not None:
            trogdor_log("ProduceSpecRunner: delivery failed: {} [{}]: {}".format(msg.topic(), msg.partition(), err))
            self.nr_failed_messages += 1
        now = time.time()
        latency = now - sent_time
#        print("latency:{}".format(latency * 1000))
        self.latency_histogram.record_value(latency * 1000)
        if now - self.last_report_time > self.report_status_interval:
            self.last_report_time = now
            self.report_status()
    def get_msg_callback(self):
        product_time = time.time()
        return lambda err,msg : self.message_on_delivery(err, msg, product_time)

    def execute_spec(self):
        self.start_produce_time = time.time()
        nr_topics = len(self.topic_partitions)
        nr_message = 0
        pause = 1.0/(self.qps);
        next_fire_time = self.start_produce_time + pause;
        while nr_message < self.max_messages:
            delta = next_fire_time - time.time()
            if delta > 0:
                time.sleep(delta)
            next_fire_time += pause
            topic_partition = self.topic_partitions[nr_message % nr_topics]
            topic = topic_partition[0]
            partition = topic_partition[1]
            while True:
                try:
                    self.producer.produce(topic, self.val_generator.nextVal(),
                                          self.key_generator.nextVal(), partition,
                                          on_delivery = self.get_msg_callback())
                    break
                except BufferError:
                    trogdor_log("Producer BufferError, retry")
                    self.producer.poll(1)
                    continue
            nr_message += 1

    def monitor(self):
        # report status every 10 seconds
        self.last_report_time = time.time()
        while self.status == "running" or self.nr_finished_messages != self.max_messages:
            self.producer.poll(10)
        self.end_produce_time = time.time()
        self.real_qps = self.nr_finished_messages / (self.end_produce_time - self.start_produce_time)
        trogdor_log("Finished all {} messages".format(self.max_messages))

        self.report_status()

    def producer_thread_main(self):
        """ Producer thread main function """
        try:
            self.execute_spec()
            self.status = "stopped"
        except KeyboardInterrupt:
            update_trogdor_status("The producer is interrupted.")
            self.status = "abort"
        except Exception as ex:
            update_trogdor_status("The producer has a fatal exception: " + str(ex))
            self.status = "exception"
    def create_spec_topics(self, producer_spec):
        active_spec_topics = producer_spec.get("activeTopics", {})
        if len(active_spec_topics.keys()) == 0:
            raise Exception("You must specify at least one active topic.")
        inactive_spec_topics = producer_spec.get("inactiveTopics", {})
        self.active_topics = expand_topics(active_spec_topics)
        self.inactive_topics = expand_topics(inactive_spec_topics)
        self.bootstrap_servers = producer_spec.get("bootstrapServers", "")
        if self.bootstrap_servers == "":
            raise Exception("You must specify the bootstrap servers")
        self.producer_conf = producer_spec.get("producerConf", {})
        self.common_client_conf = producer_spec.get("commonClientConf", {})
        self.admin_client_conf = producer_spec.get("adminClientConf", {})
        all_topics = merge_topics(self.active_topics, self.inactive_topics)
        update_trogdor_status("Creating {} topic(s)".format(len(all_topics.keys())))
        admin_conn = create_admin_conn(self.bootstrap_servers, self.common_client_conf, self.admin_client_conf)
        create_topics(admin_conn, all_topics)
        self.topic_partitions = partition_set(self.active_topics)

    def __init__(self, spec):
        self.spec = spec
        self.producer_spec = spec["workload"]
        self.status = "running"
        # between (0.000ms, 5000.000ms)
        self.latency_histogram = HdrHistogram(1, 5000, 3)
        self.report_status_interval = 10
        self.start_timestamp = time.time()
        self.create_spec_topics(self.producer_spec)
        self.key_generator = PayloadGenerator(SeqGenerator(4, 0))
        self.val_generator = PayloadGenerator(ConstGenerator(4,0xabcd))
        self.qps = self.producer_spec.get("targetMessagePerSec", 10000)
        self.max_messages = self.producer_spec.get("maxMessages", 100000)
        self.nr_finished_messages = 0
        self.nr_failed_messages = 0
        self.producer = create_producer_conn(self.bootstrap_servers, self.common_client_conf, self.producer_conf)
        trogdor_log("Topics:{}".format(str(self.producer.list_topics())))
        trogdor_log("Produce {} at message-per-sec {}".format(self.max_messages, self.qps))
        self.producer_thread = threading.Thread(target=self.producer_thread_main)
        self.producer_thread.start()




