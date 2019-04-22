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

# One example of a produce Trogdor task embedded in the ExternalCommandSpec
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
# "keyGenerator": #payloadGenerator, optional, sequential generator (default)#
#  { "type": "constant" | "sequential" | "uniformRandom" | "null" }
# "valueGenerator"#payloadGenerator, optional, constant generator (default)#"
#  {"type": "constant" | "sequential" | "uniformRandom" | "null" }
# "transactionGenerator":#Object, optional, empty (default)# {"type":"uniform"}
# "producerConf": #a JSON object#
# "commonClientConf": "a JSON object"
# "adminClientConf": "a JSON object"
# "activeTopics" : "a JSON object"
# "inactiveTopics": "a JSON object"
#
# PayloadGenerator:
# ConstantPayLoadGenerator:
# "type": #string, "constant"
# "size": #Java Int, "size of the value"
# "value": #Java byte array, optional, "constant value"
# SequentialPayloadGenerator:
# "type": "sequential"
# "size": #Java Int, "size of the value"
# "startOffset": #Java Long, "start value of the increasing value"
# uniformRandomPayloadGenerator:
# "type": "uniformRandom"
# "size": #Java Int", "size of the value"
# "seed": #Java Long", "seed of the value"
# "padding" #Java Long", #padding size of the value, size - padding is the size of random bytes"
import math
import threading
import time

from hdrh.histogram import HdrHistogram

from trogdor_utils import expand_topics, trogdor_log, merge_topics, create_topics, \
    update_trogdor_status, create_admin_client, partition_set, PayloadGenerator, create_producer_conn, \
    get_payload_generator


def execute_produce_spec(workload):
    """ Return after finishing the workload """
    update_trogdor_status("running")
    runner = ProduceSpecRunner(workload)
    runner.monitor()


class ProduceSpecRunner:
    def report_status(self, realMPS=None):
        """ Report Histogram Latency"""
        exp_status = {"totalSent": self.nr_finished_messages,
                      "totalRecorded": self.latency_histogram.get_total_count(),
                      "totalError": self.nr_failed_messages,
                      "planMPS": self.mps,
                      "averageLatencyMs": self.latency_histogram.get_mean_value()/100.0,
                      "p50LatencyMs": self.latency_histogram.get_value_at_percentile(50)/100.0,
                      "p95LatencyMs": self.latency_histogram.get_value_at_percentile(95)/100.0,
                      "p99LatencyMs": self.latency_histogram.get_value_at_percentile(99)/100.0,
                      "maxLatencyMs": self.latency_histogram.get_max_value()/100.0
                      }
        if realMPS is not None:
            exp_status["realMPS"] = realMPS
        update_trogdor_status(exp_status)

    def message_on_delivery(self, err, msg, sent_time):
        if err is not None:
            trogdor_log("ProduceSpecRunner: delivery failed: {} [{}]: {}".format(msg.topic(), msg.partition(), err))
            self.nr_failed_messages += 1
        now = time.time()
        latency = now - sent_time
        self.latency_histogram.record_value(math.ceil(latency * 100000))
        self.totalLatency += latency
        self.nr_finished_messages += 1

    def get_msg_callback(self):
        sent_time = time.time()
        return lambda err, msg: self.message_on_delivery(err, msg, sent_time)

    def create_spec_topics(self, producer_spec):
        active_spec_topics = producer_spec.get("activeTopics", {})
        if len(active_spec_topics.keys()) == 0:
            raise Exception("You must specify at least one active topic.")
        self.bootstrap_servers = producer_spec.get("bootstrapServers", "")
        if self.bootstrap_servers == "":
            raise Exception("You must specify the bootstrap servers")
        inactive_spec_topics = producer_spec.get("inactiveTopics", {})
        self.active_topics = expand_topics(active_spec_topics)
        self.inactive_topics = expand_topics(inactive_spec_topics)
        self.producer_conf = producer_spec.get("producerConf", {})
        self.common_client_conf = producer_spec.get("commonClientConf", {})
        self.admin_client_conf = producer_spec.get("adminClientConf", {})
        all_topics = merge_topics(self.active_topics, self.inactive_topics)
        update_trogdor_status("Creating {} topic(s)".format(len(all_topics.keys())))
        admin_client = create_admin_client(self.bootstrap_servers, self.common_client_conf, self.admin_client_conf)
        create_topics(admin_client, all_topics)
        self.topic_partitions = partition_set(self.active_topics)

    def monitor(self):
        """ Executed in the context of the main (polling) thread """
        start_monitoring = time.time()
        last_report_time = time.time()
        while self.status == "running" or self.nr_finished_messages != self.max_messages:
            now = time.time()
            if now - last_report_time > self.report_status_interval:
                last_report_time = now
                self.report_status()
            self.producer.poll(self.report_status_interval)
        end_monitoring = time.time()
        real_mps = int(self.nr_finished_messages / (end_monitoring - start_monitoring))
        trogdor_log("Finished all {} messages".format(self.max_messages))
        self.report_status(realMPS=real_mps)

    def producer_thread_main(self):
        """ Producer thread """
        try:
            self.execute_spec()
            self.status = "stopped"
        except KeyboardInterrupt:
            update_trogdor_status("The producer is interrupted.")
            self.status = "abort"
        except Exception as ex:
            update_trogdor_status("The producer has a fatal exception: " + str(ex))
            self.status = "exception"

    def execute_spec(self):
        """ Executed in the context of the producer thread"""
        start_produce_time = time.time()
        nr_topics = len(self.topic_partitions)
        nr_message = 0
        pause = 1.0 / self.mps
        next_fire_time = start_produce_time + pause
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
                                          on_delivery=self.get_msg_callback())
                    break
                except BufferError:
                    trogdor_log("Producer BufferError, retry")
                    self.producer.poll(1)
                    continue
            nr_message += 1

    def __init__(self, workload):
        self.produce_workload = workload
        self.status = "running"
        self.totalLatency = 0
        # Record latencies between (0.000ms, 50000.00ms)
        self.latency_histogram = HdrHistogram(1, 5000000, 3)
        self.report_status_interval = 10
        self.start_timestamp = time.time()
        self.create_spec_topics(self.produce_workload)
        self.key_generator_spec = self.produce_workload.get("keyGenerator", {"type": "sequential",
                                                                             "size": 4,
                                                                             "startOffset": 0})
        self.value_generator_spec = self.produce_workload.get("valueGenerator", {"type": "constant", "size": 512})
        key_payload_generator = get_payload_generator(self.key_generator_spec)
        value_payload_generator = get_payload_generator(self.value_generator_spec)
        self.key_generator = PayloadGenerator(key_payload_generator)
        self.val_generator = PayloadGenerator(value_payload_generator)
        self.mps = self.produce_workload.get("targetMessagesPerSec", 10000)
        self.max_messages = self.produce_workload.get("maxMessages", 100000)
        self.nr_finished_messages = 0
        self.nr_failed_messages = 0
        self.producer = create_producer_conn(self.bootstrap_servers, self.common_client_conf, self.producer_conf)
        trogdor_log("Produce {} at message-per-sec {}".format(self.max_messages, self.mps))
        self.producer_thread = threading.Thread(target=self.producer_thread_main)
        self.producer_thread.start()
