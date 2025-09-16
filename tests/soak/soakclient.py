#!/usr/bin/env python
#
# Copyright 2018 Confluent Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
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
#

#
# Soak test producer-consumer end-to-end client for
# long term validation testing.
#
# Usage:
#  tests/soak/soakclient.py -i <testid> -t <topic> -r <produce-rate> -f <client-conf-file>
#
# A unique topic should be used for each soakclient instance.
#

from confluent_kafka import KafkaError, KafkaException, version
from confluent_kafka import Producer, Consumer
from confluent_kafka.admin import AdminClient, NewTopic
from collections import defaultdict
from builtins import int
from opentelemetry import metrics
import argparse
import threading
import time
import json
import logging
import sys
import traceback
import resource
import os
import psutil


class SoakRecord (object):
    """ A private record type, with JSON serializer and deserializer """

    def __init__(self, msgid, name=None):
        self.msgid = msgid
        if name is None:
            self.name = "SoakRecord nr #{}".format(self.msgid)
        else:
            self.name = name

    def serialize(self):
        return json.dumps(self, default=lambda o: o.__dict__)

    def __str__(self):
        return "SoakRecord({})".format(self.name)

    @classmethod
    def deserialize(cls, binstr):
        d = json.loads(binstr)
        return SoakRecord(d['msgid'], d['name'])


class SoakClient (object):
    """ The SoakClient consists of a Producer sending messages at
        the given rate, and a Consumer consuming the messages.
        Both clients print their message and error counters every 10 seconds.
        The producer and consumer run in separate background threads.
    """

    # metric name prefix
    METRIC_PFX = "kafka.client.soak.python."

    def dr_cb(self, err, msg):
        """ Producer delivery report callback """
        if err is not None:
            self.logger.warning("producer: delivery failed: {} [{}]: {}".
                                format(msg.topic(), msg.partition(), err))
            self.dr_err_cnt += 1
            self.incr_counter("producer.drerr", 1)
            self.incr_counter("producer.delivery.failure", 1, {
                "topic": msg.topic(),
                "partition": str(msg.partition()),
                "err": str(err)
            })

        else:
            self.dr_cnt += 1
            self.incr_counter("producer.drok", 1)
            self.set_gauge("producer.latency", msg.latency(),
                           tags={"partition": "{}".format(msg.partition())})
            if (self.dr_cnt % self.disprate) == 0:
                self.logger.debug("producer: delivered message to {} [{}] at offset {} in {}s".format(
                    msg.topic(), msg.partition(), msg.offset(), msg.latency()))

    def produce_record(self):
        """ Asynchronously produce a single record, but block and
            and retry if buffer is full. """
        record = SoakRecord(self.producer_msgid)

        txcnt = 0
        while True:
            txcnt += 1

            try:
                self.producer.produce(self.topic, value=record.serialize(),
                                      headers={"msgid": str(record.msgid),
                                               "time": str(time.time()),
                                               "txcnt": str(txcnt)},
                                      on_delivery=self.dr_cb)
                break

            except BufferError:
                self.producer.poll(1)
                continue

        self.producer_msgid += 1
        self.incr_counter("producer.send", 1)

    def producer_status(self):
        """ Print producer status """
        self.logger.info("producer: {} messages produced, {} delivered, {} failed, {} error_cbs".format(
            self.producer_msgid, self.dr_cnt, self.dr_err_cnt,
            self.producer_error_cb_cnt))

    def producer_run(self):
        """ Producer main loop """
        sleep_intvl = 1.0 / self.rate

        self.producer_msgid = 0
        self.dr_cnt = 0
        self.dr_err_cnt = 0
        self.producer_error_cb_cnt = 0

        next_status = time.time() + self.disprate

        while self.run:

            # Produce a single record
            self.produce_record()

            # Enforce message rate by polling until interval is exceeded.
            now = time.time()
            t_end = now + sleep_intvl
            while True:
                if now > next_status:
                    # Print status
                    self.producer_status()
                    next_status = now + self.disprate

                remaining_time = t_end - now
                if remaining_time < 0:
                    remaining_time = 0
                self.producer.poll(remaining_time)
                if remaining_time <= 0:
                    break
                now = time.time()

        # Wait for outstanding messages to be delivered.
        remaining = self.producer.flush(30)
        self.logger.warning("producer: {} message(s) remaining in queue after flush()".format(remaining))
        self.producer_status()

    def producer_thread_main(self):
        """ Producer thread main function """
        try:
            self.producer_run()
        except KeyboardInterrupt:
            self.logger.info("producer: aborted by user")
            self.run = False
        except Exception as ex:
            self.logger.fatal("producer: fatal exception: {}:\n{}".format(
                ex, traceback.print_exc()))
            self.run = False

    def consumer_status(self):
        """ Print consumer status """
        self.logger.info("consumer: {} messages consumed, {} duplicates, "
                         "{} missed, {} message errors, {} consumer errors, {} error_cbs".format(
                             self.msg_cnt, self.msg_dup_cnt, self.msg_miss_cnt,
                             self.msg_err_cnt, self.consumer_err_cnt,
                             self.consumer_error_cb_cnt))

    def consumer_run(self):
        """ Consumer main loop """
        self.consumer_msgid_next = 0

        self.consumer.subscribe([self.topic])

        self.msg_cnt = 0
        self.msg_dup_cnt = 0
        self.msg_miss_cnt = 0
        self.msg_err_cnt = 0
        self.consumer_err_cnt = 0
        self.consumer_error_cb_cnt = 0
        self.last_commited = None

        # Keep track of high-watermarks to make sure we don't go backwards
        hwmarks = defaultdict(int)

        next_status = time.time() + self.disprate

        while self.run:

            now = time.time()
            if now > next_status:
                # Print status
                self.consumer_status()
                next_status = now + self.disprate

            msg = self.consumer.poll(1)
            if msg is None:
                continue

            if msg.error() is not None:
                self.logger.error("consumer: error: {}".format(msg.error()))
                self.consumer_err_cnt += 1
                self.incr_counter("consumer.error", 1)
                continue

            try:
                # Deserialize message
                record = SoakRecord.deserialize(msg.value())  # noqa unused variable
            except ValueError as ex:
                self.logger.info("consumer: Failed to deserialize message in "
                                 "{} [{}] at offset {} (headers {}): {}".format(
                                     msg.topic(), msg.partition(), msg.offset(), msg.headers(), ex))
                self.msg_err_cnt += 1
                self.incr_counter("consumer.msgerr", 1)

            self.msg_cnt += 1
            self.incr_counter("consumer.msg", 1)

            # end-to-end latency
            headers = dict(msg.headers())
            txtime = headers.get('time', None)
            if txtime is not None:
                latency = time.time() - float(txtime)
                self.set_gauge("consumer.e2e_latency", latency,
                               tags={"partition": "{}".format(msg.partition())})
            else:
                latency = None

            if (self.msg_cnt % self.disprate) == 0:
                # Show a sample message every #disprate messages
                self.logger.info("consumer: {} messages consumed: Message {} "
                                 "[{}] at offset {} (headers {}, latency {})".format(
                                     self.msg_cnt,
                                     msg.topic(), msg.partition(),
                                     msg.offset(), msg.headers(), latency))

            # Keep track of consumer's highwater mark for each partition,
            # to identify duplicates and lost messages.
            hwkey = "{}-{}".format(msg.topic(), msg.partition())
            hw = hwmarks[hwkey]

            if hw > 0:
                if msg.offset() <= hw:
                    self.logger.warning("consumer: Old or duplicate message {} "
                                        "[{}] at offset {} (headers {}): wanted offset > {} (last commited {})".format(
                                            msg.topic(), msg.partition(),
                                            msg.offset(), msg.headers(), hw,
                                            self.last_committed))
                    self.msg_dup_cnt += (hw + 1) - msg.offset()
                    self.incr_counter("consumer.msgdup", 1)
                elif msg.offset() > hw + 1:
                    self.logger.warning("consumer: Lost messages, now at {} "
                                        "[{}] at offset {} (headers {}): "
                                        "expected offset {}+1 (last committed {})".format(
                                            msg.topic(), msg.partition(),
                                            msg.offset(), msg.headers(), hw,
                                            self.last_committed))
                    self.msg_miss_cnt += msg.offset() - (hw + 1)
                    self.incr_counter("consumer.missedmsg", 1)

            hwmarks[hwkey] = msg.offset()

        self.consumer.close()
        self.consumer_status()

    def consumer_thread_main(self):
        """ Consumer thread main function """
        try:
            self.consumer_run()
        except KeyboardInterrupt:
            self.logger.info("consumer: aborted by user")
            self.run = False
        except Exception as ex:
            self.logger.fatal("consumer: fatal exception: {}\n{}".format(
                ex, traceback.print_exc()))
            self.run = False

    def consumer_error_cb(self, err):
        """ Consumer error callback """
        self.logger.error("consumer: error_cb: {}".format(err))
        self.consumer_error_cb_cnt += 1
        self.incr_counter("consumer.errorcb", 1)

    def consumer_commit_cb(self, err, partitions):
        """ Auto commit result callback """
        if err is not None:
            self.logger.error("consumer: offset commit failed for {}: {}".format(partitions, err))
            self.consumer_err_cnt += 1
            self.incr_counter("consumer.error", 1)
        else:
            self.last_committed = partitions

    def producer_error_cb(self, err):
        """ Producer error callback """
        self.logger.error("producer: error_cb: {}".format(err))
        self.producer_error_cb_cnt += 1
        self.incr_counter("producer.errorcb", 1)

    def rtt_stats(self, d):
        """ Extract broker rtt statistics from the stats dict in @param d """

        # Get leader RTT stats
        for broker in d['brokers'].values():
            if broker['toppars'] is None:
                continue

            parts = ','.join([str(x['partition']) for x in broker['toppars'].values()])

            tags = {"broker": "{}".format(broker['nodeid']),
                    "partitions": "{}".format(parts),
                    "type": "{}".format(d['type'])}

            self.set_gauge("broker.rtt.p99",
                           float(broker['rtt']['p99']) / 1000000.0, tags=tags)
            self.set_gauge("broker.rtt.avg",
                           float(broker['rtt']['avg']) / 1000000.0, tags=tags)

    def stats_cb(self, json_str):
        """ Common statistics callback. """
        d = json.loads(json_str)

        # Print number of connected brokers to monitor
        # the sparse connection functionality.
        brokers = d['brokers']
        broker_cnt = len(brokers)
        up_brokers = [brokers[x]['name'] for x in brokers if brokers[x]['state'] == 'UP']
        if self.topic in d['topics']:
            leaders = ['{}={}'.format(p['partition'], p['leader'])
                       for p in d['topics'][self.topic]['partitions'].values() if p['partition'] != -1]
        else:
            leaders = []
        self.logger.info("{} stats: {}/{} brokers UP, {} partition leaders: {}".format(
            d['name'], len(up_brokers), broker_cnt, self.topic, leaders))

        # Emit the full raw stats for troubleshooting.
        self.stats_cnt[d['type']] += 1
        self.logger.info("{} raw stats: {}".format(d['name'], json_str))

        self.rtt_stats(d)

        # Sample the producer queue length
        if d['type'] == 'producer':
            self.set_gauge("producer.outq", len(self.producer))

    def create_topic(self, topic, conf):
        """ Create the topic if it doesn't already exist """
        admin = AdminClient(conf)
        fs = admin.create_topics([NewTopic(topic, num_partitions=2, replication_factor=3)])
        f = fs[topic]
        try:
            res = f.result()  # noqa unused variable
        except KafkaException as ex:
            if ex.args[0].code() == KafkaError.TOPIC_ALREADY_EXISTS:
                self.logger.info("Topic {} already exists: good".format(topic))
            else:
                raise

    def __init__(self, testid, topic, rate, conf):
        """ SoakClient constructor. conf is the client configuration """
        self.topic = topic
        self.rate = rate
        self.disprate = int(rate * 10)
        self.run = True
        self.stats_cnt = {'producer': 0, 'consumer': 0}
        self.start_time = time.time()

        # OTEL instruments
        self.counters = {}
        self.gauges = {}
        self.gauge_cbs = {}
        self.gauge_values = {}

        self.last_rusage = None
        self.last_rusage_time = None
        self.proc = psutil.Process(os.getpid())

        self.logger = logging.getLogger('soakclient')
        self.logger.setLevel(logging.DEBUG)
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter('%(asctime)-15s %(levelname)-8s %(message)s'))
        self.logger.addHandler(handler)

        # Construct a unique id to use for metrics hostname so that
        # multiple instances of the SoakClient can run on the same machine.
        hostname = os.environ["HOSTNAME"]
        self.hostname = "py-{}-{}".format(hostname, self.topic)
        self.testid = testid
        self.meter = metrics.get_meter("njc.python.soak.tests")

        self.logger.info("SoakClient id {}".format(self.hostname))

        if 'group.id' not in conf:
            # Generate a group.id bound to this client and python version
            conf['group.id'] = 'soakclient-{}-{}-{}'.format(
                self.hostname, version()[0], sys.version.split(' ')[0])

        conf = {k: v for k, v in conf.items()}

        def filter_config(conf, filter_out, strip_prefix):
            len_sp = len(strip_prefix)
            out = {}
            for k, v in conf.items():
                if len([x for x in filter_out if k.startswith(x)]) > 0:
                    continue
                if k.startswith(strip_prefix):
                    k = k[len_sp:]
                out[k] = v
            return out

        # Create topic (might already exist)
        aconf = filter_config(conf, ["consumer.", "producer."], "admin.")
        self.create_topic(self.topic, aconf)

        #
        # Create Producer and Consumer, each running in its own thread.
        #
        conf['stats_cb'] = self.stats_cb
        conf['statistics.interval.ms'] = 120000

        # Producer
        pconf = filter_config(conf, ["consumer.", "admin."], "producer.")
        pconf['error_cb'] = self.producer_error_cb
        self.producer = Producer(pconf)

        # Consumer
        cconf = filter_config(conf, ["producer.", "admin."], "consumer.")
        cconf['error_cb'] = self.consumer_error_cb
        cconf['on_commit'] = self.consumer_commit_cb
        self.logger.info("consumer: using group.id {}".format(cconf['group.id']))
        self.consumer = Consumer(cconf)

        # Create and start producer thread
        self.producer_thread = threading.Thread(target=self.producer_thread_main)
        self.producer_thread.start()

        # Create and start consumer thread
        self.consumer_thread = threading.Thread(target=self.consumer_thread_main)
        self.consumer_thread.start()

    def terminate(self):
        """ Terminate Producer and Consumer """
        soak.logger.info("Terminating (ran for {}s)".format(time.time() - self.start_time))
        self.run = False
        # Wait for background threads to finish.
        self.producer_thread.join()
        self.consumer_thread.join()

        # Final resource usage
        soak.get_rusage()

    def incr_counter(self, metric_name, incrval, tags=None):
        """ Increment metric counter by incrval """
        if not tags:
            tags = {}
        tags.update({
            "host": self.hostname,
            "testid": self.testid
        })

        full_metric_name = self.METRIC_PFX + metric_name
        if full_metric_name not in self.counters:
            self.counters[full_metric_name] = self.meter.create_counter(
                full_metric_name,
                description=full_metric_name,
            )
        counter = self.counters[full_metric_name]
        counter.add(incrval, tags)

    def set_gauge(self, metric_name, val, tags=None):
        """ Set metric gauge to val """
        if not tags:
            tags = {}
        tags.update({
            "host": self.hostname,
            "testid": self.testid
        })

        full_metric_name = self.METRIC_PFX + metric_name
        if full_metric_name not in self.gauge_values:
            self.gauge_values[full_metric_name] = []

        self.gauge_values[full_metric_name].append([val, tags])

        if full_metric_name not in self.gauges:
            def cb(_):
                for value in self.gauge_values[full_metric_name]:
                    yield metrics.Observation(value[0], value[1])
                self.gauge_values[full_metric_name] = []

            self.gauge_cbs[full_metric_name] = cb
            self.gauges[full_metric_name] = self.meter.create_observable_gauge(
                callbacks=[self.gauge_cbs[full_metric_name]],
                name=full_metric_name,
                description=full_metric_name
            )

    def calc_rusage_deltas(self, curr, prev, elapsed):
        """ Calculate deltas between previous and current resource usage """

        # User CPU %
        user_cpu = ((curr.ru_utime - prev.ru_utime) / elapsed) * 100.0
        self.set_gauge("cpu.user", user_cpu)

        # System CPU %
        sys_cpu = ((curr.ru_stime - prev.ru_stime) / elapsed) * 100.0
        self.set_gauge("cpu.system", sys_cpu)

        # Max RSS memory (monotonic)
        max_rss = curr.ru_maxrss / 1024.0
        self.set_gauge("memory.rss.max", max_rss)

        self.logger.info("User CPU: {:.1f}%, System CPU: {:.1f}%, MaxRSS {:.3f}MiB".format(
            user_cpu, sys_cpu, max_rss))

    def get_rusage(self):
        """ Get resource usage and calculate CPU load, etc """
        ru = resource.getrusage(resource.RUSAGE_SELF)
        now = time.time()

        if self.last_rusage is not None:
            self.calc_rusage_deltas(ru, self.last_rusage,
                                    now - self.last_rusage_time)

        self.last_rusage = ru
        self.last_rusage_time = now

        # Current RSS memory
        rss = float(self.proc.memory_info().rss) / (1024.0*1024.0)
        self.set_gauge("memory.rss", rss)


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Kafka client soak test')
    parser.add_argument('-i', dest='testid', type=str, required=True, help='Test id')
    parser.add_argument('-b', dest='brokers', type=str, default=None, help='Bootstrap servers')
    parser.add_argument('-t', dest='topic', type=str, required=True, help='Topic to use')
    parser.add_argument('-r', dest='rate', type=float, default=10, help='Message produce rate per second')
    parser.add_argument('-f', dest='conffile', type=argparse.FileType('r'),
                        help='Configuration file (configprop=value format)')

    args = parser.parse_args()

    conf = dict()
    if args.conffile is not None:
        # Parse client configuration file.
        # Standard "key=value" format.
        for line in args.conffile:
            line = line.strip()
            if len(line) == 0 or line[0] == '#':
                continue

            i = line.find('=')
            if i <= 0:
                raise ValueError("Configuration lines must be `name=value..`, not {}".format(line))

            name = line[:i]
            value = line[i+1:]

            conf[name] = value

    if args.brokers is not None:
        # Overwrite any brokers specified in configuration file with
        # brokers from -b command line argument
        conf['bootstrap.servers'] = args.brokers

    # We don't care about partition EOFs
    conf['enable.partition.eof'] = False

    # Create SoakClient
    soak = SoakClient(args.testid, args.topic, args.rate, conf)

    # Get initial resource usage
    soak.get_rusage()

    # Run until interrupted
    try:
        while soak.run:
            time.sleep(10)
            soak.get_rusage()

        soak.logger.info("Soak client aborted")

    except (KeyboardInterrupt):
        soak.logger.info("Interrupted by user")
    except Exception as ex:
        soak.logger.error("Fatal exception {}\n{}".format(
            ex, traceback.print_exc()))

    # Terminate
    soak.terminate()
