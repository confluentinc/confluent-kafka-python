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
#  tests/soak/soakclient.py -t soaktest.py -r 80 -f <client-conf-file>
#

from confluent_kafka import KafkaError, KafkaException, version
from confluent_kafka import Producer, Consumer
from confluent_kafka.admin import AdminClient, NewTopic
from collections import defaultdict
from builtins import int
import argparse
import threading
import time
import json
import logging
import sys


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
    """

    def dr_cb(self, err, msg):
        """ Producer delivery report callback """
        if err is not None:
            self.logger.warning("producer: delivery failed: {} [{}]: {}".format(msg.topic(), msg.partition(), err))
            self.dr_err_cnt += 1
        else:
            self.dr_cnt += 1
            if (self.dr_cnt % self.disprate) == 0:
                self.logger.debug("producer: delivered message to {} [{}] at offset {}".format(
                    msg.topic(), msg.partition(), msg.offset()))

    def produce_record(self):
        """ Produce a single record """
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

    def producer_stats(self):
        """ Print producer stats """
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

        next_stats = time.time() + 10

        while self.run:

            self.produce_record()

            now = time.time()
            t_end = now + sleep_intvl
            while True:
                if now > next_stats:
                    self.producer_stats()
                    next_stats = now + 10

                remaining_time = t_end - now
                if remaining_time < 0:
                    remaining_time = 0
                self.producer.poll(remaining_time)
                if remaining_time <= 0:
                    break
                now = time.time()

        remaining = self.producer.flush(30)
        self.logger.warning("producer: {} message(s) remaining in queue after flush()".format(remaining))
        self.producer_stats()

    def producer_thread_main(self):
        """ Producer thread main function """
        try:
            self.producer_run()
        except KeyboardInterrupt:
            self.logger.info("producer: aborted by user")
            self.run = False
        except Exception as ex:
            self.logger.fatal("producer: fatal exception: {}".format(ex))
            self.run = False

    def consumer_stats(self):
        """ Print consumer stats """
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

        while self.run:
            msg = self.consumer.poll(1)
            if msg is None:
                continue

            if msg.error() is not None:
                self.logger.error("consumer: error: {}".format(msg.error()))
                self.consumer_err_cnt += 1
                continue

            try:
                record = SoakRecord.deserialize(msg.value()) # noqa unused variable
            except ValueError as ex:
                self.logger.info("consumer: Failed to deserialize message in "
                                 "{} [{}] at offset {} (headers {}): {}".format(
                                     msg.topic(), msg.partition(), msg.offset(), msg.headers(), ex))
                self.msg_err_cnt += 1

            self.msg_cnt += 1

            if (self.msg_cnt % self.disprate) == 0:
                self.logger.info("consumer: {} messages consumed: Message {} "
                                 "[{}] at offset {} (headers {}, latency {})".format(
                                     self.msg_cnt,
                                     msg.topic(), msg.partition(),
                                     msg.offset(), msg.headers(),
                                     time.time() - float(dict(msg.headers())['time'])))
                self.consumer_stats()

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
                elif msg.offset() > hw + 1:
                    self.logger.warning("consumer: Lost messages, now at {} "
                                        "[{}] at offset {} (headers {}): "
                                        "expected offset {}+1 (last committed {})".format(
                                            msg.topic(), msg.partition(),
                                            msg.offset(), msg.headers(), hw,
                                            self.last_committed))
                    self.msg_miss_cnt += msg.offset() - (hw + 1)

            hwmarks[hwkey] = msg.offset()

        self.consumer.close()
        self.consumer_stats()

    def consumer_thread_main(self):
        """ Consumer thread main function """
        try:
            self.consumer_run()
        except KeyboardInterrupt:
            self.logger.info("consumer: aborted by user")
            self.run = False
        except Exception as ex:
            self.logger.fatal("consumer: fatal exception: {}".format(ex))
            self.run = False

    def consumer_error_cb(self, err):
        """ Consumer error callback """
        self.logger.error("consumer: error_cb: {}".format(err))
        self.consumer_error_cb_cnt += 1

    def consumer_commit_cb(self, err, partitions):
        """ Auto commit result callback """
        if err is not None:
            self.logger.error("consumer: offset commit failed for {}: {}".format(partitions, err))
            self.consumer_err_cnt += 1
        else:
            self.last_committed = partitions

    def producer_error_cb(self, err):
        """ Producer error callback """
        self.logger.error("producer: error_cb: {}".format(err))
        self.producer_error_cb_cnt += 1

    def stats_cb(self, json_str):
        """ Common statistics callback.
            Extracts the number of connected brokers and prints it,
            to monitor the sparse connection functionality. """
        d = json.loads(json_str)
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
        self.stats_cnt[d['type']] += 1
        if (self.stats_cnt[d['type']] % 10) == 0:
            self.logger.info("{} raw stats: {}".format(d['name'], json_str))

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

    def __init__(self, topic, rate, conf):
        """ SoakClient constructor. conf is the client configuration """
        self.topic = topic
        self.rate = rate
        self.disprate = int(rate * 10)
        self.run = True
        self.stats_cnt = {'producer': 0, 'consumer': 0}
        self.start_time = time.time()

        self.logger = logging.getLogger('soakclient')
        self.logger.setLevel(logging.DEBUG)
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter('%(asctime)-15s %(levelname)-8s %(message)s'))
        self.logger.addHandler(handler)

        # Create topic (might already exist)
        self.create_topic(self.topic, conf)

        #
        # Create Producer and Consumer, each running in its own thread.
        #
        conf['stats_cb'] = self.stats_cb
        conf['statistics.interval.ms'] = 10000

        # Producer
        conf['error_cb'] = self.producer_error_cb
        self.producer = Producer(conf)

        # Consumer
        conf['error_cb'] = self.consumer_error_cb
        conf['on_commit'] = self.consumer_commit_cb
        self.logger.info("consumer: using group.id {}".format(conf['group.id']))
        self.consumer = Consumer(conf)

        self.producer_thread = threading.Thread(target=self.producer_thread_main)
        self.producer_thread.start()

        self.consumer_thread = threading.Thread(target=self.consumer_thread_main)
        self.consumer_thread.start()

    def terminate(self):
        """ Terminate Producer and Consumer """
        soak.logger.info("Terminating (ran for {}s)".format(time.time() - self.start_time))
        self.run = False
        self.producer_thread.join()
        self.consumer_thread.join()


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Kafka client soak test')
    parser.add_argument('-b', dest='brokers', type=str, default=None, help='Bootstrap servers')
    parser.add_argument('-t', dest='topic', type=str, required=True, help='Topic to use')
    parser.add_argument('-r', dest='rate', type=float, default=10, help='Message produce rate per second')
    parser.add_argument('-f', dest='conffile', type=argparse.FileType('r'),
                        help='Configuration file (configprop=value format)')

    args = parser.parse_args()

    conf = dict()
    if args.conffile is not None:
        # Parse client configuration file
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

    if 'group.id' not in conf:
        # Generate a group.id bound to this client and python version
        conf['group.id'] = 'soakclient.py-{}-{}'.format(version()[0], sys.version.split(' ')[0])

    # We don't care about partition EOFs
    conf['enable.partition.eof'] = False

    # Create SoakClient
    soak = SoakClient(args.topic, args.rate, conf)

    # Run until interrupted
    try:
        while soak.run:
            time.sleep(10)

        soak.logger.info("Soak client aborted")

    except (KeyboardInterrupt):
        soak.logger.info("Interrupted by user")
    except Exception as e:
        soak.logger.error("Fatal exception {}".format(e))

    # Terminate
    soak.terminate()
