#!/usr/bin/env python3
#

import signal
import time

from confluent_kafka import Consumer
from .verifiable_client import VerifiableClient
import argparse
import os
import time
from confluent_kafka import Consumer, KafkaError, KafkaException
from .verifiable_client import VerifiableClient


parser = argparse.ArgumentParser(description='Verifiable Python Consumer')

parser.add_argument('--bootstrap-server', dest='conf_bootstrap.servers')

args = vars(parser.parse_args())

conf = {'broker.version.fallback': '0.9.0',
        # Do explicit manual offset stores to avoid race conditions
        # where a message is consumed from librdkafka but not yet handled
        # by the Python code that keeps track of last consumed offset.
        'enable.auto.offset.store': False}

if args.get('consumer_config', None) is not None:
    args.update(VerifiableClient.read_config_file(args['consumer_config']))

args.update([x[0].split('=') for x in args.get('extra_conf', [])])

VerifiableClient.set_config(conf, args)

class VerifiableConsumer(VerifiableClient):
    """
    confluent-kafka-python backed VerifiableConsumer class for use with
    Kafka's kafkatests client tests.
    """
    def __init__(self, conf):
        """
        conf is a config dict passed to confluent_kafka.Consumer()
        """
        super(VerifiableConsumer, self).__init__(conf)
        self.conf['on_commit'] = self.on_commit
        self.conf['debug'] = 'all'
        self.consumer = Consumer(**conf)
        self.consumed_msgs = 0
        self.consumed_msgs_last_reported = 0
        self.consumed_msgs_at_last_commit = 0
        self.use_auto_commit = False
        self.use_async_commit = False
        self.max_msgs = -1
        self.assignment = []
        self.assignment_dict = dict()

vc = VerifiableConsumer(conf)

run = True

def handler(signum, frame):
    print(f'Signal {signum} received')
    global run
    run = False

signal.signal(signal.SIGINT, handler)

while run:
    #print(12345)
    time.sleep(1)

print("Jing Liu done")

