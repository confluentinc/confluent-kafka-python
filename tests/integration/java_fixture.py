#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
#
# Copyright 2020 Confluent Inc.
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

import os
from select import select
import subprocess
from sys import version_info as interpreter_version
from uuid import uuid1


def py3_bytes(str_val):
    """
    Converts Python3 string objects(unicode) to a utf-8 encoded byte sequence.

    :param str str_val: string to be converted to bytes
    :returns: utf-8 encoded byte array
    :rtype: bytes
    """
    return str_val.encode('utf-8')


def py2_bytes(str_val):
    """
    Pass through function for Python{2,3} compatibility.

    :param str str_val:
    :returns: bytes encoded with the platform's source encoding type.
    :rtype: bytes
    """
    return str_val


py_bytes = py3_bytes if interpreter_version >= (3, 4) else py2_bytes


def log_file(log_dir, file_name=str(uuid1())):
    log_file = open(os.path.join(log_dir, "{}.log".format(file_name)), "w+")
    print("Java client logs written to {}".format(log_file.name))
    return log_file


def _read(fd, timeout):
    read_ready, _, _ = select([fd], [], [], timeout)
    if fd in read_ready:
        return fd.readline().rstrip()

    return None


def _send_cmd(fd, cmd, timeout):
    _, write_ready, _ = select([], [fd], [], timeout)

    if fd in write_ready:
        fd.write(py_bytes(cmd))
        fd.flush()


class JavaMessage(object):
    __slots__ = ["_topic", "_partition", "_offset", "_key", "_value"]

    def __init__(self, buff):
        if interpreter_version >= (3, 4):
            buff = buff.decode('utf-8')

        # Handle Delivery Report(3) and consumer poll(5) responses
        payload = buff.split(':', 5)
        if len(payload) == 5:
            self._topic, self._partition, self._offset, self._key, self._value = payload
        else:
            self._topic, self._partition, self._offset = payload

    def topic(self):
        """
        Delivery Report topic

        :returns: Message topic name
        :rtype: str
        """
        return self._topic

    def partition(self):
        """
        Delivery Report Partition

        :returns: Message TopicPartition number
        :rtype: int
        """
        return self._partition

    def offset(self):
        """
        Delivery Report Offset

        :returns: Message Offset
        :rtype: int
        """
        return self._offset

    def key(self):
        return self._key if self._key != "null" else None

    def value(self):
        return self._value if self._value != "null" else None

    def __str__(self):
        return "Topic {} Partition {} Offset {} Key {} Value {}".format(self.topic(), self.partition(), self.offset(),
                                                                        self.key(), self.value())


class JavaProducer(object):
    __slots__ = ["_producer"]

    def __init__(self, args, cluster):
        # Add producer configs
        cmd = ["java/run-class.sh", "JavaTestRunner", "producer"]
        conf = ["{}={}".format(k, v) for k, v in args.items()]

        self._producer = subprocess.Popen(' '.join(cmd + conf),
                                          shell=True,
                                          env=cluster.env,
                                          stdin=subprocess.PIPE,
                                          stdout=subprocess.PIPE,
                                          stderr=log_file(cluster.cluster.root_path,
                                                          "producer-{}".format(args.get('client.id'))))

    def __del__(self):
        self._producer.terminate()

    def _send_cmd(self, cmd, timeout=1.0):
        _send_cmd(self._producer.stdin, "{}\n".format(cmd), timeout)

    def produce(self, topic, value, key=None):
        """
        Produce message to topic.

        :param topic: topic to produce message to
        :param value: message value
        :param key: optional message key
        :returns: None
        """
        if key is not None:
            self._send_cmd("send {} {}:{}".format(topic, key, value))
        else:
            self._send_cmd("send {} {}".format(topic, value))

    def flush(self, timeout=1.0):
        """
        Wait for all messages in the Producer queue to be delivered.

        :param float timeout: amount of time to block waiting for flush to return
        :return:
        """
        self._send_cmd("flush")

        drs = _read(self._producer.stdout, timeout)
        if drs is not None:
            return [JavaMessage(dr) for dr in drs.split('/t')]
        return None


class JavaConsumer(object):
    __slots__ = ["_consumer"]

    def __init__(self, args, cluster):
        cmd = ["java/run-class.sh", "JavaTestRunner", "consumer"]
        conf = (["{}={}".format(k, v) for k, v in args.items()])
        self._consumer = subprocess.Popen(' '.join(cmd + conf),
                                          shell=True,
                                          env=cluster.env,
                                          stdin=subprocess.PIPE,
                                          stdout=subprocess.PIPE,
                                          stderr=log_file(cluster.cluster.root_path,
                                                          "consumer-{}".format(args.get('client.id'))))

    def _send_cmd(self, cmd, timeout=1.0):
        _send_cmd(self._consumer.stdin, "{}\n".format(cmd), timeout)

    def _read_response(self, timeout=1.0):
        return _read(self._consumer.stdout, timeout)

    def subscribe(self, topic):
        """
        Set subscription to supplied list of topics This replaces a previous subscription.

        Regexp pattern subscriptions are supported by prefixing the topic string with "^", e.g.:

        :param list(str) topic: csv list of topics or patterns
        :returns: void
        :rtype: None
        """
        self._send_cmd("subscribe {}".format(",".join(topic)))

    def poll(self, timeout=4.0):
        """
        Requests a consumer record from the JavaConsumer.

        :param float timeout: Maximum time to block waiting for a message
        :returns: message key and value as a string delimited with ':'
        :rtype: JavaMessage
        """

        if self._consumer is None:
            raise ValueError("subscribe must be called prior to poll")

        self._send_cmd("poll {}".format(int(timeout)))

        resp = _read(self._consumer.stdout, timeout)

        return JavaMessage(resp)

    def close(self):
        self._send_cmd("close")
