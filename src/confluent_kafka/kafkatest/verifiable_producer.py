#!/usr/bin/env python
#
# Copyright 2016-2026 Confluent Inc.
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

"""
confluent-kafka-python verifiable producer for Apache Kafka's Ducktape
system tests.

Accepts the standardized verifiable-producer CLI and emits the
newline-delimited JSON event stream (``startup_complete``,
``producer_send_success``, ``producer_send_error``, ``shutdown_complete``,
``tool_data``) parsed by
``apache/kafka/tests/kafkatest/services/verifiable_producer.py``.

Run directly with::

    python -m confluent_kafka.kafkatest.verifiable_producer <options>
"""

import argparse
import time

try:
    from confluent_kafka.kafkatest.verifiable_client import VerifiableClient
except ImportError:
    # Allow running as a plain script (python verifiable_producer.py ...).
    from verifiable_client import VerifiableClient

from confluent_kafka import KafkaException, Producer


class VerifiableProducer(VerifiableClient):
    """confluent-kafka-python backed VerifiableProducer for Kafka's
    kafkatest client tests."""

    def __init__(self, conf):
        super(VerifiableProducer, self).__init__(conf)
        producer_conf = dict(self.conf)
        producer_conf['on_delivery'] = self.dr_cb
        self.producer = Producer(**producer_conf)
        self.num_sent = 0
        self.num_acked = 0
        self.num_err = 0
        self.target_throughput = -1
        self.start_ms = self._now_ms()

    def dr_cb(self, err, msg):
        """Per-message delivery report callback, served from poll()."""
        if err:
            self.num_err += 1
            self.send({
                'name': 'producer_send_error',
                'topic': msg.topic(),
                'key': _to_str(msg.key()),
                'value': _to_str(msg.value()),
                'message': str(err),
                'exception': err.name() if hasattr(err, 'name') else str(err),
            })
        else:
            self.num_acked += 1
            self.send({
                'name': 'producer_send_success',
                'topic': msg.topic(),
                'partition': msg.partition(),
                'offset': msg.offset(),
                'key': _to_str(msg.key()),
                'value': _to_str(msg.value()),
            })

    def emit_tool_data(self):
        """Emit the final tool_data summary event."""
        elapsed_ms = self._now_ms() - self.start_ms
        avg = (self.num_acked * 1000.0 / elapsed_ms) if elapsed_ms > 0 else 0.0
        self.send({
            'name': 'tool_data',
            'sent': self.num_sent,
            'acked': self.num_acked,
            'target_throughput': self.target_throughput,
            'avg_throughput': round(avg, 2),
        })


def _to_str(b):
    """Decode message key/value bytes to str for JSON; None stays None."""
    if b is None:
        return None
    if isinstance(b, bytes):
        return b.decode('utf-8', errors='replace')
    return b


def main():
    parser = argparse.ArgumentParser(description='Verifiable Python Producer')
    parser.add_argument('--topic', type=str, required=True)
    parser.add_argument('--broker-list', dest='conf_bootstrap.servers',
                        help='Bootstrap broker(s); also --bootstrap-server')
    parser.add_argument('--bootstrap-server', dest='conf_bootstrap.servers')
    # -1 = infinite. (The legacy client silently capped this at 1e6.)
    parser.add_argument('--max-messages', type=int, dest='max_messages',
                        default=-1)
    parser.add_argument('--throughput', type=int, default=-1,
                        help='Msgs/sec; -1 = unlimited')
    # 'acks' is a global producer config (alias for request.required.acks).
    parser.add_argument('--acks', type=str, dest='conf_acks', default='-1')
    parser.add_argument('--value-prefix', dest='value_prefix', type=str,
                        default=None)
    parser.add_argument('--repeating-keys', type=int, dest='repeating_keys',
                        default=0)
    parser.add_argument('--message-create-time', type=int, dest='create_time',
                        default=-1, help='Epoch ms baseline for timestamps')
    parser.add_argument('--command-config', dest='command_config',
                        help='Client properties file')
    parser.add_argument('--producer.config', dest='command_config',
                        help='Client properties file (deprecated alias)')
    parser.add_argument('--debug', dest='conf_debug',
                        help='librdkafka debug flags')
    parser.add_argument('-X', nargs=1, dest='extra_conf', action='append',
                        help='Raw librdkafka property key=value', default=[])
    args = vars(parser.parse_args())

    if args.get('conf_bootstrap.servers') is None:
        parser.error('--bootstrap-server (or --broker-list) is required')

    # Build the full config (flags + --command-config file + -X overrides +
    # JAAS), then construct the producer once.
    conf = VerifiableClient.build_conf(args, 'command_config')
    # No retries, so each delivery report reflects a single produce attempt.
    conf.setdefault('retries', 0)

    vp = VerifiableProducer(conf)

    topic = args['topic']
    max_messages = args['max_messages']
    throughput = args['throughput']
    create_time = args['create_time']
    repeating_keys = args['repeating_keys']
    vp.target_throughput = throughput

    if args['value_prefix'] is not None:
        value_fmt = args['value_prefix'] + '.%d'
    else:
        value_fmt = '%d'

    # Seconds between messages for rate limiting, or 0 for unlimited.
    delay = (1.0 / throughput) if throughput > 0 else 0.0

    vp.dbg('Producing %s messages at a rate of %s/s'
           % ('unlimited' if max_messages < 0 else max_messages, throughput))

    vp.start_ms = vp._now_ms()
    vp.emit_event('startup_complete')

    key_counter = 0
    counter = 0
    try:
        while vp.run and (max_messages < 0 or counter < max_messages):
            t_end = time.time() + delay

            if repeating_keys > 0:
                key = '%d' % (key_counter % repeating_keys)
                key_counter += 1
            else:
                key = None

            kwargs = {'value': value_fmt % counter, 'key': key}
            if create_time >= 0:
                kwargs['timestamp'] = create_time + (vp._now_ms() - vp.start_ms)

            try:
                vp.producer.produce(topic, **kwargs)
                vp.num_sent += 1
                counter += 1
            except BufferError:
                # Local queue full: serve delivery reports and retry the same
                # message without advancing the counter.
                vp.producer.poll(0.1)
                continue
            except KafkaException as e:
                vp.num_err += 1
                vp.err('produce() #%d failed: %s' % (counter, e))

            # Rate-limit: keep polling (serving DRs) until the deadline, but
            # poll at least once so deliveries get served.
            while True:
                remaining = max(0.0, t_end - time.time())
                vp.producer.poll(remaining)
                if remaining <= 1e-8:
                    break

    except KeyboardInterrupt:
        pass

    vp.dbg('Flushing')
    try:
        vp.producer.flush(30)
    except KeyboardInterrupt:
        pass

    vp.emit_event('shutdown_complete')
    vp.emit_tool_data()
    vp.dbg('All done: sent=%d acked=%d err=%d'
           % (vp.num_sent, vp.num_acked, vp.num_err))


if __name__ == '__main__':
    main()
