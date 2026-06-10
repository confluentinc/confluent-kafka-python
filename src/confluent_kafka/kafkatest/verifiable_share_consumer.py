#!/usr/bin/env python
#
# Copyright 2026 Confluent Inc.
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
confluent-kafka-python verifiable KIP-932 share consumer for Apache Kafka's
Ducktape system tests.

Accepts the standardized verifiable-share-consumer CLI and emits the
newline-delimited JSON event stream parsed by
``apache/kafka/tests/kafkatest/services/verifiable_share_consumer.py``:

  * ``startup_complete``           — consumer created
  * ``offset_reset_strategy_set``  — share.auto.offset.reset applied to group
  * ``records_consumed``           — a batch of records was polled
  * ``record_data``                — per-record detail (only with --verbose)
  * ``offsets_acknowledged``       — broker responded to an ack commit
  * ``shutdown_complete``          — consumer closed

Acknowledgement modes (``--acknowledgement-mode``):

  * ``auto``   no explicit commit; the next poll() implicitly accepts the
               previous batch.
  * ``sync``   commit_sync() per batch.
  * ``async``  commit_async() per batch.

All three run with ``share.acknowledgement.mode=implicit`` (records returned
by poll() are auto-accepted); only the commit behavior differs. The
acknowledgement-commit callback is the single source of
``offsets_acknowledged`` events for every mode, so async/auto results are
reported with real broker per-partition success/failure rather than guessed.

Run directly with::

    python -m confluent_kafka.kafkatest.verifiable_share_consumer <options>
"""

import argparse

try:
    from confluent_kafka.kafkatest.verifiable_client import VerifiableClient
except ImportError:
    # Allow running as a plain script.
    from verifiable_client import VerifiableClient

from confluent_kafka import KafkaException, ShareConsumer
from confluent_kafka.admin import (
    AdminClient, AlterConfigOpType, ConfigEntry, ConfigResource, ResourceType)

# How long poll() blocks waiting for a batch, in seconds.
POLL_TIMEOUT_S = 5.0
# How long a synchronous commit may block, in seconds.
COMMIT_TIMEOUT_S = 60.0

ACK_AUTO = 'auto'
ACK_SYNC = 'sync'
ACK_ASYNC = 'async'


class VerifiableShareConsumer(VerifiableClient):
    """confluent-kafka-python backed verifiable share consumer."""

    def __init__(self, conf, ack_mode, verbose):
        super(VerifiableShareConsumer, self).__init__(conf)
        self.ack_mode = ack_mode
        self.verbose = verbose
        self.total_acknowledged = 0

        # share.acknowledgement.mode=implicit for all modes: poll() auto-marks
        # the prior batch ACCEPT. We only vary whether/how we commit.
        share_conf = dict(self.conf)
        share_conf['share.acknowledgement.mode'] = 'implicit'
        self.consumer = ShareConsumer(share_conf)
        # The acknowledgement-commit callback is the single source of
        # offsets_acknowledged for all modes (fires on implicit auto-commit,
        # commit_sync, and commit_async alike).
        self.consumer.set_acknowledgement_commit_callback(
            self.on_acknowledgement_commit)

    # ------------------------------------------------------------------ #
    # Event emission                                                     #
    # ------------------------------------------------------------------ #

    def on_acknowledgement_commit(self, offsets, exception):
        """Acknowledgement-commit callback.

        ``offsets`` is a ``Dict[TopicPartition, set[int]]`` of acknowledged
        offsets per partition; ``exception`` is a KafkaException on failure or
        None on success. Emits one ``offsets_acknowledged`` event covering all
        partitions in this commit response.
        """
        partitions = []
        count = 0
        for tp, offset_set in offsets.items():
            sorted_offsets = sorted(offset_set)
            count += len(sorted_offsets)
            partitions.append({
                'topic': tp.topic,
                'partition': tp.partition,
                'count': len(sorted_offsets),
                'offsets': sorted_offsets,
            })

        if not partitions:
            return

        event = {
            'name': 'offsets_acknowledged',
            'count': count,
            'partitions': partitions,
            'success': exception is None,
        }
        if exception is not None:
            event['error'] = str(exception)
        else:
            self.total_acknowledged += count

        self.send(event)

    def emit_records_consumed(self, batch):
        """Group a polled batch by partition and emit records_consumed."""
        by_partition = {}
        order = []
        for msg in batch:
            key = (msg.topic(), msg.partition())
            if key not in by_partition:
                by_partition[key] = []
                order.append(key)
            by_partition[key].append(msg.offset())

        partitions = []
        for (topic, partition) in order:
            offsets = by_partition[(topic, partition)]
            partitions.append({
                'topic': topic,
                'partition': partition,
                'count': len(offsets),
                'offsets': offsets,
            })

        self.send({
            'name': 'records_consumed',
            'count': len(batch),
            'partitions': partitions,
        })

    def emit_record_data(self, msg):
        self.send({
            'name': 'record_data',
            'topic': msg.topic(),
            'partition': msg.partition(),
            'offset': msg.offset(),
            'key': _to_str(msg.key()),
            'value': _to_str(msg.value()),
        })

    def emit_offset_reset_strategy_set(self, strategy):
        self.send({
            'name': 'offset_reset_strategy_set',
            'offsetResetStrategy': strategy,
        })


def _to_str(b):
    """Decode message key/value bytes to str for JSON; None stays None."""
    if b is None:
        return None
    if isinstance(b, bytes):
        return b.decode('utf-8', errors='replace')
    return b


def set_share_group_offset_reset(conf, group_id, reset_value):
    """Set share.auto.offset.reset on the share group via
    IncrementalAlterConfigs.

    Share groups default to reset=latest, so a fresh group started after
    messages were already produced would consume nothing. Returns nothing;
    raises on failure.
    """
    admin_conf = {k: v for k, v in conf.items()
                  if k in ('bootstrap.servers', 'client.id')
                  or k.startswith(('security.', 'sasl.', 'ssl.'))}
    admin = AdminClient(admin_conf)
    resource = ConfigResource(
        ResourceType.GROUP,
        group_id,
        incremental_configs=[
            ConfigEntry('share.auto.offset.reset', reset_value,
                        incremental_operation=AlterConfigOpType.SET),
        ],
    )
    futures = admin.incremental_alter_configs([resource])
    for future in futures.values():
        future.result()  # raises on failure


def main():
    parser = argparse.ArgumentParser(
        description='Verifiable Python Share Consumer')
    parser.add_argument('--topic', type=str, required=True)
    parser.add_argument('--group-id', dest='conf_group.id', required=True)
    parser.add_argument('--broker-list', dest='conf_bootstrap.servers',
                        help='Bootstrap broker(s); also --bootstrap-server')
    parser.add_argument('--bootstrap-server', dest='conf_bootstrap.servers')
    parser.add_argument('--max-messages', type=int, dest='max_messages',
                        default=-1, help='-1 = infinite')
    parser.add_argument('--acknowledgement-mode', dest='ack_mode',
                        choices=[ACK_AUTO, ACK_SYNC, ACK_ASYNC],
                        default=ACK_AUTO)
    parser.add_argument('--offset-reset-strategy', dest='offset_reset',
                        choices=['earliest', 'latest'], default=None,
                        help='Sets share.auto.offset.reset on the group')
    parser.add_argument('--verbose', action='store_true', default=False,
                        help='Emit record_data per message')
    parser.add_argument('--command-config', dest='command_config',
                        help='Client properties file')
    parser.add_argument('--debug', dest='conf_debug',
                        help='librdkafka debug flags')
    parser.add_argument('-X', nargs=1, dest='extra_conf', action='append',
                        help='Raw librdkafka property key=value', default=[])
    args = vars(parser.parse_args())

    if args.get('conf_bootstrap.servers') is None:
        parser.error('--bootstrap-server (or --broker-list) is required')

    conf = VerifiableClient.build_conf(args, 'command_config')
    topic = args['topic']
    group_id = args['conf_group.id']
    max_messages = args['max_messages']
    ack_mode = args['ack_mode']

    vsc = VerifiableShareConsumer(conf, ack_mode, args['verbose'])
    vsc.emit_event('startup_complete')

    try:
        # Set the group's offset-reset policy before subscribing, if asked.
        if args['offset_reset']:
            set_share_group_offset_reset(conf, group_id, args['offset_reset'])
            vsc.emit_offset_reset_strategy_set(args['offset_reset'])

        vsc.consumer.subscribe([topic])

        while vsc.run and (max_messages < 0
                           or vsc.total_acknowledged < max_messages):
            try:
                batch = vsc.consumer.poll(timeout=POLL_TIMEOUT_S)
            except KafkaException as e:
                vsc.err('poll failed: %s' % e)
                continue

            consumed = []
            for msg in batch:
                if msg.error():
                    vsc.err('share msg error: %s' % msg.error())
                    continue
                consumed.append(msg)
                if vsc.verbose:
                    vsc.emit_record_data(msg)

            if not consumed:
                continue

            vsc.emit_records_consumed(consumed)

            # Commit behavior per mode. offsets_acknowledged is emitted by the
            # acknowledgement-commit callback in every case.
            #   auto:  no explicit commit; next poll() implicitly accepts.
            #   sync:  commit_sync per batch.
            #   async: commit_async per batch (callback fires on a later poll).
            try:
                if ack_mode == ACK_SYNC:
                    vsc.consumer.commit_sync(timeout=COMMIT_TIMEOUT_S)
                elif ack_mode == ACK_ASYNC:
                    vsc.consumer.commit_async()
            except KafkaException as e:
                vsc.err('commit (%s) failed: %s' % (ack_mode, e))

    except KeyboardInterrupt:
        pass
    finally:
        vsc.dbg('Closing share consumer')
        try:
            vsc.consumer.close()
        except Exception as e:
            vsc.dbg('Ignoring exception while closing: %s' % e)
        vsc.emit_event('shutdown_complete')
        vsc.dbg('All done: acknowledged=%d' % vsc.total_acknowledged)


if __name__ == '__main__':
    main()
