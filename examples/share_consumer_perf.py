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
# KIP-932 share-consumer performance tool — Python counterpart of the Java
# `kafka-share-consumer-perf-test.sh` (org.apache.kafka.tools.ShareConsumerPerformance)
# and the librdkafka `rdkafka_performance -S` mode.
#
# It consumes `--num-records` records from one or more topics in a share group
# and reports throughput. Option names, defaults, the client settings
# (fetch.max.bytes / socket.receive.buffer.bytes) and the CSV stats header are
# kept identical to the Java tool so the three clients are measured the same way.
#
# Example:
#   python share_consumer_perf.py \
#       --bootstrap-server localhost:9092 \
#       --topic perf \
#       --num-records 1000000 \
#       --show-detailed-stats
#

import argparse
import sys
import time

from confluent_kafka import ShareConsumer

# Java ShareConsumerPerformance defaults.
DEFAULT_GROUP = 'perf-share-consumer'
DEFAULT_FETCH_SIZE = 1024 * 1024        # --fetch-size       -> fetch.max.bytes (1 MiB)
DEFAULT_SOCKET_BUFFER = 2 * 1024 * 1024  # --socket-buffer-size -> socket.receive.buffer.bytes (2 MiB)
DEFAULT_TIMEOUT_MS = 10_000             # max time between returned records
DEFAULT_REPORTING_INTERVAL_MS = 5_000


def read_properties(path):
    """Parse a key=value properties file (same format as librdkafka -X file=
    and Java --command-config)."""
    cfg = {}
    with open(path) as fp:
        for line in fp:
            line = line.strip()
            if not line or line.startswith('#') or '=' not in line:
                continue
            k, v = line.split('=', 1)
            cfg[k.strip()] = v.strip()
    return cfg


def parse_args():
    p = argparse.ArgumentParser(
        description='KIP-932 share-consumer performance test (Java-parity).')
    p.add_argument('--bootstrap-server', required=True,
                   help='REQUIRED. The server(s) to connect to.')
    p.add_argument('--topic', required=True,
                   help='REQUIRED. The topic(s) to consume from (comma-separated).')
    p.add_argument('--group', default=DEFAULT_GROUP,
                   help='The group id to consume on. (default: %(default)s)')
    p.add_argument('--num-records', type=int, required=True,
                   help='REQUIRED. The number of records to consume.')
    p.add_argument('--fetch-size', type=int, default=DEFAULT_FETCH_SIZE,
                   help='Amount of data to fetch in a single request '
                        '(-> fetch.max.bytes). (default: %(default)s)')
    p.add_argument('--socket-buffer-size', type=int, default=DEFAULT_SOCKET_BUFFER,
                   help='TCP receive buffer size '
                        '(-> socket.receive.buffer.bytes). (default: %(default)s)')
    p.add_argument('--timeout', type=int, default=DEFAULT_TIMEOUT_MS,
                   help='Max allowed time in ms between returned records. '
                        '(default: %(default)s)')
    p.add_argument('--reporting-interval', type=int,
                   default=DEFAULT_REPORTING_INTERVAL_MS,
                   help='Interval in ms at which to print progress info. '
                        '(default: %(default)s)')
    p.add_argument('--command-config',
                   help='Config properties file (librdkafka properties).')
    p.add_argument('--command-property', action='append', default=[],
                   metavar='prop=val',
                   help='Client config property; takes precedence over '
                        '--command-config. May be repeated.')
    p.add_argument('--show-detailed-stats', action='store_true',
                   help='Report stats for each reporting interval.')
    return p.parse_args()


def build_conf(args):
    # Base config (Java defaults), then --command-config file, then
    # --command-property overrides (highest precedence) — matching Java.
    conf = {
        'group.id': args.group,
        'client.id': 'py-share-consumer-perf',
        'fetch.max.bytes': args.fetch_size,
        'socket.receive.buffer.bytes': args.socket_buffer_size,
        'bootstrap.servers': args.bootstrap_server,
    }
    if args.command_config:
        conf.update(read_properties(args.command_config))
    for kv in args.command_property:
        if '=' not in kv:
            sys.stderr.write('Ignoring malformed --command-property %r\n' % kv)
            continue
        k, v = kv.split('=', 1)
        conf[k.strip()] = v.strip()
    return conf


def fmt_ts(epoch_s):
    lt = time.localtime(epoch_s)
    return '%s:%03d' % (time.strftime('%Y-%m-%d %H:%M:%S', lt),
                        int((epoch_s % 1) * 1000))


def print_header():
    # Identical to Java ShareConsumerPerformance.printHeader().
    print('start.time, end.time, data.consumed.in.MB, MB.sec, nMsg.sec, '
          'data.consumed.in.nMsg, fetch.time.ms')


def print_progress(start_s, now_s, total_bytes, records_read,
                   last_bytes, last_records, fetch_time_ms):
    elapsed_ms = (now_s - start_s) * 1000.0
    total_mb = total_bytes / (1024.0 * 1024.0)
    interval_mb = (total_bytes - last_bytes) / (1024.0 * 1024.0)
    interval_mb_per_sec = 1000.0 * interval_mb / elapsed_ms if elapsed_ms else 0.0
    interval_rec_per_sec = \
        ((records_read - last_records) / elapsed_ms) * 1000.0 if elapsed_ms else 0.0
    print('%s, %s, %.4f, %.4f, %.4f, %d, %d'
          % (fmt_ts(start_s), fmt_ts(now_s), total_mb, interval_mb_per_sec,
             interval_rec_per_sec, records_read, fetch_time_ms))


def main():
    args = parse_args()
    topics = [t.strip() for t in args.topic.split(',') if t.strip()]
    conf = build_conf(args)

    sc = ShareConsumer(conf)
    sc.subscribe(topics)

    print_header()

    num_records = args.num_records
    timeout_s = args.timeout / 1000.0
    report_s = args.reporting_interval / 1000.0

    records_read = 0
    bytes_read = 0
    last_records = 0
    last_bytes = 0

    start = time.monotonic()
    start_wall = time.time()
    last_report = start
    last_record_time = start

    try:
        while records_read < num_records:
            messages = sc.poll(timeout=0.1)  # returns a list (possibly empty)
            now = time.monotonic()

            if messages:
                last_record_time = now
                for msg in messages:
                    if msg.error():
                        continue
                    records_read += 1
                    bytes_read += len(msg.key() or b'') + len(msg.value() or b'')
                    # Implicit ack: the next poll() acknowledges this record.
            elif (now - last_record_time) >= timeout_s:
                sys.stderr.write(
                    'WARNING: Exiting before consuming the expected number of '
                    'records: timeout (%d ms) exceeded. Consumed %d of %d.\n'
                    % (args.timeout, records_read, num_records))
                break

            if args.show_detailed_stats and (now - last_report) >= report_s:
                fetch_time_ms = int((now - start) * 1000.0)
                print_progress(start, now, bytes_read, records_read,
                               last_bytes, last_records, fetch_time_ms)
                last_report = now
                last_records = records_read
                last_bytes = bytes_read
    except KeyboardInterrupt:
        sys.stderr.write('%% Aborted by user\n')
    finally:
        end = time.monotonic()
        elapsed_s = end - start or 1e-9
        total_mb = bytes_read / (1024.0 * 1024.0)
        # Final summary line (start, end, totalMB, MB/s, nMsg/s, nMsg, fetch.time.ms).
        print('%s, %s, %.4f, %.4f, %.4f, %d, %d'
              % (fmt_ts(start_wall), fmt_ts(time.time()), total_mb,
                 total_mb / elapsed_s, records_read / elapsed_s,
                 records_read, int(elapsed_s * 1000.0)))
        sc.close()


if __name__ == '__main__':
    main()
