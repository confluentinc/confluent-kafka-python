#!/usr/bin/env python
#
# Rate-paced producer for KIP-714 telemetry / perf testing.
# Sends fixed-size messages at a configurable rate.
#

import sys
import time

from confluent_kafka import Producer


def main():
    if len(sys.argv) < 3:
        sys.stderr.write(
            'Usage: %s <bootstrap-brokers> <topic> '
            '[rate_per_sec=1000] [size_bytes=100] [duration_sec=0]\n' % sys.argv[0]
        )
        sys.exit(1)

    broker = sys.argv[1]
    topic = sys.argv[2]
    rate = int(sys.argv[3]) if len(sys.argv) > 3 else 1000
    size = int(sys.argv[4]) if len(sys.argv) > 4 else 100
    duration = float(sys.argv[5]) if len(sys.argv) > 5 else 0  # 0 = forever

    conf = {
        'bootstrap.servers': broker,
        'client.id': 'py-producer-perf',
        'enable.metrics.push': True,
        'linger.ms': 5,
    }
    p = Producer(**conf)
    payload = b'x' * size
    state = {'sent': 0, 'errors': 0}

    def cb(err, _msg):
        if err is not None:
            state['errors'] += 1

    interval = 1.0 / rate
    start = time.monotonic()
    next_send = start
    last_report = start

    sys.stderr.write(
        'producing to %s: rate=%d msg/s size=%d bytes duration=%s\n'
        % (topic, rate, size, ('%.0fs' % duration) if duration else 'unlimited')
    )

    try:
        while True:
            now = time.monotonic()
            if duration and (now - start) >= duration:
                break
            if now >= next_send:
                try:
                    p.produce(topic, payload, callback=cb)
                    state['sent'] += 1
                    next_send += interval
                except BufferError:
                    p.poll(0.001)
                    continue
            else:
                p.poll(0)
                sleep_for = next_send - now
                if sleep_for > 0.0005:
                    time.sleep(sleep_for - 0.0005)
            if (now - last_report) >= 1.0:
                avg = state['sent'] / (now - start)
                sys.stderr.write(
                    'sent=%d  rate=%.0f msg/s  errors=%d  inflight=%d\n'
                    % (state['sent'], avg, state['errors'], len(p))
                )
                last_report = now
    except KeyboardInterrupt:
        sys.stderr.write('Aborted by user\n')
    finally:
        sys.stderr.write('flushing %d msgs...\n' % len(p))
        p.flush()
        elapsed = time.monotonic() - start
        avg = state['sent'] / elapsed if elapsed else 0
        sys.stderr.write(
            'done. total=%d  avg_rate=%.0f msg/s  errors=%d  elapsed=%.1fs\n'
            % (state['sent'], avg, state['errors'], elapsed)
        )


if __name__ == '__main__':
    main()
