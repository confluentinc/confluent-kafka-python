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

"""KIP-932 share consumer -- implicit acknowledgement (hands-off).

Implicit mode does the bookkeeping for you: you never call acknowledge(), and
you don't even need to commit -- each poll() auto-ACCEPTs everything from the
previous poll, so records are marked done just by continuing to poll. The
ack-commit callback fires as those implicit accepts land at the broker. Pair
with share_consumer_ack_producer.py.

    python examples/share_consumer_implicit.py localhost:9092 share-demo
"""

import argparse
import sys
import uuid

from confluent_kafka import ShareConsumer
from confluent_kafka.admin import (
    AdminClient,
    AlterConfigOpType,
    ConfigEntry,
    ConfigResource,
    ResourceType,
)

# Tiny ANSI colour helper -- auto-off when stdout isn't a terminal so piped logs
# don't fill with escape codes. No dependency; the example just runs.
DIM, CYAN, GREEN, RED = '2', '36', '32', '31'
_TTY = sys.stdout.isatty()


def paint(code, text):
    return f"\033[{code}m{text}\033[0m" if _TTY else text


def set_group_offset_reset(bootstrap, group, reset):
    # share.auto.offset.reset is a broker-side *group* config (default 'latest').
    # Set it before joining so a fresh group reads from the start.
    admin = AdminClient({'bootstrap.servers': bootstrap})
    res = ConfigResource(
        ResourceType.GROUP, group,
        incremental_configs=[
            ConfigEntry('share.auto.offset.reset', reset, incremental_operation=AlterConfigOpType.SET),
        ],
    )
    for fut in admin.incremental_alter_configs([res]).values():
        fut.result()


def fmt_offsets(offsets):
    # Collapse offsets into compact ranges: "0-21" beats 22 separate numbers.
    offs = sorted(offsets)
    if not offs:
        return ""
    parts, start, prev = [], offs[0], offs[0]
    for o in offs[1:]:
        if o == prev + 1:
            prev = o
            continue
        parts.append(str(start) if start == prev else f"{start}-{prev}")
        start = prev = o
    parts.append(str(start) if start == prev else f"{start}-{prev}")
    return ", ".join(parts)


def on_ack_commit(offsets, exc):
    # In implicit mode the accepts are triggered by poll() (not an explicit
    # commit), but they still report through this callback once the broker
    # confirms them.
    if exc is not None:
        print(paint(RED, f"[ack-cb] commit FAILED: {exc}"))
        return
    for tp in sorted(offsets, key=lambda t: (t.topic, t.partition)):
        offs = offsets[tp]
        print(paint(GREEN, f"[ack-cb] partition={tp.partition} offsets={fmt_offsets(offs)} ({len(offs)} msg)"))


def main():
    ap = argparse.ArgumentParser(description='KIP-932 share consumer: implicit acknowledgement')
    ap.add_argument('bootstrap', help='bootstrap servers, e.g. localhost:9092')
    ap.add_argument('topic', help='topic to consume')
    ap.add_argument('--group', default=None, help='share group id (default: random)')
    ap.add_argument('--reset', choices=['earliest', 'latest'], default='latest')
    args = ap.parse_args()

    group = args.group or f'share-demo-{uuid.uuid4().hex[:8]}'
    set_group_offset_reset(args.bootstrap, group, args.reset)
    print(paint(DIM, f"[setup] group={group} mode=implicit reset={args.reset} topic={args.topic}"))

    sc = ShareConsumer({
        'bootstrap.servers': args.bootstrap,
        'group.id': group,
        'share.acknowledgement.mode': 'implicit',
    })
    sc.set_acknowledgement_commit_callback(on_ack_commit)
    sc.subscribe([args.topic])

    try:
        while True:
            # No acknowledge(), no commit -- the next poll() implicitly ACCEPTs
            # whatever this one returned. Just keep polling.
            for msg in sc.poll(timeout=1.0):
                if msg.error():
                    print(paint(RED, f"[poll]  error: {msg.error()}"))
                    continue
                val = msg.value().decode() if msg.value() else None
                print(paint(CYAN, f"[poll]   partition={msg.partition()} offset={msg.offset()}  msg={val}"))
    except KeyboardInterrupt:
        print(paint(DIM, '\n[consumer] stopping'))
    finally:
        sc.close()


if __name__ == '__main__':
    main()
