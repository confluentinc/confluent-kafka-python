#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
#
# Copyright 2019 Confluent Inc.
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
# limit
#

import threading
import time

from confluent_kafka import Producer

try:
    from queue import Queue, Empty
except ImportError:
    from Queue import Queue, Empty


class IntendedException (Exception):
    pass


def thread_run(myid, p, q):
    def do_crash(err, msg):
        raise IntendedException()

    for i in range(1, 3):
        cb = None
        if i == 2:
            cb = do_crash
        p.produce('mytopic', value='hi', callback=cb)
        t = time.time()
        try:
            p.flush()
            print(myid, 'Flush took %.3f' % (time.time() - t))
        except IntendedException:
            print(myid, "Intentional callback crash: ok")
            continue

    print(myid, 'Done')
    q.put(myid)


def test_thread_safety():
    """ Basic thread safety tests. """

    q = Queue()
    p = Producer({'socket.timeout.ms': 10,
                  'message.timeout.ms': 10})

    threads = list()
    for i in range(1, 5):
        thr = threading.Thread(target=thread_run, name=str(i), args=[i, p, q])
        thr.start()
        threads.append(thr)

    for thr in threads:
        thr.join()

    # Count the number of threads that exited cleanly
    cnt = 0
    try:
        for x in iter(q.get_nowait, None):
            cnt += 1
    except Empty:
        pass

    if cnt != len(threads):
        raise Exception('Only %d/%d threads succeeded' % (cnt, len(threads)))

    print('Done')


if __name__ == '__main__':
    test_thread_safety()
