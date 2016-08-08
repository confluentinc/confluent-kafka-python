#!/usr/bin/env python

from confluent_kafka import Producer, KafkaError, KafkaException
import threading, time, queue


class IntendedException (Exception):
    pass

def thread_run (myid,p,q):
    def do_crash (err, msg):
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
        except:
            raise

    print(myid, 'Done')
    q.put(myid)

    

def test_thread_safety():
    """ Basic thread safety tests. """

    q = queue.Queue()
    p = Producer({'socket.timeout.ms':10,
                  'socket.blocking.max.ms': 10,
                  'default.topic.config': {'message.timeout.ms': 10}})

    threads = list()
    for i in range(1, 5):
        thr = threading.Thread(target=thread_run, name=str(i), args=[i,p,q])
        thr.start()
        threads.append(thr)

    for thr in threads:
        thr.join()


    # Count the number of threads that exited cleanly
    cnt = 0
    try:
        for x in iter(q.get_nowait, None):
            cnt += 1
    except queue.Empty:
        pass

    if cnt != len(threads):
        raise Exception('Only %d/%d threads succeeded' % (cnt, len(threads)))

    print('Done')

    
if __name__=='__main__':
    test_thread_safety()
