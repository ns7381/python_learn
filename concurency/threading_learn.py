# coding: utf-8
import threading
# print(threading.active_count())
# print(threading.enumerate())
import time

from queue import Queue


def thread_job():
    print("T1 start\n")
    for i in range(10):
        time.sleep(0.1)  # 任务间隔0.1s
    print("T1 finish\n")


def thread_no_join():
    added_thread = threading.Thread(target=thread_job, name='T1')
    added_thread.start()
    added_thread.join()
    print("all done\n")


def job(l, q):
    for i in range(len(l)):
        l[i] = l[i] ** 2
    q.put(l)


def multi_threading():
    q = Queue()
    threads = []
    data = [[1, 2, 3], [3, 4, 5], [4, 4, 4], [5, 5, 5]]
    for i in range(4):
        t = threading.Thread(target=job, args=(data[i], q))
        t.start()
        threads.append(t)
    for thread in threads:
        thread.join()
    results = []
    for _ in range(4):
        results.append(q.get())
    print(results)


def job1():
    global A, lock
    lock.acquire()
    for i in range(10):
        A += 1
        print('job1', A)
    lock.release()


def job2():
    global A, lock
    lock.acquire()
    for i in range(10):
        A += 10
        print('job2', A)
    lock.release()


if __name__ == '__main__':
    lock = threading.Lock()
    A = 0
    t1 = threading.Thread(target=job1)
    t2 = threading.Thread(target=job2)
    t1.start()
    t2.start()
    t1.join()
    t2.join()
