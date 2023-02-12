import sys
import time

from utils import reporter, base_reducer, report_progress, callback_message, drop_and_merge, list_dir
from multiprocessing.pool import ThreadPool
from collections import defaultdict


def thread_reduce_error_callback(error):
    print(error)
    time.sleep(3)
    element = str(error).split("-")
    print(element[0] + "-" + element[1] + " interval task has been reset")
    interval = (ord(element[0]), ord(element[1]))
    run_reducer(base_reducer, interval, element[3], element[4])
    print(element[0] + "-" + element[1] + " interval task running again")


def run_reducer(reducer, interval, r_id, m_id):
    files = list_dir(m_id + "_mapped_batch/", r'\d+')
    i_interval = interval[0]
    f_interval = interval[1]
    print("Running Reducer " + r_id + " from " + m_id)
    data = []
    for file in files:
        path = m_id + "_mapped_batch/" + file
        f = open(path, "r")
        content_list = [x.strip() for x in f.readlines()]
        for element in content_list:
            for i in range(i_interval, f_interval):
                if element.startswith(chr(i)):
                    e = element.split()
                    p = (e[0], int(e[1]))
                    data.append(p)
        f.close()
    distributor = defaultdict(list)
    for key, value in data:
        distributor[key].append(value)
    add = []
    for r in distributor.items():
        p = reducer(r)
        add.append(p)

    new_path = m_id + "_processed_batch/" + chr(i_interval) + "-" + chr(f_interval) + ".txt"
    with open(new_path, 'w', encoding='utf-8') as f:
        f.write('\n'.join(f'{tup[0]} {tup[1]}' for tup in add))
    return add


def async_reducer(reducer, interval, r_id, m_id):
    async_returns = run_reducer(reducer, interval, r_id, m_id),
    return async_returns


def reduce_each(reducer, interval, r_id, m_id):
    if int(sys.argv[4]) == 1 and interval[0] == 48:
        raise Exception(chr(interval[0]) + "-" + chr(interval[1])
                        + '-Error Processing Interval in:-' + r_id + '-' + m_id)
    map_returns = async_reducer(reducer, interval, r_id, m_id)
    return map_returns


class MyReducer:

    def __init__(self, intervals, r_id, m_id, n_pools):
        pool_reduce = ThreadPool(n_pools)
        b = []
        for interval in intervals:
            b.append(pool_reduce.apply_async(
                reduce_each, (base_reducer, interval, r_id, m_id),
                callback=callback_message(chr(interval[0]) + "-" + chr(interval[1]) + " interval has been queued"),
                error_callback=thread_reduce_error_callback
            ))
        report_progress(b, 'reduce from ' + m_id, reporter)
        pool_reduce.close()
        pool_reduce.join()

        drop_and_merge(m_id + '_processed_batch/', m_id)
