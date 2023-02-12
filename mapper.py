import time
import sys
from utils import get_words, reporter, base_mapper, base_reducer, report_progress, callback_message
from multiprocessing.pool import ThreadPool
from collections import defaultdict
from reducer import MyReducer


def thread_map_error_callback(error):
    print(error)
    time.sleep(3)
    val = str(error).split()
    element = val[0].split("-")
    print(element[0] + " file task has been reset")
    run_mapper(base_mapper, base_reducer, element[0], element[1])
    print(element[0] + " running again")


def run_mapper(mapper, combiner, path, identifier):
    data = get_words(path)
    new_path = identifier + "_mapped_batch/" + path
    print("Running Mapper " + identifier + ": " + path)
    ret = []
    for datum in data:
        ret.append(mapper(datum))
    distributor = defaultdict(list)
    for key, value in ret:
        distributor[key].append(value)
    add = []
    for r in distributor.items():
        add.append(combiner(r))
    with open(new_path, 'w', encoding='utf-8') as f:
        f.write('\n'.join(f'{tup[0]} {tup[1]}' for tup in add))
    return add


def async_map(mapper, combiner, path, identifier):
    async_returns = run_mapper(mapper, combiner, path, identifier)
    return async_returns


def map_each(path, mapper, combiner, identifier):
    if int(sys.argv[3]) == 1 and path == "final_1.txt":
        print("\n")
        raise Exception(path + "-" + identifier + ' Error Processing File')
    map_returns = async_map(mapper, combiner, path, identifier)
    return map_returns


class MyMapper:

    def __init__(self, files, identifier, n_pools):
        intervals = [(48, 57), (97, 104), (105, 113), (114, 122)]
        self.ready = False
        pool_map = ThreadPool(n_pools)
        a = []
        for path in files:
            a.append(pool_map.apply_async(
                map_each, (path, base_mapper, base_reducer, identifier),
                callback=callback_message(path + " file has been queued"),
                error_callback=thread_map_error_callback
            ))
        report_progress(a, 'map ' + identifier, reporter)
        pool_map.close()
        pool_map.join()

        callback_message('Mapping Task ' + identifier + ' Completed')

        callback_message('Reducing Task ' + identifier + ' Starting')

        a_list = intervals[:2]

        b_list = intervals[2:]
        list_intervals = [[a_list, "p"], [b_list, "q"]]

        pool_reducer = ThreadPool(2)
        for x_list in list_intervals:
            pool_reducer.apply_async(MyReducer, (x_list[0], x_list[1], identifier, 2))
        pool_reducer.close()
        pool_reducer.join()

        self.ready = True

    def state(self):
        self.ready = True
