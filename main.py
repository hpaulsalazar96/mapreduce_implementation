import os
import time
import sys
from collections import defaultdict

from tabulate import tabulate

from mapper import MyMapper
from utils import base_reducer, list_dir, callback_message
from multiprocessing.pool import ThreadPool
from filesplit.split import Split


def merge_by_key(b_path):
    merged_files = list_dir(b_path, "n")
    data = []
    while len(merged_files) < 2:
        time.sleep(0.3)

    for file in merged_files:
        path = b_path + file
        f = open(path, "r")
        content_list = [x.strip() for x in f.readlines()]
        for element in content_list:
            e = element.split()
            p = (e[0], int(e[1]))
            data.append(p)
        f.close()

    distributor = defaultdict(list)
    for key, value in data:
        distributor[key].append(value)
    add = []
    for r in distributor.items():
        p = base_reducer(r)
        add.append(p)

    add.sort(key=lambda a: a[1], reverse=True)
    with open('final_map_reduce.txt', 'a', encoding='utf-8') as f:
        f.write(tabulate(add, headers=["Words", "Frequency"]))
    f.close()


def split_big_file():
    BYTES_PER_FILE = int(sys.argv[1])  # 20 900 000 b max 20mb
    filename = 'final.txt'  # large file
    out_dir = 'base_batch/'
    Split(filename, out_dir).bysize(BYTES_PER_FILE, True)


def prepare_files():
    if not os.path.exists("base_batch"):
        os.makedirs("base_batch")
    if not os.path.exists("mapped_batch"):
        os.makedirs("a_mapped_batch")
    if not os.path.exists("mapped_batch"):
        os.makedirs("b_mapped_batch")
    if not os.path.exists("a_processed_batch"):
        os.makedirs("a_processed_batch")
    if not os.path.exists("b_processed_batch"):
        os.makedirs("b_processed_batch")
    if not os.path.exists("merged_by_mapper"):
        os.makedirs("merged_by_mapper")
    split_big_file()
    ex = os.path.exists('.base_batch/manifest')
    while ex:
        time.sleep(0.5)
        ex = os.path.exists('.base_batch/manifest')
    os.remove('base_batch/manifest')


def coordinator(callback_m):

    prepare_files()
    files = list_dir('base_batch/', r'\d+')
    if int(sys.argv[2]) == 1:
        raise OSError
    # 52 files processed with distribution of two mappers
    a_list = files[0:26]
    b_list = files[26:]
    list_files = [[a_list, "a"], [b_list, "b"]]
    m_pool = ThreadPool(2)
    for x_list in list_files:
        m_pool.apply_async(MyMapper, (x_list[0], x_list[1], 4))
    m_pool.close()
    m_pool.join()

    callback_m('Mapping Task Completed Successfully')
    callback_m('Reduce Task Completed Successfully')

    merge_by_key('merged_by_mapper/')

    callback_m('Main Task Completed Successfully')


if __name__ == '__main__':
    start_time = time.time()
    try:
        coordinator(callback_message)
    except OSError:
        raise Exception('Coordinator Session Ended Unexpectedly')
    except KeyboardInterrupt:
        raise Exception('Coordinator Session Aborted Manually')
    print("--- %s seconds ---" % (time.time() - start_time))
