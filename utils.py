import os
import re
import time

final = "final.txt"
original = "original.txt"


def get_words(doc):
    words = [word
             for word in map(lambda x: x.strip().rstrip(),
                             ' '.join(open('base_batch/' + doc, 'rt', encoding='utf-8').readlines()).split(' '))
             if word != '']
    return words


def append():
    f1 = open(final, 'a+')
    f2 = open(original, 'r')

    # appending the contents of the second file to the first file
    f1.write(f2.read())

    f1.seek(0)
    f2.seek(0)

    f1.close()
    f2.close()


def reporter(tag, done, total):
    print(f'Processing Operation {tag}: {done}/{total}')


def base_mapper(word):
    return word, 1


def base_reducer(val):
    return val[0], sum(val[1])


def report_progress(map_returns, tag, callback):
    done = 0
    num_jobs = len(map_returns)
    while num_jobs > done:
        done = 0
        for ret in map_returns:
            if ret.ready():
                done += 1
        time.sleep(0.5)
        if callback:
            callback(tag, done, num_jobs)


def list_dir(b_path, reg):
    dir_path = b_path
    res = []
    for path in os.listdir(dir_path):
        if os.path.isfile(os.path.join(dir_path, path)):
            res.append(path)
    if reg == "n":
        return res
    else:
        p = re.compile(reg)
        return sorted(res, key=lambda s: int(p.search(s).group()))


def drop_and_merge(b_path, m_id):
    files = list_dir(b_path, "n")
    data = []
    for file in files:
        path = b_path + file
        f = open(path, "r")
        content_list = [x.strip() for x in f.readlines()]
        for element in content_list:
            e = element.split()
            p = (e[0], int(e[1]))
            data.append(p)
        f.close()
    data.sort(key=lambda a: a[1], reverse=True)
    fold = 'merged_by_mapper/' + m_id + "_merged_files.txt"
    with open(fold, 'w', encoding='utf-8') as f:
        f.write(''.join(f'{tup[0]} {tup[1]}\n' for tup in data))
    f.close()

    callback_message('Merge Files by Mapper Completed')


def callback_message(message):
    print(message + '\n')


if __name__ == '__main__':
    for i in range(145):
        append()
