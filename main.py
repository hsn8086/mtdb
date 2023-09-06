import json
import random
from pathlib import Path
import cProfile
from mtdb.db import db
from zipfile import ZipFile


def main():
    # print(db(Path('test.db').resolve().as_uri()))
    d = db(Path('test').resolve().as_uri())
    for i in range(5000):
        d.insert({'name': 'hsn', 'age': random.randint(1, 10000)})
    d.insert_sche()

    # print('----------')
    # with open('test/index/age-int.idx', 'rb') as f:
    #     g = True
    #     while g:
    #         g = f.read(6)
    #         v = json.load(open(Path('test') / 'data' / f'{int.from_bytes(g, "big", signed=False)}.json', 'r'))[
    #             'age']
    #
    #         print(v)
    # with open('test/index/age-int.idx', 'rb') as f:
    #     while d:
    #         d = f.read(6)
    #         v = json.load(open(Path('test') / 'data' / f'{int.from_bytes(d, "big", signed=False)}.json', 'r'))[
    #             'age']
    #         print(v)

    d.close()
    print('--------')
    with open('test/index/age-int.idx', 'rb') as f:
        ov = 0
        while d:
            d = f.read(6)
            v = json.load(open(Path('test') / 'data' / f'{int.from_bytes(d, "big", signed=False)}.json', 'r'))[
                'age']
            if d:
                print(v)
                if v >= ov:
                    ov = v
                else:
                    raise ValueError


if __name__ == '__main__':
    cProfile.run('main()')
