#!/usr/bin/env python
# -*- coding: UTF-8 -*-

#  Copyright (C) $originalComment.match("Copyright (\d+)", 1, "-", "$today.year")$today.year. Suto-Commune
#  This program is free software: you can redistribute it and/or modify
#  it under the terms of the GNU Affero General Public License as
#  published by the Free Software Foundation, either version 3 of the
#  License, or (at your option) any later version.
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU Affero General Public License for more details.
#  You should have received a copy of the GNU Affero General Public License
#  along with this program.  If not, see <https://www.gnu.org/licenses/>.

"""
@File       : db.py

@Author     : hsn

@Date       : 8/28/23 8:43 PM
"""
import json
import math
from pathlib import Path
from typing import IO
from urllib.parse import urlparse
from zipfile import ZipFile


def db(uri: str):
    uri_ = urlparse(uri)
    if uri_.scheme == 'file':
        return FileDB(uri)


class EmpID:
    def __init__(self, f: IO):
        self.f = f
        if f.read(1):
            f.seek(0)
            self.l = json.load(f)
        else:
            self.l = [0]

    def save(self):
        self.f.seek(0)
        json.dump(self.l, self.f)
        self.f.truncate()

    def close(self):
        self.save()
        self.f.close()

    def __next__(self):
        rt = self.l.pop(0)
        if len(self.l) == 0:
            self.l.append(rt + 1)
        return rt

    def add(self, _id):
        self.l.append(_id)
        self.save()


class ListFile:
    def __init__(self, f: IO, item_len: int = 4):
        self.f = f
        self.item_len = item_len
        self.len = f.seek(0, 2) / item_len
        assert self.len == int(self.len)
        self.len = int(self.len)

    def __getitem__(self, item: int):
        if item >= self.len:
            raise IndexError
        self.f.seek(item * self.item_len)
        return self.f.read(self.item_len)

    def __setitem__(self, key: int, value: bytes):
        if key >= self.len:
            raise IndexError
        if len(value) != self.item_len:
            raise ValueError
        self.f.seek(key * self.item_len)
        self.f.write(value)

    def append(self, value: bytes):
        self.f.seek(0, 2)
        self.f.write(value)
        self.len += 1

    def insert(self, key: int, value: bytes):
        if key > self.len:
            raise IndexError
        if len(value) != self.item_len:
            raise ValueError
        self.f.seek(key * self.item_len)
        od = value
        while od:
            d = self.f.read(self.item_len)
            if self.f.tell() != 0 and d:
                self.f.seek(-self.item_len, 1)
            self.f.write(od)

            od = d
        self.len += 1

    def close(self):
        self.f.close()

    def __del__(self):
        self.close()


class IndexedData:
    def __init__(self, data_fold: Path, index: Path, primary_key: str, idx_item_len: int = 4):
        self.data_fold = data_fold
        self.index = ListFile(index.open('r+b'), item_len=idx_item_len)
        self.primary_key = primary_key

    def __getitem__(self, item):
        return json.load(open(self.data_fold / f'{int.from_bytes(self.index[item], "big", signed=False)}.json', 'r'))[
            self.primary_key]

    def binary_search(self, l, r, x):
        if r == 0 and l == 0:
            if x > self[0]:
                return 1
            return 0
        if r >= l:
            mid = int(l + (r - l) / 2)
            # print(mid, self.index.len, l, r)
            if self[mid] == x:
                return mid
            elif self[mid] > x:
                return self.binary_search(l, mid - 1, x)
            else:
                return self.binary_search(mid + 1, r, x)
        else:
            if r < 0:
                return 0
            if l == r:
                return r
            else:
                return l

    def close(self):
        self.index.close()


class FileDB:
    def __init__(self, uri: str):
        self.uri = urlparse(uri)
        self.path = Path(self.uri.path)
        if not self.path.exists():
            self.path.mkdir(parents=True)
        eid_path = (self.path / 'emp_ids.json')
        if not eid_path.exists():
            eid_path.touch()
        self.emp_ids = EmpID(eid_path.open('r+'))

    def insert(self, document: dict):
        _id = next(self.emp_ids)
        for k, v in document.items():
            if type(v) in [int]:
                p = (self.path / 'index' / f'{k}-{type(v).__name__}.idx')
                if not p.parent.exists():
                    p.parent.mkdir(parents=True)
                if not p.exists():
                    p.touch()

                lf = IndexedData(self.path / 'data', p, k, idx_item_len=6)
                idx_insert_pos = lf.binary_search(0, lf.index.len - 1, v)
                # if idx_insert_pos == lf.index.len:
                #     try:
                #         print(lf[idx_insert_pos - 1], v)
                #     except:
                #         pass
                lf.index.insert(idx_insert_pos, _id.to_bytes(6, 'big', signed=False))
                lf.close()
        if not (self.path / 'data').exists():
            (self.path / 'data').mkdir()
        with (self.path / 'data' / f'{_id}.json').open('w') as f:
            json.dump(document, f)

    def close(self):
        self.emp_ids.close()
