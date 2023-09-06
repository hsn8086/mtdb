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
@File       : io_operation.py

@Author     : hsn

@Date       : 9/3/23 8:37 PM
"""
import _collections_abc
import collections
import json
from abc import abstractmethod
from pathlib import Path
from typing import IO, List, Dict, Tuple, overload
from bisect import bisect_left
from collections.abc import MutableSequence


class ListFile(MutableSequence):
    @overload
    @abstractmethod
    def __delitem__(self, index: int) -> None:
        ...

    @overload
    @abstractmethod
    def __delitem__(self, index: slice) -> None:
        ...

    def __delitem__(self, index: int) -> None:
        if index >= self.len:
            raise IndexError
        chunk_size = 1024 * 1024
        start = index * self.item_len
        while True:
            self.f.seek(start + self.item_len)
            d = self.f.read(chunk_size)
            if not d:
                break
            self.f.seek(start)
            self.f.write(d)
            start += chunk_size
        self.f.truncate(self.f.tell() - self.item_len)
        self.len -= 1

    def __len__(self) -> int:
        return self.len

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


class IndexedData:
    def __init__(self, data_fold: Path, index: Path, primary_key: str, idx_item_len: int = 4):
        self.data_fold = data_fold
        self.index = ListFile(index.open('r+b'), item_len=idx_item_len)
        self.primary_key = primary_key

    def __getitem__(self, item: int | bytes):
        if isinstance(item, int):
            item = self.index[item]

        return json.load(open(self.data_fold / f'{int.from_bytes(item, "big", signed=False)}.json', 'r'))[
            self.primary_key]

    def bisect_left(self, left, right, value):
        return bisect_left(self.index, value, left, right, key=lambda y: self[y])

    def close(self):
        self.index.close()

    def inserts(self, values: List[Tuple[int, bytes]]):
        vs = sorted(values, key=lambda x: json.load(
            open(self.data_fold / f'{int.from_bytes(x[1], "big", signed=False)}.json', 'r'))[
            self.primary_key])
        self.index.f.seek(vs[0][0] * self.index.item_len)
        pre_writing_list = []

        for i, v in enumerate(vs, start=0):
            pos, value = v
            while True:
                rd = self.index.f.read(self.index.item_len)

                if rd:
                    pre_writing_list.append(rd)
                    if self.index.f.tell() >= self.index.item_len:
                        self.index.f.seek(-self.index.item_len, 1)

                if self.index.f.tell() >= (pos + i) * self.index.item_len or not pre_writing_list:
                    break

                self.index.f.write(pre_writing_list.pop(0))

            self.index.f.write(value)
