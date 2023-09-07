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
import functools
import json
from abc import abstractmethod
from pathlib import Path
from typing import IO, List, Dict, Tuple, overload, Any
from bisect import bisect_left
from collections.abc import MutableSequence
from queue import LifoQueue
from collections import deque

from mtdb.utils import Cacher


class ListFile(MutableSequence):
    """
    A list-like object whose items are bytes stored in a file.
    """
    f: IO
    item_len: int
    _len: int

    def __init__(self, f: IO, item_len: int = 4):
        self.f = f
        self.item_len = item_len
        _len = f.seek(0, 2) / item_len
        assert _len == int(_len)  # check if the file is corrupted.
        self._len = int(_len)

    def __getitem__(self, item: int):
        """
        Return self[key].
        :param item:
        :return:
        """
        if item >= self._len:
            raise IndexError
        self.f.seek(item * self.item_len)
        return self.f.read(self.item_len)

    def __setitem__(self, index: int, value: bytes):
        """
        Set self[key] to value.
        :param index:
        :param value:
        :return:
        """
        if index >= self._len:
            raise IndexError
        if len(value) != self.item_len:
            raise ValueError

        self.f.seek(index * self.item_len)
        self.f.write(value)

    def append(self, value: bytes):
        """
        Append value to the end of the list.
        :param value:
        :return:
        """
        self.f.seek(0, 2)  # seek to the end of the file.
        self.f.write(value)
        self._len += 1

    def insert(self, index: int, value: bytes):
        """
        Insert value at index, shifting the subsequent values rightward.
        :param index:
        :param value:
        :return:
        """
        if index > self._len:
            raise IndexError
        if len(value) != self.item_len:
            raise ValueError

        self.f.seek(index * self.item_len)

        last_data = value
        while last_data:
            data = self.f.read(self.item_len)
            if self.f.tell() != 0 and data:
                self.f.seek(-self.item_len, 1)
            self.f.write(last_data)

            last_data = data
        self._len += 1

    def close(self):
        """
        Close the underlying file.
        :return:
        """
        self.f.close()

    def __del__(self):
        self.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    @overload
    @abstractmethod
    def __delitem__(self, index: int) -> None:
        ...

    @overload
    @abstractmethod
    def __delitem__(self, index: slice) -> None:
        ...

    def __delitem__(self, index: int) -> None:
        if index >= self._len:
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
        self._len -= 1

    def __len__(self) -> int:
        return self._len


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
        self.cacher = Cacher()

        def __read(_id):
            return json.load(open(self.data_fold / f'{_id}.json', 'r'))[
                self.primary_key]

        self._read = self.cacher.register(__read)

    def __getitem__(self, item: int | bytes):
        if isinstance(item, int):
            item = self.index[item]
        _id = int.from_bytes(item, "big", signed=False)

        return self._read(_id)

    def bisect_left(self, left, right, value):
        return bisect_left(self.index, value, left, right, key=lambda y: self[y])

    def close(self):
        self.index.close()

    def inserts(self, values: list[tuple[bytes, Any]]):
        """
        Insert values into the index.
        :param values:
        :return:
        """
        # bisect_left
        vs = sorted(values, key=lambda x: x[1])
        vs = list(map(lambda x: (self.bisect_left(0, len(self.index) - 1, x[1]), *x), vs))

        tell = self.index.f.seek(vs[0][0] * self.index.item_len)
        pre_writing_list = []

        for i, v in enumerate(vs, start=0):
            pos, value, _ = v
            while True:
                rd = self.index.f.read(self.index.item_len)
                tell += len(rd)

                if rd:
                    pre_writing_list.append(rd)
                    if tell >= self.index.item_len:
                        tell = self.index.f.seek(-self.index.item_len, 1)

                if tell >= (pos + i) * self.index.item_len or not pre_writing_list:
                    break

                tell += self.index.f.write(pre_writing_list.pop(0))

            tell += self.index.f.write(value)
