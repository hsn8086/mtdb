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
import time
from pathlib import Path
from typing import Dict, Any
from urllib.parse import urlparse

from mtdb.io_operation import EmpID, IndexedData
from mtdb.utils import FileLock


def db(uri: str):
    uri_ = urlparse(uri)
    if uri_.scheme == 'file':
        return FileDB(uri)


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
        self.pre_insert_idx: Dict[str, Dict] = {}

        self.idxed_data_map = {}

    def get_idxed_data(self, key: str, value_type: type | str):
        if isinstance(value_type, type):
            value_type = value_type.__name__
        if f'{key}-{value_type}' not in self.idxed_data_map:
            self.idxed_data_map[f'{key}-{value_type}'] = IndexedData(self.path / 'data',
                                                                     self.path / 'index' / f'{key}-{value_type}.idx',
                                                                     key, idx_item_len=6)
        return self.idxed_data_map[f'{key}-{value_type}']

    def insert(self, document: dict):
        _id = next(self.emp_ids)
        for key_of_doc, val_of_doc in document.items():
            if type(val_of_doc) in [int]:
                p = (self.path / 'index' / f'{key_of_doc}-{type(val_of_doc).__name__}.idx')
                if not p.parent.exists():
                    p.parent.mkdir(parents=True)
                if not p.exists():
                    p.touch()
                self.insert_idx(key_of_doc, val_of_doc, _id)
        if not (self.path / 'data').exists():
            (self.path / 'data').mkdir()
        with (self.path / 'data' / f'{_id}.json').open('w') as f:
            json.dump(document, f)

    def insert_idx(self, key: str, value: Any, _id: int):
        value_type = type(value)
        if f'{key}-{value_type.__name__}' not in self.pre_insert_idx:
            self.pre_insert_idx[f'{key}-{value_type.__name__}'] = {}
        self.pre_insert_idx[f'{key}-{value_type.__name__}'][_id] = {'key': key, 'value_type': value_type,
                                                                    'value': value}

    def insert_sche(self):
        for path, indexes in self.pre_insert_idx.items():
            full_path = self.path / 'index' / f'{path}.idx'
            lock = FileLock(full_path)
            if not lock.acquire(5):
                return

            data = self.get_idxed_data(*path.split('-'))
            # print(indexes)
            data.inserts([(int.to_bytes(_id, 6, 'big', signed=False), d['value']) for _id, d in indexes.items()])
            data.close()
            lock.release()

    def close(self):
        self.emp_ids.close()
