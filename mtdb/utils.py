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
@File       : utils.py

@Author     : hsn

@Date       : 9/3/23 8:38 PM
"""
import time
from pathlib import Path
import signal


def inf_gen():
    while True:
        yield 1


class FileLock:
    def __init__(self, path: Path):
        self.path = path
        self.lock_path = path.with_suffix('.lock')

    def acquire(self, timeout: int = 0):
        timer = range(timeout * 20) if timeout else inf_gen()
        for _ in timer:
            if not self.lock_path.exists():
                break
            time.sleep(0.05)
        else:
            return False
        try:
            self.lock_path.touch()
            return True
        except OSError:
            return False

    def release(self):
        try:
            self.lock_path.unlink()
            return True
        except OSError:
            return False


class Cacher:
    def __init__(self):
        self.map = {}

    def register(self, func):
        def warp(*args):

            if args in self.map:
                return self.map[args]
            else:
                rst = func(*args)
                self.map[args] = rst
                return rst

        return warp

    def clear(self):
        self.map.clear()

    def remove(self, *args):
        if args in self.map:
            self.map.pop(args)
