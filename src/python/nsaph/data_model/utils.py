#  Copyright (c) 2021. Harvard University
#
#  Developed by Research Software Engineering,
#  Faculty of Arts and Sciences, Research Computing (FAS RC)
#  Author: Michael A Bouzinier
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#

import json
import os
import re
from typing import Any, List

from nsaph_utils.utils.pyfst import FSTReader
from nsaph_utils.utils.io_utils import fopen
import csv


def split(node) -> (str, dict):
    """
    Given a node in an array produced from JSON/YAML structure
    returns a name and definition associated with the node

    :param node: an array node, usually from YAML or JSON file
    :return: a tuple consisting of a name and an array containing
        definitions
    """
    if isinstance(node, str):
        return node, {}
    if not isinstance(node, dict):
        raise Exception("Unsupported type for column spec: " + str(node))
    name = None
    for entry in node:
        name = entry
        break
    node = node[name]
    if isinstance(node, str):
        node = {"type": node}
    if not isinstance(node, dict):
        raise Exception("Unsupported spec type for node: " + name)
    return name, node


def basename(table):
    """
    Given fully qualified table name (schema.table)
    returns just a basename of a table

    :param table: a fully qualified table name
    :return: just the base name of the table, a piece of name after teh last dot
    """
    return table.split('.')[-1]


def entry_to_path(entry: Any) -> str:
    if isinstance(entry, tuple):
        path, _ = entry
        return path if isinstance(path, str) else path.name
    return str(entry)


class CSVLikeJsonReader:
    def __init__(self, path:str, columns: List, returns_mapping = False):
        self.path = path
        self.stream = None
        self.current = None
        self.columns = columns
        self.returns_mapping = returns_mapping

    def __next__(self):
        line = next(self.stream)
        data: dict = json.loads(line)
        if self.returns_mapping:
            row = data
        else:
            row = [data.get(column, None) for column in self.columns]
        return row

    def open(self):
        self.stream = fopen(self.path, "rt")
        return

    def close(self):
        self.stream.close()
        return

    def __enter__(self):
        self.open()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def __iter__(self):
        return self


class DataReader:
    """
    Generalized reader for columns-structured files, such as CSV and FST
    """

    def __init__(self, path: str,
                 buffer_size = None,
                 quoting=None,
                 has_header = None,
                 columns = None):
        self.path = path
        self.reader = None
        self.columns = columns
        self.to_close = None
        self.buffer_size = buffer_size
        if quoting:
            self.quoting = quoting
        else:
            self.quoting = csv.QUOTE_NONNUMERIC
        if has_header is not None:
            self.has_header = has_header
        else:
            self.has_header = True

    def open_fst(self, name = None):
        if name is None:
            name = self.path
        bs = self.buffer_size if self.buffer_size else 100000
        self.reader = FSTReader(name, bs)
        self.reader.open()
        self.to_close = self.reader
        self.columns = list(self.reader.columns.keys())

    def open_csv(self, path, f= lambda s: fopen(s, "rt")):
        self.to_close = f(path)
        self.reader = csv.reader(self.to_close, quoting=self.quoting)
        # self.reader = csv.reader((line.replace('\0',' ') for line in self.to_close), quoting=self.quoting)
        if self.has_header:
            header = next(self.reader)
            self.columns = header
        return

    def open_json(self, path):
        self.reader = CSVLikeJsonReader(path, self.columns)
        self.reader.open()
        self.to_close = self.reader
        return

    def get_path(self) -> str:
        return entry_to_path(self.path)

    def __enter__(self):
        opened = False
        name = self.path
        if isinstance(self.path, tuple):
            path, f = self.path
            name = path if isinstance(path, str) else path.name
            if name.lower().endswith(".fst"):
                if not os.path.isfile(name):
                    raise Exception(
                        "Not implemented: reading fst files from archive"
                    )
            elif ".json" in path.lower():
                self.open_json(path)
                opened = True
            else:
                self.open_csv(path, f)
                opened = True
        if not opened:
            if name.lower().endswith(".fst"):
                self.open_fst(name)
            elif ".csv" in name.lower():
                self.open_csv(name)
            elif ".json" in name.lower():
                self.open_json(name)
            else:
                raise Exception("Unsupported file format: " + self.path)
        return self

    def rows(self):
        return self.reader

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.to_close.close()


def regex(pattern: str):
    pattern = 'A' + pattern.replace('.', '_') + 'Z'
    x = pattern.split('*')
    y = [re.escape(s) for s in x]
    regexp = ".*".join(y)[1:-1]
    return re.compile(regexp)