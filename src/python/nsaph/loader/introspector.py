"""
This module introspects columnar data to infer the types of
the columns
"""
import csv
import json
import logging
import os
import re
import sys
import tarfile
from collections import OrderedDict
from typing import Dict, Callable, List

import yaml
from nsaph_utils.utils.io_utils import fopen, SpecialValues, is_dir, get_entries
from nsaph.pg_keywords import *

PG_MAXINT = 2147483647


integer = re.compile("-?\d+")
float_number = re.compile("(-?\d*)\.(\d+)([e|E][-|+]?\d+)?")
exponent = re.compile("(-?\d+)([e|E][-|+]?\d+)")
date = re.compile("([12]\d{3}-(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01]))")


class Introspector:
    @staticmethod
    def load_range(n, f) -> int:
        for i in range(0, n):
            try:
                f()
            except StopIteration:
                return i
        return n

    @staticmethod
    def name(path) -> str:
        if isinstance(path, tarfile.TarInfo):
            full_name =  path.name
        else:
            full_name = str(path)
        name, _ = os.path.splitext(os.path.basename(full_name))
        return name

    @staticmethod
    def csv_reader(data, unquote = True):
        if unquote:
            q = csv.QUOTE_ALL
        else:
            q = csv.QUOTE_NONE
        return csv.reader(data, quotechar='"', delimiter=',',
            quoting=q, skipinitialspace=True)

    @staticmethod
    def unquote(s: str) -> str:
        return s.strip().strip('"')

    def __init__(self,
                 data_file: str,
                 column_name_replacement: Dict = None):
        self.entries = None
        self.lines_to_load = 10000
        if is_dir(data_file):
            self.entries, self.open_entry_function = get_entries(data_file)
        else:
            self.open_entry_function = lambda x: fopen(x, "rt")
        self.file_path = data_file
        self.has_commas = False
        self.quoted_values = False
        self.sql_columns = []
        self.csv_columns = []
        self.types = []
        self.column_map = column_name_replacement \
            if column_name_replacement else dict()

    def fopen(self, source):
        entry = self.open_entry_function(source)
        return entry

    def handle_csv(self, entry):
        rows, lines = self.load_csv(entry)
        for row in rows:
            for cell in row:
                if ',' in cell:
                    self.has_commas = True
                    break
        if not rows:
            raise Exception("No data in {}".format(self.file_path))
        self.guess_types(rows, lines)

    def handle_json(self, entry):
        rows = self.load_json(entry)
        if not rows:
            raise Exception("No data in {}".format(self.file_path))
        m = len(rows)
        n = len(self.csv_columns)
        for c in range(0, n):
            c_type = None
            max_val = 0
            for l in range(0, m):
                v = rows[l][c]
                if v is None:
                    continue
                if isinstance(v, int):
                    t = PG_INT_TYPE
                    max_val = max(max_val, v)
                elif isinstance(v, float):
                    t = PG_NUMERIC_TYPE
                elif isinstance(v, str):
                    if date.fullmatch(v):
                        t = "DATE"
                    else:
                        t = PG_STR_TYPE
                else:
                    raise ValueError(v)
                try:
                    c_type = self.reconcile(t, c_type)
                except InconsistentTypes:
                    msg = "Inconsistent type for column {:d} [{:s}]. " \
                        .format(c + 1, self.csv_columns[c])
                    msg += "Up to line {:d}: {:s}, for line={:d}: {:s}. " \
                        .format(l - 1, c_type, l, t)
                    msg += "Value = {}".format(v)
                    raise Exception(msg)
            self.types.append(self.db_type(c_type, max_val, None, None))
        return

    def introspect(self, entry=None):
        if not entry:
            if self.entries is not None:
                entry = self.entries[0]
            else:
                entry = self.file_path
        logging.info("Using for data analysis: " + self.name(entry))
        if ".json" in self.name(entry).lower():
            self.handle_json(entry)
        else:
            self.handle_csv(entry)
        self.sql_columns = []
        for c in self.csv_columns:
            if not c:
                self.sql_columns.append("Col")
            elif c.lower() in self.column_map:
                self.sql_columns.append(self.column_map[c.lower()])
            else:
                if c[0].isdigit():
                    c = "c_" + c
                self.sql_columns.append(
                    c.replace('.', '_')
                    .replace(' ', '_')
                    .lower()
                )
        return

    def load_csv(self, entry) -> (List[List[str]], List[List[str]]):
        with self.fopen(entry) as data:
            reader = self.csv_reader(data, True)
            row = next(reader)
            self.csv_columns = [self.unquote(c) for c in row]

            rows = []
            self.load_range(self.lines_to_load, lambda : rows.append(next(reader)))
        with self.fopen(entry) as data:
            reader = self.csv_reader(data, False)
            next(reader)
            lines = []
            self.load_range(self.lines_to_load, lambda : lines.append(next(reader)))
        return rows, lines

    def load_json(self, entry) -> List[List]:
        headers = OrderedDict()
        records = []
        counter = 0
        with self.fopen(entry) as data:
            for line in data:
                record = json.loads(line)
                for h in record:
                    if h not in headers:
                        headers[h] = 1
                records.append(record)
                counter += 1
                if counter > self.lines_to_load:
                    break
        self.csv_columns = list(headers.keys())
        rows = [
            [record.get(h, None) for h in self.csv_columns]
            for record in records
        ]
        return rows

    def guess_types(self, rows: list, lines: list):
        m = len(rows)
        n = len(rows[0])
        self.types.clear()
        for c in range(0, n):
            c_type = None
            precision = 0
            scale = 0
            max_val = 0
            for l in range(0, m):
                v = rows[l][c].strip()
                if lines:
                    v2 = lines[l][c].strip()
                else:
                    v2 = None
                if date.fullmatch(v):
                    t = "DATE"
                elif v2 and v2 == '"{}"'.format(v):
                    t = PG_STR_TYPE
                    self.quoted_values = True
                elif SpecialValues.is_untyped(v):
                    t = "0"
                else:
                    f = float_number.fullmatch(v)
                    if f:
                        t = PG_NUMERIC_TYPE
                        s = len(f.group(2))
                        p = len(f.group(1))
                        scale = max(scale, s)
                        precision = max(precision, p)
                    elif exponent.fullmatch(v):
                        t = PG_NUMERIC_TYPE
                    elif integer.fullmatch(v):
                        t = PG_INT_TYPE
                        max_val = max(max_val, abs(int(v)))
                    else:
                        t = PG_STR_TYPE
                if t == "0":
                    continue
                try:
                    c_type = self.reconcile(t, c_type)
                except InconsistentTypes:
                    msg = "Inconsistent type for column {:d} [{:s}]. " \
                        .format(c + 1, self.csv_columns[c])
                    msg += "Up to line {:d}: {:s}, for line={:d}: {:s}. " \
                        .format(l - 1, c_type, l, t)
                    msg += "Value = {}".format(v)
                    raise Exception(msg)
            self.types.append(self.db_type(c_type, max_val, precision, scale))
        return

    @staticmethod
    def reconcile(cell_type, column_type) -> str:
        if column_type == "0":
            column_type = cell_type
        elif column_type == PG_NUMERIC_TYPE and cell_type == PG_INT_TYPE:
            return column_type
        elif column_type == PG_STR_TYPE and cell_type in [PG_INT_TYPE, PG_NUMERIC_TYPE]:
            return column_type
        elif column_type == PG_INT_TYPE and cell_type == PG_NUMERIC_TYPE:
            column_type = cell_type
        elif column_type in [PG_INT_TYPE, PG_NUMERIC_TYPE] and cell_type == PG_STR_TYPE:
            column_type = cell_type
        elif (column_type and column_type != cell_type):
            raise InconsistentTypes
        else:
            column_type = cell_type
        return column_type

    @staticmethod
    def db_type(column_type, max_val, precision, scale) -> str:
        if column_type == PG_INT_TYPE and max_val * 10 > PG_MAXINT:
            column_type = PG_BIGINT_TYPE
        if column_type == PG_NUMERIC_TYPE and precision and scale:
            column_type = column_type + "({:d},{:d})".format(
                precision + scale + 2, scale
            )
        if column_type == "0":
            column_type = PG_NUMERIC_TYPE
        if not column_type:
            column_type = PG_STR_TYPE
        return column_type

    def get_columns(self) -> List[Dict]:
        columns = []
        for i, c in enumerate(self.sql_columns):
            t = self.types[i]
            s = self.csv_columns[i]
            column = {
                c: {
                    "type": t
                }
            }
            if s != c:
                column[c]["source"] = s
            columns.append(column)

        return columns


class InconsistentTypes(Exception):
    pass


def test():
    for arg in sys.argv[1:]:
        introspector = Introspector(arg)
        introspector.introspect()
        columns = introspector.get_columns()
        print(arg)
        print(yaml.dump(columns))


if __name__ == '__main__':
    test()

