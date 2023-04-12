"""
This module introspects columnar data to infer the types of
the columns
"""
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

import csv
import datetime
import glob
import json
import logging
import os
import re
import sys
import tarfile
from collections import OrderedDict
from typing import Dict, Callable, List, Union, Optional

import numbers
import yaml
from nsaph_utils.utils.io_utils import fopen, SpecialValues, is_dir, get_entries
from sas7bdat import SAS7BDAT

from nsaph.pg_keywords import *
from nsaph_utils.utils.pyfst import FSTReader

PG_MAXINT = 2147483647


integer = re.compile("-?\d+")
float_number = re.compile("(-?\d*)\.(\d+)([e|E][-|+]?\d+)?")
exponent = re.compile("(-?\d+)([e|E][-|+]?\d+)")
_date = "([12]\d{3}-(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01]))"
date = re.compile(_date)
timestamp = re.compile(_date + "[T|t][0-9]{2}:[0-9]{2}")


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
        #name, _ = os.path.splitext(os.path.basename(full_name))
        return full_name

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
        self.descriptions = None
        self.column_map = column_name_replacement \
            if column_name_replacement else dict()
        self.appended_columns = []
        return

    def fopen(self, source):
        entry = self.open_entry_function(source)
        return entry

    def handle_csv(self, entry):
        rows, lines = self.load_csv(entry)
        for row in rows:
            for cell in row:
                if isinstance(cell, str) and ',' in cell:
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
        scale = 0
        precision = 0
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
                    t, scale, precision, max_val = self.guess_str(
                        v, None, scale, precision, max_val
                    )
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
            self.types.append(self.db_type(c_type, max_val, precision, scale))
        return

    @classmethod
    def sas2db_type(cls, column, rows):
        if column.type == "number":
            values = [row[column.col_id] for row in rows]
            is_date = all([isinstance(v, datetime.date) or v is None for v in values])
            if is_date:
                return PG_DATE_TYPE
            is_ts = all([isinstance(v, datetime.datetime) or v is None for v in values])
            if is_ts:
                return PG_TS_TYPE
            max_value = max(values)
            is_int = all([isinstance(v, numbers.Integral) or v is None for v in values])
            t = PG_INT_TYPE if is_int else PG_NUMERIC_TYPE
            return cls.db_type(t, max_value, None, None)
        if column.type == "string":
            return "{}({:d})".format(PG_STR_TYPE, column.length)
        raise ValueError("Unknown SAS datatype: {}".format(column.type))

    def handle_sas(self, entry):
        reader = SAS7BDAT(entry, skip_header=True)
        rows = self.load_sas(reader)
        sas_columns = reader.columns
        self.csv_columns = [
            column.name.decode("utf-8") for column in sas_columns
        ]
        self.descriptions = [
            column.label.decode("utf-8") for column in sas_columns
        ]
        self.types = [self.sas2db_type(column, rows)  for column in sas_columns]

    def introspect(self, entry=None):
        if not entry:
            if self.entries is not None:
                entry = self.entries[0]
            else:
                entry = self.file_path
        logging.info("Using for data analysis: " + self.name(entry))
        if ".json" in self.name(entry).lower():
            self.handle_json(entry)
        elif self.name(entry).lower().endswith(".sas7bdat"):
            self.handle_sas(entry)
        else:
            self.handle_csv(entry)
        self.sql_columns = []
        for i, c in enumerate(self.csv_columns):
            if not c:
                self.append_sql_column("Col{:03d}".format(i+1))
            elif c.lower() in self.column_map:
                self.append_sql_column(self.column_map[c.lower()])
            else:
                if c[0].isdigit():
                    c = "c_" + c
                self.append_sql_column(
                    c.replace('.', '_')
                    .replace(' ', '_')
                    .lower()
                )
        return

    def append_sql_column(self, name: str):
        fmt = "{}_{:03d}"
        if name in self.sql_columns:
            n = len(self.sql_columns)
            i = self.sql_columns.index(name)
            self.sql_columns[i] = fmt.format(name, i+1)
            name = fmt.format(name, n)
        self.sql_columns.append(name)
        return

    def load_csv(self, entry) -> (List[List[str]], List[List[str]]):
        if isinstance(entry, str) and entry.lower().endswith(".fst"):
            return self.load_fst(entry)
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

    def load_fst(self, entry) -> (List[List[str]], List[List[str]]):
        with FSTReader(entry, buffer_size=self.lines_to_load + 10) as reader:
            self.csv_columns = [c for c in reader.columns]
            rows = []
            self.load_range(self.lines_to_load, lambda : rows.append(next(reader)))
            lines = None
        return rows, lines

    def load_sas(self, reader: SAS7BDAT) -> List[List]:
        rows = []
        for row in reader:
            rows.append(row)
            if len(rows) >= self.lines_to_load:
                break
        return rows

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

    def guess_str(self, v, v2: Optional[str], scale, precision, max_val):
        if timestamp.fullmatch(v):
            t = PG_TS_TYPE
        elif date.fullmatch(v):
            t = PG_DATE_TYPE
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
        if t == PG_STR_TYPE:
            max_val = max(max_val, len(v))
        return t, scale, precision, max_val

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
                cell = rows[l][c]
                if isinstance(cell, numbers.Number):
                    if isinstance(cell, numbers.Integral):
                        t = PG_INT_TYPE
                    else:
                        t = PG_NUMERIC_TYPE
                elif SpecialValues.is_missing(cell):
                    t = "0"
                else:
                    v = cell.strip()
                    if lines:
                        try:
                            v2 = lines[l][c].strip()
                        except:
                            v2 = None
                    else:
                        v2 = None
                    t, scale, precision, max_val = \
                        self.guess_str(v, v2, scale, precision, max_val)
                if t == "0":
                    continue
                try:
                    c_type = self.reconcile(t, c_type)
                except InconsistentTypes:
                    msg = "Inconsistent type for column {:d} [{:s}]. " \
                        .format(c + 1, self.csv_columns[c])
                    msg += "Up to line {:d}: {:s}, for line={:d}: {:s}. " \
                        .format(l - 1, c_type, l, t)
                    msg += "Value = {}".format(cell)
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
        if column_type == PG_STR_TYPE and max_val > 256:
            column_type = PG_TXT_TYPE
        return column_type

    def get_columns(self) -> List[Dict]:
        columns = self.appended_columns
        for i, c in enumerate(self.sql_columns):
            t = self.types[i]
            s = self.csv_columns[i]
            column = {
                c: {
                    "type": t,
                }
            }
            if self.descriptions is not None:
                column[c]["description"] = self.descriptions[i]
            if not s:
                column[c]["source"] = i
            elif s != c:
                column[c]["source"] = s
            columns.append(column)

        return columns

    def append_file_column(self):
        self.appended_columns.append({
                    "FILE": {
                        "description": "original file name",
                        "index": {
                            "required_before_loading_data": True
                        },
                        "source": {
                            "type": "file"
                        },
                        "type": "VARCHAR(128)"
                    }
                })

    def append_record_column(self):
        self.appended_columns.append({
                        "RECORD": {
                            "description": "Record (line) number in the file",
                            "index": True,
                            "type": PG_SERIAL_TYPE
                        }
                    }
        )

    @classmethod
    def classify(cls, files):
        classes = []
        for f in files:
            print(f)
            introspector = Introspector(f)
            introspector.introspect()
            cc = introspector.get_columns()
            new = True
            for c in classes:
                if c[0] == cc:
                    c[1].append(f)
                    new = False
                    break
            if new:
                classes.append([cc, [f]])
        print("Found {:d} classes".format(len(classes)))
        for i, c in enumerate(classes):
            print("{:d}:".format(i+1))
            for f in c[1]:
                print("\t{}".format(f))


class InconsistentTypes(Exception):
    pass


def test():
    if len(sys.argv) == 2:
        args = glob.glob(sys.argv[1])
    else:
        args = sys.argv[1:]

    if len(args) > 1:
        Introspector.classify(args)
    else:
        arg = args[0]
        print(arg)
        introspector = Introspector(arg)
        introspector.introspect()
        columns = introspector.get_columns()
        print(yaml.dump(columns))


if __name__ == '__main__':
    test()

