import json
import yaml
import logging
import os
import re
import csv
import datetime
import sys
import traceback
from typing import Dict, Optional

from nsaph.data_model.utils import regex
from nsaph_utils.utils.io_utils import CSVFileWrapper, basename, fopen, SpecialValues
from nsaph.pg_keywords import *


PG_MAXINT = 2147483647

METADATA_TYPE_KEY = "type"
METADATA_TYPE_MODEL = "model_specification"
INDEX_DDL_PATTERN = "CREATE INDEX {option} {name} ON {table} USING {method} ({column})"
INDEX_NAME_PATTERN = "{table}_{column}_idx"

BTREE = "btree"
#HASH = "hash"
HASH = BTREE  # problem building huge hash indices

index_columns = {
    "date":BTREE,
    "fips":HASH,
    "monitor":HASH,
    "name":BTREE,
    "state":HASH,
    "st_abbrev":HASH,
    "year":BTREE,
    "zip":BTREE,
    "*.code":BTREE,
    "*code":BTREE,
    "fips*": BTREE,
    "*.date":BTREE,
    "date_local":BTREE,
    "*.type":HASH,
    "*.name":BTREE
}


def index_method(c: str) -> (str,None):
    c = c.lower()
    for i in index_columns:
        if "*" in i:
            if regex(i).fullmatch(c):
                return index_columns[i]
        else:
            if i == c:
                return index_columns[i]
    return None


integer = re.compile("-?\d+")
float_number = re.compile("(-?\d*)\.(\d+)([e|E][-|+]?\d+)?")
exponent = re.compile("(-?\d+)([e|E][-|+]?\d+)")
date = re.compile("([12]\d{3}-(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01]))")

SET_COLUMN = "UPDATE {table} SET {column} = {expression}"

def unquote(s: str) -> str:
    return s.strip().strip('"')


def load_range(n, f) -> int:
    for i in range(0, n):
        try:
            f()
        except StopIteration:
            return i
    return n


def csv_reader(data, unquote = True):
    if unquote:
        q = csv.QUOTE_ALL
    else:
        q = csv.QUOTE_NONE
    return csv.reader(data, quotechar='"', delimiter=',',
        quoting=q, skipinitialspace=True)


class CustomColumn:
    def __init__(self, name, type, extraction_method):
        self.name = name
        self.type = type
        self.extraction_method = extraction_method

    def extract_value(self, input_source):
        if isinstance(self.extraction_method, int):
            n = self.extraction_method - 1
            return basename(input_source).split('_')[n]


class Table:
    def __init__(self, metadata_file: str = None,
                 get_entry = None,
                 concurrent_indices: bool = False,
                 data_file: str = None,
                 column_name_replacement: Dict = None):
        metadata = None
        if metadata_file:
            fn = metadata_file.lower()
            with open(metadata_file) as fp:
                if fn.endswith("json"):
                    metadata = json.load(fp)
                elif fn.endswith(".yaml") or fn.endswith(".yml"):
                    metadata = yaml.safe_load(fp)
                else:
                    raise Exception("Unsupported file type for metadata")
            if metadata.get(METADATA_TYPE_KEY) == METADATA_TYPE_MODEL:
                self.open_entry_function = get_entry
                with open(metadata_file) as fp:
                    j = json.load(fp)
                for a in j:
                    setattr(self, a, j[a])
                if data_file:
                    self.file_path = data_file
                return

        self.metadata = metadata
        if concurrent_indices:
            self.index_option = "CONCURRENTLY"
        else:
            self.index_option = ""
        self.file_path = os.path.abspath(data_file)
        file_name = os.path.basename(self.file_path)
        tname, ext = os.path.splitext(file_name)
        while ext not in [".csv", ""]:
            tname, ext = os.path.splitext(tname)
        tname = tname.replace('-', '_')
        self.table = tname
        self.has_commas = False
        self.quoted_values = False
        self.sql_columns = []
        self.csv_columns = []
        self.types = []
        self.open_entry_function = get_entry
        self.force_manual = False
        self.custom_columns = []
        self.create_table_ddl = None
        self.index_ddl = []
        self.column_map = column_name_replacement \
            if column_name_replacement else dict()

    def save(self, to_dir):
        f = os.path.join(to_dir, self.table + ".json")
        j = dict()
        j[METADATA_TYPE_KEY] = METADATA_TYPE_MODEL
        for att in dir(self):
            if att[0] != '_':
                v = getattr(self, att)
                if callable(v):
                    continue
                j[att] = v

        with open (f, "w") as config:
            json.dump(j, config, indent=2)
        logging.info("Saved table definition to: " + f)

    def fopen(self, source):
        entry = self.open_entry_function(source)
        if isinstance(entry, str):
            return fopen(entry, "rt")
        else:
            return entry

    def get_index_ddl(self, column, method):
        n = INDEX_NAME_PATTERN.format(table = self.table, column = column)
        ddl = INDEX_DDL_PATTERN \
            .format(option=self.index_option,
                    name = n,
                    table=self.table,
                    column=column,
                    method = method)
        return n, ddl

    def add_column(self, name, type, extraction_method):
        self.custom_columns.append(CustomColumn(name, type, extraction_method))
        if extraction_method:
            self.force_manual = True

    def make_column(self, name, type, sql, cursor, index=False, include_in_index=None):
        ddl = "ALTER TABLE {table} ADD {column} {type}"\
            .format(table = self.table, column = name, type = type)
        logging.info(ddl)
        cursor.execute(ddl)
        logging.info(sql)
        cursor.execute(sql)
        if index:
            idx, ddl = self.get_index_ddl(column=name, method=HASH)
            if include_in_index:
                ddl += " include({})".format(include_in_index)
            logging.info(ddl)
            cursor.execute(ddl)

    def make_fips_column(self, cursor):
        column = "fips5"
        s = "format('%2s', state_code)"
        c = "format('%3s', county_code)"
        e = "replace({} || {}, ' ', '0')".format(s, c)
        sql = SET_COLUMN.format(table=self.table, column=column, expression=e)
        self.make_column(column, PG_STR_TYPE, sql, cursor, True)

    def make_iso_column(self, anchor, cursor, include = None):
        """
        Add US State ISO 3166-2 code
        Source: https://simplemaps.com/data/us-cities

        :param anchor: existing column to use to calculate state iso code
        :param cursor: Database cursor
        :param include: Additional data (column) to include in index on the new column
        :return:
        """

        column = "state_iso"
        if anchor.lower() == "state_name":
            e = "(SELECT iso FROM us_states " \
                "WHERE us_states.state_name = {}.{})"\
                .format(self.table, anchor)
        else:
            us_iso_column = {
                "fips": "county_fips",
                "fips5": "county_fips"
            }.get(anchor.lower(), anchor.lower())
            e = "(SELECT iso FROM us_iso WHERE us_iso.{} = {}.{} LIMIT 1)"\
                .format(us_iso_column, self.table, anchor)
        sql = SET_COLUMN.format(table=self.table, column=column, expression=e)
        self.make_column(column, PG_STR_TYPE, sql, cursor, index=True, include_in_index=include)

    def parse_fips12(self, cursor):
        column = "fips5"
        e = "CAST(substring(fips12, 1, 5) AS INTEGER)"
        sql = SET_COLUMN.format(table=self.table, column=column, expression=e)
        # self.make_column(column, PG_STR_TYPE, sql, cursor, True)
        self.make_column(column, "INTEGER", sql, cursor, True)

    def make_int_column(self, cursor, source: str, target:str, index: bool):
        type = "INTEGER"
        e = "CAST(NULLIF(REGEXP_REPLACE({what}, '[^0-9]', '','g'), '') AS {to})"\
            .format(what=source, to=type)
        sql = SET_COLUMN.format(table=self.table, column=target, expression=e)
        self.make_column(target, type, sql, cursor, index)

    def analyze(self, entry=None):
        if not entry:
            entry = self.file_path
        logging.info("Using for data analysis: " + name(entry))
        with self.fopen(entry) as data:
            reader = csv_reader(data, True)
            row = next(reader)
            self.csv_columns = [unquote(c) for c in row]

            rows = []
            load_range(10000, lambda : rows.append(next(reader)))
        with self.fopen(entry) as data:
            reader = csv_reader(data, False)
            next(reader)
            lines = []
            load_range(10000, lambda : lines.append(next(reader)))

        if not rows:
            raise Exception("No data in {}".format(self.file_path))
        for row in rows:
            for cell in row:
                if ',' in cell:
                    self.has_commas = True
                    break
        self.guess_types(rows, lines)


        self.sql_columns = []
        for c in self.csv_columns:
            if not c:
                self.sql_columns.append("Col")
            elif c.lower() in self.column_map:
                self.sql_columns.append(self.column_map[c.lower()])
            else:
                self.sql_columns.append(c.replace('.', '_').lower())

        col_spec = [
            "{} \t{}".format(self.sql_columns[i], self.types[i])
                for i in range(0, len(self.csv_columns))
        ]
        for c in self.custom_columns:
            self.sql_columns.append(c.name)
            col_spec.append("{} \t{}".format(c.name, c.type))
        self.create_table_ddl = \
            "CREATE TABLE {}\n ({})".format(self.table, ",\n\t".join(col_spec))

        for c in self.sql_columns:
            m = index_method(c)
            if m:
                self.index_ddl.append(self.get_index_ddl(c, m))

    def create(self, cursor):
        logging.info(self.create_table_ddl)
        cursor.execute(self.create_table_ddl)

    def build_indices(self, cursor, flag: str = None):
        for ddl in self.index_ddl:
            command = ddl[1]
            name = ddl[0]
            if flag == INDEX_REINDEX:
                sql = "DROP INDEX IF EXISTS {name}".format(name=name)
                logging.info(str(datetime.datetime.now()) + ": " + sql)
                cursor.execute(sql)
            elif flag == INDEX_INCREMENTAL:
                command = command.replace(name, "IF NOT EXISTS " + name)
            elif flag and flag != "default":
                raise Exception("Invalid indexing flag: " + flag)
            logging.info(str(datetime.datetime.now()) + ": " + command)
            cursor.execute(command)

    def drop(self, cursor):
        sql = "DROP TABLE {} CASCADE".format(self.table)
        logging.info(sql)
        cursor.execute(sql)

    def type_from_metadata(self, c: int) -> Optional[str]:
        if not self.metadata:
            return None
        c_name = self.csv_columns[c]
        metadata = self.metadata
        if "table" in metadata:
            metadata = metadata["table"]
        if self.table in metadata:
            metadata = metadata[self.table]
        if "columns" in metadata:
            metadata = metadata["columns"]
        if c_name in metadata:
            return metadata[c_name]
        if isinstance(metadata, list):
            for element in metadata:
                if c_name in element:
                    return element[c_name]
        return None

    def guess_types(self, rows: list, lines: list):
        m = len(rows)
        n = len(rows[0])
        self.types.clear()
        for c in range(0, n):
            c_type = self.type_from_metadata(c)
            precision = 0
            scale = 0
            max_val = 0
            for l in range(0, m):
                v = rows[l][c].strip()
                v2 = lines[l][c].strip()
                if date.fullmatch(v):
                    t = "DATE"
                elif v2 == '"{}"'.format(v):
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
                    else:
                        t = PG_STR_TYPE
                if t == "0":
                    continue
                if t in [PG_INT_TYPE]:
                    max_val = max(max_val, abs(int(v)))
                if c_type == "0":
                    c_type = t
                elif c_type == PG_NUMERIC_TYPE and t == PG_INT_TYPE:
                    continue
                elif c_type == PG_STR_TYPE and t in [PG_INT_TYPE, PG_NUMERIC_TYPE]:
                    continue
                elif c_type == PG_INT_TYPE and t == PG_NUMERIC_TYPE:
                    c_type = t
                elif (c_type and c_type != t):
                    msg = "Inconsistent type for column {:d} [{:s}]. " \
                        .format(c + 1, self.csv_columns[c])
                    msg += "Up to line {:d}: {:s}, for line={:d}: {:s}. " \
                        .format(l - 1, c_type, l, t)
                    msg += "Value = {}".format(v)
                    raise Exception(msg)
                else:
                    c_type = t
            if c_type == PG_INT_TYPE and max_val * 10 > PG_MAXINT:
                c_type = PG_BIGINT_TYPE
            if c_type == PG_NUMERIC_TYPE:
                precision += scale
                c_type = c_type + "({:d},{:d})".format(precision + 2, scale)
            if c_type == "0":
                c_type = PG_NUMERIC_TYPE
            if not c_type:
                c_type = PG_STR_TYPE
            self.types.append(c_type)
        return

    def add_data(self, cursor, entry):
        if self.has_commas:
            logging.info("The CSV file contains commas, copying manually.")
            self.copy_data(cursor, entry)
        elif self.quoted_values:
            logging.info("The CSV file uses quoted values.")
            self.copy_data(cursor, entry)
        elif self.force_manual:
            logging.info("Forcing manual copy of the data.")
            self.copy_data(cursor, entry)
        else:
            logging.info("Copying data using system function.")
            with self.fopen(entry) as data:
                with CSVFileWrapper(data) as csv_data:
                    csv_data.readline()
                    cursor.copy_from(csv_data, self.table, sep=',',
                                     null=SpecialValues.NA,
                                     columns=self.sql_columns)


    def copy_data (self, cursor, input_source):
        N = 10000
        insert = "INSERT INTO {table} ({columns}) VALUES "\
            .format(table = self.table, columns=','.join(self.sql_columns))
        #values = "({})".format('')
        with self.fopen(input_source) as data:
            reader = csv_reader(data, True)
            next(reader)
            lines = 0
            chars = 0
            sql = insert
            t0 = datetime.datetime.now()
            t1 = t0
            while True:
                try:
                    row = next(reader)
                except(StopIteration):
                    break
                except:
                    logging.info(row)
                    traceback.print_exc(file=sys.stdout)
                lines += 1
                chars += sum([len(cell) for cell in row])
                for i in range(0, len(self.types)):
                    if SpecialValues.is_missing(row[i]):
                        row[i] = "NULL"
                    elif self.types[i] in [PG_STR_TYPE, PG_DATE_TYPE, PG_TS_TYPE, PG_TIME_TYPE]:
                        row[i] = "'{}'".format(row[i].replace("'", "''"))
                    elif self.types[i] in [PG_INT_TYPE, PG_BIGINT_TYPE,
                                           PG_NUMERIC_TYPE] and not row[i]:
                        row[i] = "NULL"

                for c in self.custom_columns:
                    row.append(c.extract_value(input_source))
                values = "({})".format(','.join(row))
                sql += values
                if (lines % N) == 0:
                    cursor.execute(sql)
                    t1 = self.log_progress(t0, t1, chars, lines, N)
                    sql = insert
                else:
                    sql += ","
            if (lines % N) != 0:
                if sql[-1] == ',':
                    sql = sql[:-1]
                cursor.execute(sql)
                self.log_progress(t0, t1, chars, lines, N)
        return

    @staticmethod
    def log_progress(t0, t1, chars, lines, N):
        if chars > 1000000000:
            c = "{:7.2f}G".format(chars / 1000000000.0)
        elif chars > 1000000:
            c = "{:6.2f}M".format(chars / 1000000.0)
        else:
            c = str(chars)
        t = datetime.datetime.now()
        dt1 = t - t1
        dt0 = t - t0
        r1 = N / dt1.total_seconds()
        r0 = lines / dt0.total_seconds()
        logging.info("{}: Processed {:,}/{} lines/chars [t={}({}), r={:5.1f}({:5.1f}) lines/sec]"
              .format(str(t), lines, c, str(dt1), str(dt0), r1, r0))
        return t


