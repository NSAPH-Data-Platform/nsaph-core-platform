import codecs
import gzip
import io
import tarfile

import psycopg2
import os
import re
#from re import Pattern
import sys
from configparser import ConfigParser
import csv
import datetime


#def regex(pattern: str) -> Pattern:
def regex(pattern: str):
    pattern = 'A' + pattern.replace('.', '_') + 'Z'
    x = pattern.split('*')
    y = [re.escape(s) for s in x]
    regexp = ".*".join(y)[1:-1]
    return re.compile(regexp)


index_columns = {
    "date":"btree",
    "fips":"hash",
    "monitor":"hash",
    "name":"btree",
    "state":"hash",
    "st_abbrev":"hash",
    "year":"btree",
    "zip":"btree",
    "*.code":"hash",
    "*.date":"btree",
    "*.type":"hash",
    "*.name":"btree"
}


NA = "NA"
NaN = "NaN"


def fopen(path):
    if isinstance(path, io.BufferedReader):
        return codecs.getreader("utf-8")(path)
    if not isinstance(path, str):
        return path
    if path.endswith(".gz"):
        return gzip.open(path, "rt")
    return open(path)


def name(path):
    if isinstance(path, tarfile.TarInfo):
        full_name =  path.name
    else:
        full_name = str(path)
    name, _ = os.path.splitext(os.path.basename(full_name))
    return name


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


class CSVFileWrapper():
    def __init__(self, file_like_object, sep = ',', null_replacement = NA):
        self.file_like_object = file_like_object
        self.sep = sep
        self.null_replacement = null_replacement
        self.empty_string = self.sep + self.sep
        self.null_string = self.sep + self.null_replacement + sep
        self.empty_string_eol = self.sep + '\n'
        self.null_string_eol = self.sep + self.null_replacement + '\n'
        self.l = len(sep)
        self.remainder = ""
        self.line_number = 0
        self.last_printed_line_number = 0
        self.chars = 0

    def __getattr__(self, called_method):
        if called_method == "readline":
            return self._readline
        if called_method == "read":
            return self._read
        return getattr(self.file_like_object, called_method)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.file_like_object.close()

    def _replace_empty(self, s: str):
        while self.empty_string in s:
            s = s.replace(self.empty_string, self.null_string)
        s = s.replace(self.empty_string_eol, self.null_string_eol)
        return s

    def _readline(self):
        line = self.file_like_object.readline()
        self.line_number += 1
        self.chars += len(line)
        return self._replace_empty(line)

    def _read(self, size, *args, **keyargs):
        if (len(self.remainder) < size):
            raw_buffer = self.file_like_object.read(size, *args, **keyargs)
            buffer = raw_buffer
            while buffer[-self.l:] == self.sep:
                next_char = self.file_like_object.read(self.l)
                buffer += next_char
            buffer = self._replace_empty(buffer)
        else:
            raw_buffer = ""
            buffer = raw_buffer
        if self.remainder:
            buffer = self.remainder + buffer
            self.remainder = ""

        if len(buffer) > size:
            self.remainder = buffer[size - len(buffer):]
            result = buffer[0:size]
        else:
            result = buffer

        self.chars += len(result)
        nl = result.count('\n')
        self.line_number += nl
        t = datetime.datetime.now()
        if (self.line_number - self.last_printed_line_number) > 1000000:
            if self.chars > 1000000000:
                c = "{:7.2f}G".format(self.chars/1000000000.0)
            elif self.chars > 1000000:
                c = "{:6.2f}M".format(self.chars/1000000.0)
            else:
                c = str(self.chars)
            dt = datetime.datetime.now() - t
            t = datetime.datetime.now()
            print("{}: Processed {:d}/{} lines/chars [{}]"
                  .format(str(t), self.line_number, c, str(dt)))
            self.last_printed_line_number = self.line_number
        return result



integer = re.compile("-?\d+")
float_number = re.compile("(-?\d*)\.(\d+)([e|E][-|+]?\d+)?")
exponent = re.compile("(-?\d+)([e|E][-|+]?\d+)")
date = re.compile("([12]\d{3}-(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01]))")


def unquote(s: str) -> str:
    return s.strip().strip('"')


def config(filename='database.ini', section='postgresql'):
    home = os.getenv("NSAPH_HOME")
    if home and not os.path.isabs(filename):
        filename = os.path.join(home, filename)
    parser = ConfigParser()
    parser.read(filename)

    db = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db[param[0]] = param[1]
    else:
        raise Exception('Section {0} not found in the {1} file'.format(section, filename))

    return db


def connect():
    params = config()

    print('Connecting to the PostgreSQL database...')
    conn = psycopg2.connect(**params)
    return conn


def test_connection ():
    conn = None
    try:
        conn = connect()

        cur = conn.cursor()

        print('PostgreSQL database version:')
        cur.execute('SELECT version()')

        db_version = cur.fetchone()
        print(db_version)

        cur.close()
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')

def load_range(n, f) -> int:
    for i in range(0, n):
        try:
            f()
        except StopIteration:
            return i
    return n


class CustomColumn:
    def __init__(self, name, type, extraction_method):
        self.name = name
        self.type = type
        self.extraction_method = extraction_method

    def extract_value(self, input_source):
        if isinstance(self.extraction_method, int):
            n = self.extraction_method - 1
            return name(input_source).split('_')[n]


class Table:
    def __init__(self, file_path: str, get_entry, concurrent_indices: bool = False):
        if concurrent_indices:
            self.index_option = "CONCURRENTLY"
        else:
            self.index_option = ""
        self.file_path = file_path
        file_name = os.path.basename(file_path)
        name, ext = os.path.splitext(file_name)
        while ext not in [".csv", ""]:
            name, ext = os.path.splitext(name)
        self.table = name
        self.has_commas = False
        self.sql_columns = []
        self.csv_columns = []
        self.types = []
        self.get_entry = get_entry
        self.force_manual = False
        self.add_columns = []

    def fopen(self, source):
        return fopen(self.get_entry(source))

    def add_column(self, name, type, extraction_method):
        self.add_columns.append(CustomColumn(name, type, extraction_method))
        self.force_manual = True

    def create(self, cursor = None, entry = None):
        if not entry:
            entry = self.file_path
        print("Using for data analysis: " + name(entry))
        with self.fopen(entry) as data:
            reader = csv.reader(data, quotechar='"', delimiter=',',
                         quoting=csv.QUOTE_ALL, skipinitialspace=True)
            row = next(reader)
            self.csv_columns = [unquote(c) for c in row]

            rows = []
            load_range(10000, lambda : rows.append(next(reader)))
        with self.fopen(entry) as data:
            reader = csv.reader(data, quotechar='"', delimiter=',',
                         quoting=csv.QUOTE_NONE, skipinitialspace=True)
            next(reader)
            lines = []
            load_range(10000, lambda : lines.append(next(reader)))

        if not rows:
            raise Exception("No data in {}".format(self.file_path))
        self.guess_types(rows, lines)
        for row in rows:
            for cell in row:
                if ',' in cell:
                    self.has_commas = True
                    break

        self.sql_columns = [
            c.replace('.', '_') if c else "Col" for c in self.csv_columns
        ]
        col_spec = [
            "{} \t{}".format(self.sql_columns[i], self.types[i])
                for i in range(0, len(self.csv_columns))
        ]
        for c in self.add_columns:
            self.sql_columns.append(c.name)
            col_spec.append("{} \t{}".format(c.name, c.type))
        ddl = "CREATE TABLE {}\n ({})".format(self.table, ",\n\t".join(col_spec))

        print (ddl)
        if cursor:
            cursor.execute(ddl)

    def build_indices(self, cursor):
        ddl_pattern = "CREATE INDEX {option} {table}_{column}_idx ON {table} USING {method} ({column})"
        for c in self.sql_columns:
            m = index_method(c)
            if m:
                ddl = ddl_pattern \
                    .format(option=self.index_option,
                            table = self.table,
                            column = c,
                            method = m)
                print(str(datetime.datetime.now()) + ": " + ddl)
                if cursor:
                    cursor.execute(ddl)

    def drop(self, cursor):
        sql = "DROP TABLE {} CASCADE".format(self.table)
        print(sql)
        cursor.execute(sql)

    def guess_types(self, rows: list, lines: list):
        m = len(rows)
        n = len(rows[0])
        self.types.clear()
        for c in range(0, n):
            type = None
            precision = 0
            scale = 0
            for l in range(0, m):
                v = rows[l][c].strip()
                v2 = lines[l][c].strip()
                if date.fullmatch(v):
                    t = "DATE"
                elif '"' in v2:
                    t = "VARCHAR"
                elif not v or v in ['0', NA, NaN]:
                    t = "0"
                else:
                    f = float_number.fullmatch(v)
                    if f:
                        t = "NUMERIC"
                        s = len(f.group(2))
                        p = len(f.group(1))
                        scale = max(scale, s)
                        precision = max(precision, p)
                    elif exponent.fullmatch(v):
                        t = "NUMERIC"
                    elif integer.fullmatch(v):
                        t = "INT"
                    else:
                        t = "VARCHAR"
                if t == "0":
                    continue
                if type == "0":
                    type = t
                elif type == "NUMERIC" and t == "INT":
                    continue
                elif type == "VARCHAR" and t in ["INT", "NUMERIC"]:
                    continue
                elif type == "INT" and t == "NUMERIC":
                    type = t
                elif (type and type != t):
                    msg = "Inconsistent type for column {:d} [{:s}]. " \
                        .format(c + 1, self.csv_columns[c])
                    msg += "Up to line {:d}: {:s}, for line={:d}: {:s}. " \
                        .format(l - 1, type, l, t)
                    msg += "Value = {}".format(v)
                    raise Exception(msg)
                else:
                    type = t
            if type == "NUMERIC":
                precision += scale
                type = type + "({:d},{:d})".format(precision + 2, scale)
            if type == "0":
                type = "NUMERIC"
            if not type:
                type = "VARCHAR"
            self.types.append(type)
        return

    def add_data(self, cursor, entry):
        if self.has_commas:
            print("The CSV file contains commas, copying manually.")
            self.copy_data(cursor, entry)
        elif self.force_manual:
            print("Forcing manual copy of the data.")
            self.copy_data(cursor, entry)
        else:
            print("Copying data using system function.")
            with self.fopen(entry) as data:
                with CSVFileWrapper(data) as csv_data:
                    csv_data.readline()
                    cursor.copy_from(csv_data, self.table, sep=',', null=NA,
                                     columns=self.sql_columns)


    def copy_data (self, cursor, input_source):
        N = 10000
        insert = "INSERT INTO {table} ({columns}) VALUES "\
            .format(table = self.table, columns=','.join(self.sql_columns))
        #values = "({})".format('')
        with self.fopen(input_source) as data:
            reader = csv.reader(data, quotechar='"', delimiter=',',
                         quoting=csv.QUOTE_ALL, skipinitialspace=True)
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
                lines += 1
                chars += sum([len(cell) for cell in row])
                for i in range(0, len(self.types)):
                    if row[i] in [NA, NaN]:
                        row[i] = "NULL"
                    elif self.types[i] in ["VARCHAR", "DATE", "TIMESTAMP", "TIME"]:
                        row[i] = "'{}'".format(row[i].replace("'", "''"))

                for c in self.add_columns:
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
        print("{}: Processed {:d}/{} lines/chars [t={}/{}, r={:5.1f}/{:5.1f} lines/sec]"
              .format(str(t), lines, c, str(dt1), str(dt0), r1, r0))
        return t


def ingest(args: list, force: bool = False):
    path = args[0]
    connection = None
    table = None
    try:
        connection = connect()
        cur = connection.cursor()

        entries = []
        f = lambda e: e
        if path.endswith(".tar") or path.endswith(".tgz") or path.endswith(".tar.gz"):
            tfile = tarfile.open(path)
            entries = [e for e in tfile.getmembers() if e.isfile()]
            f = lambda e: tfile.extractfile(e)
        elif os.path.isdir(path):
            pass
        else:
            entries.append(path)

        table = Table(path, f)
        if force:
            try:
                table.drop(connection.cursor())
            except:
                pass
        for a in args[1:]:
            x = a.split(':')
            table.add_column(x[0], x[1], int(x[2]))
        table.create(cur, entries[0])

        for e in entries:
            print ("Adding: " + name(e))
            table.add_data(cur, e)

        print("Table created: " + table.table)
        cur.close()
        connection.commit()

        print("Building indices:")
        table.build_indices(connection.cursor())
        print ("All DONE")

    except Exception as x:
        if table and connection and connection.autocommit:
            table.drop(connection.cursor())
        raise x
    finally:
        if connection is not None:
            connection.close()
            print('Database connection closed.')



if __name__ == '__main__':
    #test_connection()
    ingest(sys.argv[1:], True)

