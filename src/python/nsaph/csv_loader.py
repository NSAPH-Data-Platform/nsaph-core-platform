from re import Pattern

import psycopg2
import os
import re
import sys
from configparser import ConfigParser
import csv


def regex(pattern: str) -> re.Pattern:
    pattern = 'A' + pattern.replace('.', '_') + 'Z'
    x = pattern.split('*')
    y = [re.escape(s) for s in x]
    regexp = ".*".join(y)[1:-1]
    return re.compile(regexp)


index_columns = [
    "fips",
    "monitor",
    "name",
    "year",
    "zip",
    regex("*.code"),
    regex("*.type"),
    regex("*.name")
]


NA = "NA"

def is_index_column(c: str) -> bool:
    c = c.lower()
    for i in index_columns:
        if isinstance(i, Pattern):
            if i.fullmatch(c):
                return True
        else:
            if i == c:
                return True
    return False


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
        return self._replace_empty(line)

    def _read(self, size, *args, **keyargs):
        _line = self.file_like_object.read(size, *args, **keyargs)
        line = _line
        while line[-self.l:] == self.sep:
            next_char = self.file_like_object.read(self.l)
            line += next_char
        line = self._replace_empty(line)
        if self.remainder:
            line = self.remainder + line
            self.remainder = ""

        self.line_number += line.count('\n')
        if len(line) > size:
            self.remainder = line[size - len(line):]
            return line[0:size]
        return line



integer = re.compile("\d+")
float_number = re.compile("(\d*)\.(\d+)([e|E][-|+]?\d+)?")
exponent = re.compile("(\d+)([e|E][-|+]?\d+)")


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


def guess_types(rows: list, lines: list) -> list:
    m = len(rows)
    n = len(rows[0])
    types = []
    for c in range(0,n):
        type = None
        precision = 0
        scale = 0
        for l in range(0,m):
            v = rows[l][c].strip()
            v2 = lines[l][c].strip()
            if '"' in v2:
                t = "VARCHAR"
            elif not v or v in ['0', NA]:
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
                raise Exception("Inconsistent type for column {:d}".format(c))
            else:
                type = t
        if type == "NUMERIC":
            precision += scale
            type = type + "({:d},{:d})".format(precision+2, scale)
        if type == "0":
            type =  "NUMERIC"
        types.append(type)
    return types


def create_table(file_path: str, cursor = None) -> str:
    has_commas = False
    file_name = os.path.basename(file_path)
    name, _ = os.path.splitext(file_name)
    with open(file_path) as data:
        reader = csv.reader(data, quotechar='"', delimiter=',',
                     quoting=csv.QUOTE_ALL, skipinitialspace=True)
        row = next(reader)
        columns = [unquote(c) for c in row]

        rows = []
        for i in range(0,10000):
            rows.append(next(reader))
    with open(file_path) as data:
        reader = csv.reader(data, quotechar='"', delimiter=',',
                     quoting=csv.QUOTE_NONE, skipinitialspace=True)
        next(reader)
        lines = []
        for i in range(0,10000):
            lines.append(next(reader))

    if not rows:
        raise Exception("No data in {}".format(file_path))
    types = guess_types(rows, lines)
    for row in rows:
        for cell in row:
            if ',' in cell:
                has_commas = True
                break

    sql_columns = [c.replace('.', '_') for c in columns]
    col_spec = ["{} \t{}".format(sql_columns[i], types[i]) for i in range(0, len(columns))]
    ddl = "CREATE TABLE {}\n ({})".format(name, ",\n\t".join(col_spec))

    for c in sql_columns:
        if is_index_column(c):
            ddl += ";\nCREATE INDEX {table}_{column}_idx ON {table} ({column})"\
                .format(table = name, column = c)

    print (ddl)

    if cursor:
        cursor.execute(ddl)
        if has_commas:
            copy_data(cursor, name, sql_columns, types, file_path)
        else:
            with open(file_path) as data:
                with CSVFileWrapper(data) as csv_data:
                    csv_data.readline()
                    cursor.copy_from(csv_data, name, sep=',', null=NA, columns=sql_columns)

    return name


def copy_data (cursor, table, columns, types, file_path):
    N = 10000
    insert = "INSERT INTO {table} ({columns}) VALUES "\
        .format(table = table, columns=','.join(columns))
    #values = "({})".format('')
    with open(file_path) as data:
        reader = csv.reader(data, quotechar='"', delimiter=',',
                     quoting=csv.QUOTE_ALL, skipinitialspace=True)
        next(reader)
        counter = 0
        sql = insert
        while True:
            try:
                row = next(reader)
            except(StopIteration):
                break
            counter += 1
            for i in range(0, len(types)):
                if row[i] == NA:
                    row[i] = "NULL"
                elif types[i] == "VARCHAR":
                    row[i] = "'{}'".format(row[i].replace("'", "''"))
            values = "({})".format(','.join(row))
            sql += values
            if (counter % N) == 0:
                cursor.execute(sql)
                print(counter)
                sql = insert
            else:
                sql += ","
        if (counter % N) != 0:
            if sql[-1] == ',':
                sql = sql[:-1]
            cursor.execute(sql)
    return


def ingest(path: str) -> str:
    connection = None
    try:
        connection = connect()
        cur = connection.cursor()
        name = create_table(path, cur)

        print("Table created")
        cur.close()

        connection.commit()
    finally:
        if connection is not None:
            connection.close()
            print('Database connection closed.')



if __name__ == '__main__':
    test_connection()
    ingest(sys.argv[1])

