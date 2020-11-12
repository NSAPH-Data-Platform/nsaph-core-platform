import psycopg2
import os
import re
#from re import Pattern
import sys
from configparser import ConfigParser
import csv


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

def index_method(c: str) -> (str,None):
    c = c.lower()
    for i in index_columns:
        if "*" in i:
            if regex(i).fullmatch(c):
                return index_columns[c]
        else:
            if i == c:
                return index_columns[c]
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
            _line = self.file_like_object.read(size, *args, **keyargs)
            line = _line
            while line[-self.l:] == self.sep:
                next_char = self.file_like_object.read(self.l)
                line += next_char
            line = self._replace_empty(line)
        else:
            _line = ""
            line = _line
        if self.remainder:
            line = self.remainder + line
            self.remainder = ""

        nl = line.count('\n')
        self.line_number += nl
        self.chars += len(line) - nl
        if (self.line_number - self.last_printed_line_number) > 1000000:
            print("Processed {:d}/{:d} lines/chars".format(self.line_number, self.chars))
            self.last_printed_line_number = self.line_number
        if len(line) > size:
            self.remainder = line[size - len(line):]
            return line[0:size]
        return line



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


def guess_types(rows: list, lines: list, columns: list) -> list:
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
            if date.fullmatch(v):
                t = "DATE"
            elif '"' in v2:
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
                msg = "Inconsistent type for column {:d} [{:s}]. "\
                    .format(c+1, columns[c])
                msg += "Up to line {:d}: {:s}, for line={:d}: {:s}. "\
                    .format(l-1, type, l, t)
                msg += "Value = {}".format(v)
                raise Exception(msg)
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
    types = guess_types(rows, lines, columns)
    for row in rows:
        for cell in row:
            if ',' in cell:
                has_commas = True
                break

    sql_columns = [c.replace('.', '_') for c in columns]
    col_spec = ["{} \t{}".format(sql_columns[i], types[i]) for i in range(0, len(columns))]
    ddl = "CREATE TABLE {}\n ({})".format(name, ",\n\t".join(col_spec))

    for c in sql_columns:
        m = index_method(c)
        if m:
            ddl += ";\nCREATE INDEX {table}_{column}_idx ON {table} USING {method} ({column})"\
                .format(table = name, column = c, method = m)

    print (ddl)

    if cursor:
        cursor.execute(ddl)
        if has_commas:
            print("The CSV file contains commas, copying manually.")
            copy_data(cursor, name, sql_columns, types, file_path)
        else:
            print("Copying data using system function.")
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
                elif types[i] in ["VARCHAR", "DATE", "TIMESTAMP", "TIME"]:
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
            print("\r" + str(counter))
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

