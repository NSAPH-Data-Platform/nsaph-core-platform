import json
import os
import re
import csv
import datetime
from nsaph.reader import CSVFileWrapper, name, fopen, SpecialValues


INDEX_REINDEX = "reindex"
INDEX_INCREMENTAL = "incremental"


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


def unquote(s: str) -> str:
    return s.strip().strip('"')


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
    def __init__(self, file_path: str, get_entry,
                 concurrent_indices: bool = False, data_file: str = None):
        if file_path.endswith("json"):
            self.open_entry_function = get_entry
            with open(file_path) as fp:
                j = json.load(fp)
            for a in j:
                setattr(self, a, j[a])
            if data_file:
                self.file_path = data_file
            return
        if concurrent_indices:
            self.index_option = "CONCURRENTLY"
        else:
            self.index_option = ""
        self.file_path = os.path.abspath(file_path)
        file_name = os.path.basename(file_path)
        name, ext = os.path.splitext(file_name)
        while ext not in [".csv", ""]:
            name, ext = os.path.splitext(name)
        self.table = name
        self.has_commas = False
        self.sql_columns = []
        self.csv_columns = []
        self.types = []
        self.open_entry_function = get_entry
        self.force_manual = False
        self.custom_columns = []
        self.create_table_ddl = None
        self.index_ddl = []

    def save(self, to_dir):
        f = os.path.join(to_dir, self.table + ".json")
        j = dict()
        for att in dir(self):
            if att[0] != '_':
                v = getattr(self, att)
                if callable(v):
                    continue
                j[att] = v

        with open (f, "w") as config:
            json.dump(j, config, indent=2)
        print("Saved table definition to: " + f)

    def fopen(self, source):
        return fopen(self.open_entry_function(source))

    def add_column(self, name, type, extraction_method):
        self.custom_columns.append(CustomColumn(name, type, extraction_method))
        self.force_manual = True

    def analyze(self, entry=None):
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
        for c in self.custom_columns:
            self.sql_columns.append(c.name)
            col_spec.append("{} \t{}".format(c.name, c.type))
        self.create_table_ddl = \
            "CREATE TABLE {}\n ({})".format(self.table, ",\n\t".join(col_spec))

        index_name_pattern = "{table}_{column}_idx"
        index_ddl_pattern = "CREATE INDEX {option} {name} ON {table} USING {method} ({column})"
        for c in self.sql_columns:
            m = index_method(c)
            if m:
                n = index_name_pattern.format(table = self.table, column = c)
                ddl = index_ddl_pattern \
                    .format(option=self.index_option,
                            name = n,
                            table=self.table,
                            column=c,
                            method = m)
                self.index_ddl.append((n, ddl))

    def create(self, cursor):
        cursor.execute(self.create_table_ddl)

    def build_indices(self, cursor, flag: str = None):
        for ddl in self.index_ddl:
            command = ddl[1]
            name = ddl[0]
            if flag == INDEX_REINDEX:
                sql = "DROP INDEX IF EXISTS {name}".format(name=name)
                print(str(datetime.datetime.now()) + ": " + sql)
                cursor.execute(sql)
            elif flag == INDEX_INCREMENTAL:
                command = command.replace(name, "IF NOT EXISTS " + name)
            elif flag and flag != "default":
                raise Exception("Invalid indexing flag: " + flag)
            print(str(datetime.datetime.now()) + ": " + command)
            cursor.execute(command)

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
                elif SpecialValues.is_untyped(v):
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
                    cursor.copy_from(csv_data, self.table, sep=',',
                                     null=SpecialValues.NA,
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
                    if SpecialValues.is_missing(row[i]):
                        row[i] = "NULL"
                    elif self.types[i] in ["VARCHAR", "DATE", "TIMESTAMP", "TIME"]:
                        row[i] = "'{}'".format(row[i].replace("'", "''"))

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
        print("{}: Processed {:d}/{} lines/chars [t={}({}), r={:5.1f}({:5.1f}) lines/sec]"
              .format(str(t), lines, c, str(dt1), str(dt0), r1, r0))
        return t


