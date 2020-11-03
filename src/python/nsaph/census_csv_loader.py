import psycopg2
import os
import re
import sys
from configparser import ConfigParser


index_columns = [
    "fips",
    "name",
    "year"
]


integer = re.compile("\d+")
float_number = re.compile("(\d*)\.(\d+)")


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


def guess_types(lines: list) -> list:
    m = len(lines)
    n = len(lines[0])
    types = []
    for c in range(0,n):
        type = None
        precision = 0
        scale = 0
        for l in range(0,m):
            v = lines[l][c].strip()
            if '"' in v:
                t = "VARCHAR"
            elif v in ['0', "NA"]:
                t = "0"
            else:
                f = float_number.match(v)
                if f:
                    t = "NUMERIC"
                    s = len(f.group(2))
                    p = len(f.group(1))
                    scale = max(scale, s)
                    precision = max(precision, p)
                elif integer.match(v):
                    t = "INT"
                else:
                    t = "VARCHAR"
            if t == "0":
                continue
            if type == "0":
                type = t
            elif type == "NUMERIC" and t == "INT":
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
    file_name = os.path.basename(file_path)
    name, _ = os.path.splitext(file_name)
    with open(file_path) as data:
        line = data.readline()
        columns = line.split(',')
        columns = [unquote(c) for c in columns]

        lines = []
        for i in range(0,10000):
            line = data.readline()
            if line:
                values = line.split(',')
            else:
                break
            lines.append(values)
        if not lines:
            raise Exception("No data in {}".format(file_path))
        types = guess_types(lines)

    col_spec = ["{} \t{}".format(columns[i], types[i]) for i in range(0, len(columns))]
    ddl = "CREATE TABLE {}\n ({})".format(name, ",\n\t".join(col_spec))

    for c in columns:
        if c.lower() in index_columns:
            ddl += ";\nCREATE INDEX {column}_idx ON {table} ({column})"\
                .format(table = name, column = c)

    print (ddl)

    if cursor:
        cursor.execute(ddl)
        with open(file_path) as data:
            data.readline()
            cursor.copy_from(data, name,sep=',', null="NA", columns=columns)



    return name


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

