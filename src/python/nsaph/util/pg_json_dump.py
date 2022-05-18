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

import decimal
import gzip
import json
import logging
import os
import sys
from argparse import ArgumentParser
from contextlib import contextmanager
from numbers import Number
from typing import Dict, List
from psycopg2.extras import RealDictCursor
from psycopg2.extensions import connection

from nsaph import init_logging
from nsaph.util.resources import get_resources, get_resource_dir, name2path
from nsaph.db import Connection
from nsaph.fips import fips_dict


def fqn(conn: connection, table: str) -> str:
    if '.' not in table:
        sql = """
        SELECT
            table_schema
        FROM
            information_schema.tables
        WHERE table_name = '{}'
        """.format(table)
        with conn.cursor() as cursor:
            cursor.execute(sql)
            rows = [row for row in cursor]
            if len(rows) < 1:
                raise Exception("Table {} not found".format(table))
            elif len(rows) > 1:
                raise Exception("Multiple tables {}: {}".format(table, ', '.join(rows)))
            schema = rows[0][0]
            table = "{schema}.{table}".format(schema=schema, table=table)
    return table


def quote(x):
    if isinstance(x, Number):
        return str(x)
    return "'{}'".format(str(x).replace("'", "''"))


def flush(cursor, table, rows):
    if not rows:
        return
    columns = [column for column in rows[0]]
    sql = "INSERT INTO {} ({}) VALUES \n\t".format(table, ','.join(columns))
    values = [
        "({})".format(','.join([quote(row[column]) for column in columns]))
        for row in rows
    ]
    sql += ",\n\t".join(values)
    cursor.execute(sql)


@contextmanager
def result_set(conn: connection, table: str):
    sql = "SELECT * FROM " + table
    with conn.cursor(cursor_factory=RealDictCursor) as cursor:
        cursor.execute(sql)
        yield cursor


def dump(conn: connection, table: str, fd, corrector = None):
    with result_set(conn, table) as rs:
        for row in rs:
            if corrector:
                corrector(row)
            for key in row:
                if isinstance(row[key], decimal.Decimal):
                    row[key] = float(row[key])
            print(json.dumps(row), file=fd)


def export(conn: connection, table: str):
    if table == "us_states":
        corrector = add_state_fips
    else:
        corrector = None
    table = fqn(conn, table)
    resource = os.path.join(
        get_resource_dir(), name2path(table)
    )
    data_path = resource + ".json.gz"
    with gzip.open(data_path, "wt") as fd:
        dump(conn, table, fd, corrector)
    logging.info("Exported table {} to {}.".format(table, data_path))


def ensure(conn: connection, table: str):
    if '.' not in table:
        table = "public." + table
    SQL = """
    SELECT EXISTS (
        SELECT FROM information_schema.tables
        WHERE table_name = '{}'
    )
    """
    sql = SQL.format(table.split('.')[-1])
    with conn.cursor() as cursor:
        logging.info(sql)
        cursor.execute(sql)
        for row in cursor:
            if row[0]:
                logging.info("True")
                return 
    import_table(conn=conn, table=table, replace=False)


def import_table(conn: connection, table: str, replace = True):
    if '.' not in table:
        table = "public." + table
    if replace:
        sql = "DROP TABLE IF EXISTS {} CASCADE".format(table)
        with conn.cursor() as cursor:
            logging.info(sql)
            cursor.execute(sql)
    resource = get_resources(table)
    ddl_path = resource['ddl']
    with open(ddl_path) as f:
        ddl = ''.join([
            line for line in f
        ])
    data_path = resource["json.gz"]
    n = 0
    with conn.cursor() as cursor:
        logging.info(ddl)
        cursor.execute(ddl)
        with gzip.open(data_path, "rt") as fd:
            rows = []
            for line in fd:
                rows.append(json.loads(line))
                n += 1
                if len(rows) > 100:
                    flush(cursor, table, rows)
                    rows.clear()
                if (n % 10000) == 0:
                    print('*', end="")
            if len(rows) > 0:
                flush(cursor, table, rows)

    logging.info("Inserted: {:d} records into {}".format(n, table))
    return


def add_state_fips(row: Dict):
    if row.get("fips2"):
        return
    fips= fips_dict[row["state_id"]]
    row["fips2"] = "{:02d}".format(fips)


def show(conn: connection, table: str):
    if table == "us_states":
        corrector = add_state_fips
    else:
        corrector = None
    dump(conn, table, sys.stdout, corrector)


if __name__ == '__main__':
    init_logging()
    parser = ArgumentParser (description="Import/Export resources")
    parser.add_argument("--table", "-t",
                        help="Name of the table to load data into",
                        required=False)
    parser.add_argument("--db",
                        help="Path to a database connection parameters file",
                        default="database.ini",
                        required=False)
    parser.add_argument("--connection",
                        help="Section in the database connection parameters file",
                        default="nsaph2",
                        required=False)
    parser.add_argument("--action", help="Action to perform", required=True)

    arguments = parser.parse_args()
    action = locals()[arguments.action]
    with Connection(arguments.db, arguments.connection) as db:
        action(db, arguments.table)
        db.commit()
