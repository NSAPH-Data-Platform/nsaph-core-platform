"""
Initializes Database with core tables, functions and procedures
"""

#  Copyright (c) 2022. Harvard University
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
import os
from argparse import ArgumentParser
from pathlib import Path

from nsaph.util.pg_json_dump import ensure

from nsaph.db import Connection


def load_data(args):
    with Connection(args.db, args.connection) as db:
        print("Loading us_states")
        ensure(db, "us_states")
        print("Loading us_iso")
        ensure(db, "us_iso")
        db.commit()
        print("Loading hud_zip2fips")
        ensure(db, "hud_zip2fips")
        db.commit()
    return


def execute(args, sql_file: str):
    print("Executing: " + sql_file)
    with open(sql_file) as reader:
        lines = [line for line in reader]
    sql = ''.join(lines)
    with Connection(args.db, args.connection) as cnxn:
        with cnxn.cursor() as cursor:
            cursor.execute(sql)
        cnxn.commit()


def get_sql_dir(me: str) -> str:
    ppp = Path(me).parts
    n = len(ppp) - 1
    while n > 0:
        if ppp[n] == "python":
            break
        n -= 1
    if n < 1:
        raise SystemError("Cannot locate source code root")
    root = os.path.join(*(ppp[:n]))
    return os.path.join(root, "sql")


def init_core(args):
    sdir = get_sql_dir(__file__)
    load_data(args)
    execute(args, os.path.join(sdir, "utils.sql"))
    execute(args, os.path.join(sdir, "zip2fips.sql"))


def parse_args():
    parser = ArgumentParser (description="Init database resources")
    parser.add_argument("--db",
                        help="Path to a database connection parameters file",
                        default="database.ini",
                        required=True)
    parser.add_argument("--connection",
                        help="Section in the database connection parameters file",
                        default="nsaph2",
                        required=True)

    return parser.parse_args()


if __name__ == '__main__':
    arguments = parse_args()
    init_core(arguments)

