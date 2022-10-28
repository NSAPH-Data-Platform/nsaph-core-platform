"""
Utility to execute SQL statement or statements taken from
command line arguments
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
from nsaph.db import Connection


def execute(args):
    sql = ' '.join(args.sql)
    print("Executing: " + sql)
    with Connection(args.db, args.connection) as cnxn:
        with cnxn.cursor() as cursor:
            cursor.execute(sql)
        cnxn.commit()


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
    parser.add_argument(dest="sql",
                        nargs='+',
                        help="SQL statement(s)")

    return parser.parse_args()


if __name__ == '__main__':
    arguments = parse_args()
    execute(arguments)

