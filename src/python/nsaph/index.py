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

import argparse
import threading
import time

from nsaph import init_logging
from nsaph.db import Connection
from nsaph.data_model.model import Table, INDEX_REINDEX, INDEX_INCREMENTAL
from nsaph.loader.index_builder import IndexBuilder, IndexerConfig


def index(table, cursor, flag):
    table.build_indices(cursor, flag)


def build_indices(table: Table, flag: str, db: str = None,
                  section: str = None):
    with Connection(db, section) as connection:
        connection.autocommit = True
        cursor = connection.cursor()
        x = threading.Thread(target=index, args=(table, cursor, flag))
        x.start()
        n = 0
        step = 100
        config = IndexerConfig("")
        config.db = db
        config.connection = section
        index_builder = IndexBuilder(config)
        while (x.is_alive()):
            time.sleep(0.1)
            n += 1
            if (n % step) == 0:
                index_builder.print_stat()
                if n > 100000:
                    step = 6000
                elif n > 10000:
                    step = 600
        x.join()


if __name__ == '__main__':
    init_logging()
    parser = argparse.ArgumentParser (description="Build or drop indices")
    parser.add_argument("--tdef", "-t",
                        help="Path to a config file for a table",
                        required=True)
    parser.add_argument("--force", action='store_true',
                        help="Force reindexing if index already exists")
    parser.add_argument("--incremental", "-i", action='store_true',
                        help="Skip over indices that already exist")
    parser.add_argument("--db",
                        help="Path to a database connection parameters file",
                        default="database.ini",
                        required=False)
    parser.add_argument("--section",
                        help="Section in the database connection parameters file",
                        default="postgresql",
                        required=False)

    args = parser.parse_args()

    table = Table(args.tdef, None)
    flag = None
    if args.force:
        flag = INDEX_REINDEX
    elif args.incremental:
        flag = INDEX_INCREMENTAL
    build_indices(table, flag, args.db, args.section)
