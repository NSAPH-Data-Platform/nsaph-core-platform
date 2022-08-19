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

from deprecated.sphinx import deprecated

from nsaph import init_logging
from nsaph.data_model.model import Table
from nsaph.db import Connection
from nsaph_utils.utils.io_utils import get_entries, basename


@deprecated(
    reason="Use nsaph.loader.data_loader",
    version="0.2"
)


def create_table(table: Table, force: bool = False, db: str = None,
                 section: str = None):
    entries, open_function = get_entries(table.file_path)
    table.open_entry_function = open_function
    with Connection(filename=db, section=section) as connection:
        if force:
            try:
                table.drop(connection.cursor())
            except:
                connection.rollback()
        cur = connection.cursor()
        table.create(cur)
        for e in entries:
            print ("Adding: " + basename(e))
            table.add_data(cur, e)
        connection.commit()


if __name__ == '__main__':
    init_logging()
    parser = argparse.ArgumentParser (description="Create table and load data")
    parser.add_argument("--tdef", "-t",
                        help="Path to a table definition file for a table",
                        required=True)
    parser.add_argument("--data",
                        help="Path to a data file",
                        required=False)
    parser.add_argument("--force", action='store_true',
                        help="Force recreating table if it already exists")
    parser.add_argument("--db",
                        help="Path to a database connection parameters file",
                        default="database.ini",
                        required=False)
    parser.add_argument("--section",
                        help="Section in the database connection parameters file",
                        default="postgresql",
                        required=False)

    args = parser.parse_args()

    table = Table(args.tdef, None, data_file=args.data)
    create_table (table, args.force, args.db, args.section)
