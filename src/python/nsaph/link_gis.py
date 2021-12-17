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

from nsaph import init_logging
from nsaph.data_model.model import Table
from nsaph.db import Connection


def add_gis_columns(table: Table, db: str = None, section: str = None):
    with Connection(filename=db, section=section) as connection:
        cur = connection.cursor()
        table.make_fips_column(cur)
        table.make_iso_column("state_name", cur, include="arithmetic_mean")
        connection.commit()


if __name__ == '__main__':
    init_logging()
    parser = argparse.ArgumentParser (description="Link EPA data to GIS info")
    parser.add_argument("--tdef", "-t",
                        help="Path to a table definition file for a table",
                        required=True)
    parser.add_argument("--db",
                        help="Path to a database connection parameters file",
                        default="database.ini",
                        required=False)
    parser.add_argument("--section",
                        help="Section in the database connection parameters file",
                        default="postgres",
                        required=False)

    args = parser.parse_args()

    table = Table(args.tdef, None)
    add_gis_columns (table, args.db, args.section)
