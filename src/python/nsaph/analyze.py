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
import os

from nsaph import init_logging
from nsaph.ds import create_datasource_def
from nsaph.reader import get_entries, get_readme
from nsaph.data_model.model import Table


def analyze(path: str, metadata_path: str=None, columns=None, column_map=None):
    entries, open_function = get_entries(path)
    table = Table(data_file=path, get_entry=open_function,
                  metadata_file=metadata_path,
                  column_name_replacement=column_map)
    table.analyze(entries[0])
    if columns:
        for c in columns:
            x = c.split(':')
            table.add_column(x[0], x[1], int(x[2]))
    return table


if __name__ == '__main__':
    init_logging()
    parser = argparse.ArgumentParser\
        (description="Analyze file or directory and prepare import")
    parser.add_argument("--source", "-s", help="Path to a file or directory",
                        required=True)
    parser.add_argument("--metadata", "-m",
                        default="",
                        help="Optional file with explicit columns metadata")
    parser.add_argument("--column", "-c", help="Additional columns", nargs="*")
    parser.add_argument("--outdir", help="Output directory")
    parser.add_argument("--readme", help="Readme.md file [optional]")
    parser.add_argument("--map", help="Mapping for column names: \"csv_name:sql_name\"", nargs="*")

    args = parser.parse_args()

    column_map = None
    if args.map:
        column_map = dict()
        for e in args.map:
            m = e.split(':')
            column_map[m[0]] = m[1]

    table = analyze(args.source, args.metadata, args.column, column_map)

    if args.outdir:
        d = os.path.abspath(args.outdir)
    else:
        d = os.path.dirname(os.path.abspath(table.file_path))
        pd, cd = os.path.split(d)
        if cd == "data":
            d = os.path.join(pd, "config")
            if not os.path.isdir(d):
                os.mkdir(d)
    table.save(d)

    if args.readme:
        readme = args.readme
    else:
        readme = get_readme(table.file_path)
    create_datasource_def(table, readme, d)
