import argparse
import os

from nsaph import init_logging
from nsaph.ds import create_datasource_def
from nsaph.reader import get_entries, get_readme
from nsaph.model import Table


def analyze(path: str, columns = None, column_map = None):
    entries, open_function = get_entries(path)
    table = Table(path, open_function, column_name_replacement=column_map)
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

    table = analyze(args.source, args.column, column_map)

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
