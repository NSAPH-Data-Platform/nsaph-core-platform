import sys

from nsaph.analyze import analyze
from nsaph.create import create_table
from nsaph.index import build_indices
from nsaph.db import Connection


def ingest(args: list, force: bool = False):
    Connection.default_section = "postgresql10"
    path = args[0]
    table = analyze (path)

    create_table(table, force)
    print("Table created: " + table.table)

    print("Building indices:")
    build_indices(table, force)
    print ("All DONE")




if __name__ == '__main__':
    #test_connection()
    ingest(sys.argv[1:], True)

