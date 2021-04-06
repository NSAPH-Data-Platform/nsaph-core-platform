import logging
import sys

from nsaph.analyze import analyze
from nsaph.create import create_table
from nsaph.index import build_indices
from nsaph.db import Connection
from nsaph.model import INDEX_REINDEX


def ingest(args: list, force: bool = False):
    logging.getLogger().setLevel("DEBUG")
    Connection.default_section = "postgresql10"
    path = args[0]
    table = analyze (path)

    create_table(table, force)
    logging.info("Table created: " + table.table)

    logging.info("Building indices:")
    flag = INDEX_REINDEX if force else None
    build_indices(table, flag)
    print ("All DONE")




if __name__ == '__main__':
    #test_connection()
    ingest(sys.argv[1:], True)

