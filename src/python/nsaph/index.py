import argparse
import threading
import time

from nsaph.db import Connection
from nsaph.model import Table, INDEX_REINDEX, INDEX_INCREMENTAL

SQL12 = """
    SELECT 
      now()::TIME(0),
      p.command, 
      a.query, 
      p.phase, 
      p.blocks_total, 
      p.blocks_done, 
      p.tuples_total, 
      p.tuples_done,
      p.pid
    FROM pg_stat_progress_create_index p 
    JOIN pg_stat_activity a ON p.pid = a.pid
"""

SQL11 = """
    SELECT 
      now()::TIME(0), 
      a.query,
      a.state
    FROM pg_stat_activity a
    WHERE a.query LIKE 'CREATE%INDEX%' 
"""

def index(table, cursor, flag):
    table.build_indices(cursor, flag)


def print_stat(db: str = None, section: str = None):
    with Connection(db, section, silent=True) as connection:
        cursor = connection.cursor()
        version = connection.info.server_version
        if version > 120000:
            sql = SQL12
        else:
            sql = SQL11
        cursor.execute(sql)
        for row in cursor:
            if version > 120000:
                t = row[0]
                c = row[1]
                q = row[2][len(c):].strip().split(" ")
                if q:
                    n = "None"
                    for x in q:
                        if x not in ["IF", "NOT", "EXISTS"]:
                            n = x
                            break
                else:
                    n = "?"
                p = row[3]
                b = row[5] * 100.0 / row[4] if row[4] else 0
                tp = row[7] * 100.0 / row[6] if row[6] else 0
                pid = row[8]
                msg = "[{}] {}: {}. Blocks: {:2.0f}%, Tuples: {:2.0f}%. PID = {:d}"\
                    .format(str(t), p, n, b, tp, pid)
            else:
                t = row[0]
                q = row[2]
                s = row[2]
                msg = "[{}] {}: {}".format(t, s, q)
            print(msg)


def build_indices(table: Table, flag: str, db: str = None,
                  section: str = None):
    with Connection(db, section) as connection:
        connection.autocommit = True
        cursor = connection.cursor()
        x = threading.Thread(target=index, args=(table, cursor, flag))
        x.start()
        n = 0
        step = 100
        while (x.is_alive()):
            time.sleep(0.1)
            n += 1
            if (n % step) == 0:
                print_stat(db, section)
                if n > 100000:
                    step = 6000
                elif n > 10000:
                    step = 600
        x.join()


if __name__ == '__main__':
    parser = argparse.ArgumentParser (description="Build indices")
    parser.add_argument("--tdef", "-t",
                        help="Path to a config file for a table",
                        required=True)
    parser.add_argument("--force", action='store_true',
                        help="Force reindexing if index already exists")
    parser.add_argument("--incremental", "-i", action='store_true',
                        help="Force reindexing if index already exists")
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
    flag = None
    if args.force:
        flag = INDEX_REINDEX
    elif args.incremental:
        flag = INDEX_INCREMENTAL
    build_indices(table, flag, args.db, args.section)
