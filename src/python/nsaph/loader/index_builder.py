"""
This module supports building indices on domain tables
and monitoring the build progress
"""


import logging
import threading
import time
from datetime import datetime

from nsaph.loader import LoaderBase
from nsaph.loader.conf import IndexerConfig


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


def find_name(sql):
    x = sql.split(" ")
    for i in range(0, len(x)):
        if x[i] == "ON":
            return x[i-1]
    raise Exception(sql)


class IndexBuilder(LoaderBase):
    """
    Index Builder Class
    """

    def __init__(self, context: IndexerConfig = None):
        if not context:
            context = IndexerConfig(__doc__).instantiate()
        super().__init__(context)
        self.context: IndexerConfig = context

    def run(self):
        x = threading.Thread(target=self.execute)
        x.start()
        n = 0
        step = 100
        while x.is_alive():
            time.sleep(0.1)
            n += 1
            if (n % step) == 0:
                self.print_stat()
                if n > 100000:
                    step = 6000
                elif n > 10000:
                    step = 600
        x.join()

    def execute(self):
        domain = self.domain

        if self.context.table is not None:
            indices = domain.indices_by_table[domain.fqn(self.context.table)]
        else:
            indices = domain.indices
        print(indices)

        with self._connect() as connection:
            for index in indices:
                name = find_name(index)
                with (connection.cursor()) as cursor:
                    if self.context.reset:
                        sql = "DROP INDEX IF EXISTS {name}".format(name=name)
                        logging.info(str(datetime.now()) + ": " + sql)
                        cursor.execute(sql)
                    if self.context.incremental:
                        sql = index.replace(name, "IF NOT EXISTS " + name)
                    else:
                        sql = index
                    logging.info(str(datetime.now()) + ": " + sql)
                    cursor.execute(sql)

    def print_stat(self):
        with self._connect() as connection:
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
                logging.info(msg)
        return


if __name__ == '__main__':
    IndexBuilder().run()
