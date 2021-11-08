"""
A utility that prints the statistics about
currently running indexing processes
"""
from typing import List

from nsaph.db import Connection
from nsaph.loader.common import DBConnectionConfig


INDEX_MON_SQL12 = """
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

INDEX_MON_SQL11 = """
    SELECT 
      now()::TIME(0), 
      a.query,
      a.state
    FROM pg_stat_activity a
    WHERE a.query LIKE 'CREATE%INDEX%' 
"""




class DBActivityMonitor:
    def __init__(self, context: DBConnectionConfig = None):
        if not context:
            context = DBConnectionConfig(DBActivityMonitor, __doc__).instantiate()
        self.context = context

    def run(self):
        for msg in self.get_indexing_progress():
            print(msg)

    def get_indexing_progress(self) -> List[str]:
        with Connection(self.context.db,
                        self.context.connection,
                        silent=True).connect() as connection:
            cursor = connection.cursor()
            version = connection.info.server_version
            if version > 120000:
                sql = INDEX_MON_SQL12
            else:
                sql = INDEX_MON_SQL11
            cursor.execute(sql)
            msgs = []
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
                    msgs.append("[{}] {}: {}. Blocks: {:2.0f}%, Tuples: {:2.0f}%. PID = {:d}"
                        .format(str(t), p, n, b, tp, pid))
                else:
                    t = row[0]
                    q = row[2]
                    s = row[2]
                    msgs.append("[{}] {}: {}".format(t, s, q))
        return msgs


if __name__ == '__main__':
    DBActivityMonitor().run()