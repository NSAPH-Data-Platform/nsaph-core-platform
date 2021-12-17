"""
A utility that prints the statistics about
currently running indexing processes
"""
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

import datetime
from typing import List, Dict
from psycopg2.extras import RealDictCursor
from psycopg2.extensions import connection

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


ACTIVITY_QUERY = """
SELECT 
    "datname", 
    "pid", 
    "leader_pid", 
    "usename", 
    "application_name", 
    "client_addr", 
    "backend_start", 
    "xact_start",
    "backend_xid", 
    "query_start", 
    "state_change", 
    "wait_event_type", 
    "wait_event", 
    "state", 
    "backend_xmin", 
    "query"
FROM 
    "pg_catalog"."pg_stat_activity"

"""

ACTIVITY_BY_PID = ACTIVITY_QUERY + "WHERE pid = {pid} or leader_pid = {pid}"
ACTIVITY_BY_DB = ACTIVITY_QUERY + "WHERE datname = '{}'"


class DBActivityMonitor:
    def __init__(self, context: DBConnectionConfig = None):
        if not context:
            context = DBConnectionConfig(None, __doc__).instantiate()
        self.context = context
        self.connection = None

    def run(self):
        for msg in self.get_indexing_progress():
            print(msg)
        for msg in self.get_activity():
            print(msg)

    def _connect(self):
        self.connection = Connection(self.context.db,
                        self.context.connection,
                        silent=True,
                        app_name_postfix=".monitor")
        return self.connection.connect()

    def get_indexing_progress(self) -> List[str]:
        with self._connect() as cnxn:
            cursor = cnxn.cursor()
            version = cnxn.info.server_version
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

    def get_activity(self, pid: int = None) -> List[str]:
        msgs = []
        leaders: List[Dict] = []
        workers: List[Dict] = []
        with self._connect() as c:
            with c.cursor() as cursor:
                cursor.execute("SELECT now()")
                for row in cursor:
                    now = row[0]
                    break
            with c.cursor(cursor_factory=RealDictCursor) as cursor:
                if pid:
                    sql = ACTIVITY_BY_PID.format(pid = pid)
                else:
                    db = self.connection.parameters["database"]
                    sql = ACTIVITY_BY_DB.format(db)
                cursor.execute(sql)
                for row in cursor:
                    if row["leader_pid"]:
                        workers.append(row.copy())
                    else:
                        leaders.append(row.copy())
        for l in [leaders, workers]:
            for p in l:
                activity = Activity(p, now)
                msgs.append(str(activity))
        return msgs


class Activity:
    def __init__(self, activity: Dict, now: datetime):
        self.now = now
        self.database = activity["datname"]
        self.pid = int(activity["pid"])
        self.leader = int(activity["leader_pid"]) if activity["leader_pid"] else None
        self.app = activity["application_name"]
        self.state = activity["state"] if activity["state"] else "wait"
        if self.state == "wait":
            self.wait = "{}:{}".format(
                activity["wait_event_type"], activity["wait_event"]
            )
        else:
            self.wait = ""
        self.xid = activity["backend_xid"]
        self.in_transaction = True if self.xid else False
        self.query = activity["query"]
        self.start = activity["backend_start"]
        self.last = activity["state_change"]
        if self.in_transaction:
            self.xact_start = activity["xact_start"]
        else:
            self.xact_start = None
        if self.query:
            self.query_start = activity["query_start"]
        else:
            self.query_start = None

    def __str__(self):
        msg = "{:d}".format(self.pid)
        if self.leader:
            msg += " <= {}".format(self.leader)
        msg += " {}".format(self.state)
        if self.app:
            if self.database:
                app = " [{}:{}]".format(self.app, self.database)
            else:
                app = " [{}]".format(self.app)
            msg += app
        msg += ". Started at {}, running for {}".format(
            str(self.start),
            str(self.now - self.start)
        )
        if self.in_transaction:
            msg += ". Transaction {} started at {}, running {}".format(
                self.xid, str(self.xact_start),
                str(self.now - self.xact_start)
            )
        if self.wait:
            msg += ". Waiting from {} for {}".format(
                str(self.last),
                str(self.now - self.last)
            )
        if self.query:
            msg += ". Executing: {}".format(self.query[:32])

        return msg


if __name__ == '__main__':
    DBActivityMonitor().run()