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
import logging
import threading
import time
from typing import List, Dict, Optional, Callable
from psycopg2.extras import RealDictCursor
from psycopg2.extensions import connection

from nsaph.db import Connection
from nsaph.loader.common import DBConnectionConfig
from nsaph_utils.utils.context import Argument, Cardinality

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

LOCK_QUERY = """
SELECT blocked_locks.pid     AS blocked_pid,
         blocked_activity.usename  AS blocked_user,
         blocking_locks.pid     AS blocking_pid,
         blocking_activity.usename AS blocking_user,
         blocked_activity.application_name AS blocked_application,
         blocking_activity.application_name AS blocking_application
FROM  pg_catalog.pg_locks         blocked_locks
    JOIN pg_catalog.pg_stat_activity blocked_activity  
        ON blocked_activity.pid = blocked_locks.pid
    JOIN pg_catalog.pg_locks         blocking_locks 
        ON blocking_locks.locktype = blocked_locks.locktype
            AND blocking_locks.DATABASE IS NOT DISTINCT FROM blocked_locks.DATABASE
            AND blocking_locks.relation IS NOT DISTINCT FROM blocked_locks.relation
            AND blocking_locks.page IS NOT DISTINCT FROM blocked_locks.page
            AND blocking_locks.tuple IS NOT DISTINCT FROM blocked_locks.tuple
            AND blocking_locks.virtualxid IS NOT DISTINCT FROM blocked_locks.virtualxid
            AND blocking_locks.transactionid IS NOT DISTINCT FROM blocked_locks.transactionid
            AND blocking_locks.classid IS NOT DISTINCT FROM blocked_locks.classid
            AND blocking_locks.objid IS NOT DISTINCT FROM blocked_locks.objid
            AND blocking_locks.objsubid IS NOT DISTINCT FROM blocked_locks.objsubid
            AND blocking_locks.pid != blocked_locks.pid
    JOIN pg_catalog.pg_stat_activity blocking_activity 
        ON blocking_activity.pid = blocking_locks.pid
WHERE 
    NOT blocked_locks.GRANTED;
"""


ACTIVITY_BY_PID = ACTIVITY_QUERY + "WHERE pid = {pid} or leader_pid = {pid}"
ACTIVITY_BY_DB = ACTIVITY_QUERY + "WHERE datname = '{}'"


class Lock:
    def __init__(self, data: List):
        self.blocked_pid = data[0]
        self.blocking_pid = data[2]
        self.blocked_user = data[1]
        self.blocking_user = data[3]
        self.blocked_app = data[4]
        self.blocking_app = data[5]

    def __str__(self) -> str:
        msg = "{}[{:d}] is blocked by {}@{}[{}]".format(
            self.blocked_app, self.blocked_pid,
            self.blocking_user, self.blocking_app, self.blocking_pid
        )
        return super().__str__()



class DBMonitorConfig(DBConnectionConfig):
    _pid = Argument("pid",
        help = "Display monitoring information only for selected process ids",
        type = int,
        required = False,
        aliases = ["p"],
        default = None,
        cardinality = Cardinality.multiple
    )

    _state = Argument("state",
        help = "Show only processes in the given state",
        type = str,
        required = False,
        default=None,
        cardinality = Cardinality.single
    )

    def __init__(self, subclass, doc):
        self.pid:List[int] = []
        ''' process id list '''

        self.state:Optional[str] = None
        ''' Display only processes in the given state '''

        if subclass is None:
            super().__init__(DBMonitorConfig, doc)
        else:
            super().__init__(subclass, doc)
            self._attrs += [
                attr[1:] for attr in DBMonitorConfig.__dict__
                if attr[0] == '_' and attr[1] != '_'
            ]

class DBActivityMonitor:
    @classmethod
    def get_instance (cls, context: DBConnectionConfig) -> DBMonitorConfig:
        if isinstance(context, DBMonitorConfig):
            return context
        if isinstance(context, DBConnectionConfig):
            obj = DBMonitorConfig(None, __doc__)
            obj.connection = context.connection
            obj.db = context.db
            obj.verbose = context.verbose
            return obj
        raise TypeError(
            "{} cannot be cast to DBMonitorConfig"
            .format(str(context))
        )

    def __init__(self, context: DBConnectionConfig = None):
        if context:
            context = self.get_instance(context)
        else:
            context = DBMonitorConfig(None, __doc__).instantiate()
        self.context = context
        self.connection = None
        self.blocks: Dict[int,Lock] = dict()

    def run(self):
        self.get_blocks()
        for lock in self.blocks.values():
            print(str(lock))
        for msg in self.get_indexing_progress():
            print(msg)
        if self.context.pid:
            for pid in self.context.pid:
                for msg in self.get_activity(pid):
                    print(msg)
        else:
            for msg in self.get_activity():
                print(msg)

    def _connect(self):
        self.connection = Connection(self.context.db,
                        self.context.connection,
                        silent=True,
                        app_name_postfix=".monitor")
        return self.connection.connect()

    def get_blocks(self):
        with self._connect() as cnxn:
            with cnxn.cursor() as cursor:
                cursor.execute(LOCK_QUERY)
                for row in cursor:
                    lock = Lock(row)
                    self.blocks[lock.blocked_pid] = lock
        return

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
                if (self.context.verbose):
                    print(sql)
                cursor.execute(sql)
                for row in cursor:
                    if row["leader_pid"]:
                        workers.append(row.copy())
                    else:
                        leaders.append(row.copy())
        for l in [leaders, workers]:
            for p in l:
                if self.context.state is not None:
                    if p["state"] != self.context.state:
                        continue
                if self.context.verbose:
                    activity = Activity(p, self.blocks, now, -1)
                else:
                    activity = Activity(p, self.blocks, now)
                msgs.append(str(activity))
        return msgs

    @staticmethod
    def execute(what: Callable, on_monitor: Callable):
        x = threading.Thread(target=what)
        x.start()
        n = 0
        step = 100
        while x.is_alive():
            time.sleep(0.1)
            n += 1
            if (n % step) == 0:
                on_monitor()
                if n > 100000:
                    step = 6000
                elif n > 10000:
                    step = 600
        x.join()

    def log_activity(self, pid: int):
        activity = self.get_activity(pid)
        for msg in activity:
            logging.info(msg)
        return


class Activity:
    def __init__(self, activity: Dict, known_blocks, now: datetime, msg_len=32):
        self.now = now
        self.msg_len = msg_len
        self.database = activity["datname"]
        self.pid = int(activity["pid"])
        self.leader = int(activity["leader_pid"]) if activity["leader_pid"] else None
        self.app = activity["application_name"]
        self.state = activity["state"] if activity["state"] else "wait"
        self.blocked_by = ""
        if self.state == "wait" or (
            self.state == "active"
            and activity["wait_event_type"]
            and activity["wait_event"]
        ):
            self.wait = "{} waiting for {}".format(
                activity["wait_event"], activity["wait_event_type"]
            )
            if self.pid in known_blocks:
                self.blocked_by = " [{}]".format(known_blocks[self.pid])
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
            msg += ". {} from {} for {}".format(
                self.wait.capitalize(),
                str(self.last),
                str(self.now - self.last)
            )
        if self.blocked_by:
            msg += self.blocked_by
        if self.query:
            if self.msg_len < 0:
                msg += ". Executing: \n{}".format(self.query)
            else:
                msg += ". Executing: {}".format(self.query[:self.msg_len])

        return msg


if __name__ == '__main__':
    DBActivityMonitor().run()