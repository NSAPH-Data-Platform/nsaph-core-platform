"""
This module executes VACUUM ANALYZE command
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

import logging
from datetime import datetime

from nsaph.loader import LoaderBase
from nsaph.loader.common import CommonConfig
from nsaph.loader.monitor import DBActivityMonitor


def find_name(sql):
    x = sql.split(" ")
    for i in range(0, len(x)):
        if x[i] == "ON":
            return x[i-1]
    raise Exception(sql)


class Vacuum(LoaderBase):
    """
    PostgreSQL Vacuum Class
    """

    def __init__(self, context: CommonConfig = None):
        if not context:
            context = CommonConfig(None, __doc__).instantiate()
        super().__init__(context)
        context.autocommit = None
        self.context: CommonConfig = context

    def run(self):
        domain = self.domain

        if self.context.table is not None:
            tables = [domain.fqn(self.context.table)]
        else:
            tables = [t for t in domain.ddl_by_table]

        connection = self._connect()
        # can not use context manager (with) for VACUUM
        connection.set_isolation_level(0)
        for table in tables:
            with (connection.cursor()) as cursor:
                sql = "VACUUM (VERBOSE, PARALLEL 6, ANALYZE) {};".format(
                    table
                )
                logging.info(str(datetime.now()) + ": " + sql)
                self.execute_with_monitor(lambda: cursor.execute(sql),
                                          connxn=connection)
                if self.exception is not None:
                    raise self.exception
                logging.info("Done")

    def log_activity(self, connxn):
        activity = self.monitor.get_activity(connxn)
        for msg in activity:
            logging.info(msg)


if __name__ == '__main__':
    Vacuum().run()
