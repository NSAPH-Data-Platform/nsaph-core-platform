"""
This module supports building indices on domain tables
and monitoring the build progress
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

from nsaph.loader import LoaderBase, CommonConfig
from nsaph_utils.utils.context import Argument, Cardinality

from nsaph.loader.monitor import DBActivityMonitor


def find_name(sql):
    x = sql.split(" ")
    for i in range(0, len(x)):
        if x[i] == "ON":
            return x[i-1]
    raise Exception(sql)


class IndexerConfig(CommonConfig):
    """
        Configurator class for index builder
    """

    _reset = Argument("reset",
        help = "Force rebuilding indices it/they already exist",
        type = bool,
        aliases=["r"],
        default = False,
        cardinality = Cardinality.single
    )

    _incremental = Argument("incremental",
        help = "Skip over existing indices",
        aliases=["i"],
        type = bool,
        default = False,
        cardinality = Cardinality.single
    )

    def __init__(self, doc):
        self.reset = None
        self.incremental = None
        super().__init__(IndexerConfig, doc)


class IndexBuilder(LoaderBase):
    """
    Index Builder Class
    """

    def __init__(self, context: IndexerConfig = None):
        if not context:
            context = IndexerConfig(__doc__).instantiate()
        super().__init__(context)
        self.context: IndexerConfig = context
        self.exception = None

    def run(self):
        self.execute_with_monitor(self.execute, on_monitor=self.print_stat)
        if self.exception is not None:
            raise self.exception

    def execute(self):
        try:
            self._execute()
        except BaseException as ex:
            logging.exception("Exception building indices")
            self.exception = ex
            raise

    def _execute(self):
        domain = self.domain

        if self.context.table is not None:
            indices = domain.indices_by_table[domain.fqn(self.context.table)]
        else:
            indices = domain.indices
        print(indices)

        if self.context.autocommit:
            for index in indices:
                with self._connect() as cnxn:
                    self.build(index, cnxn)
        else:
            with self._connect() as cnxn:
                for index in indices:
                    self.build(index, cnxn)
        logging.info("All indices have been built")

    def build(self, index, cnxn):
        name = find_name(index)
        fqn = self.domain.fqn(name)
        with (cnxn.cursor()) as cursor:
            if self.context.reset:
                sql = "DROP INDEX IF EXISTS {name}".format(name=fqn)
                logging.info(str(datetime.now()) + ": " + sql)
                cursor.execute(sql)
            if self.context.incremental:
                sql = index.replace(name, "IF NOT EXISTS " + name)
            else:
                sql = index
            logging.info(str(datetime.now()) + ": " + sql)
            cursor.execute(sql)
            logging.info(str(datetime.now()) + ": Index " +
                         name + " is ready.")

    def print_stat(self):
        for msg in self.monitor.get_indexing_progress():
            logging.info(msg)

    def drop(self, schema: str, table: str = None):
        with self._connect() as connection:
            self.drop_all(connection, schema, table)

    @classmethod
    def drop_all (cls, connection, schema: str, table: str = None, act = True):
        query = """
        SELECT 
            i.relname, 
            n.nspname,
            c.relname 
        FROM 
            "pg_catalog"."pg_index" as x
                JOIN pg_class as i ON  x.indexrelid = i.oid
                JOIN pg_class as c ON  x.indrelid = c.oid
                JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE nspname = '{}' 
        """.format(schema)
        if table is not None:
            query += " AND c.relname = '{}'".format(table)
        with (connection.cursor()) as cursor:
            cursor.execute(query)
            indices = [row[0] for row in cursor]
        logging.info("Found {:d} indices".format(len(indices)))
        with (connection.cursor()) as cursor:
            for index in indices:
                sql = "DROP INDEX {}.{}".format(schema, index)
                logging.info(sql)
                if act:
                    cursor.execute(sql)
        return


if __name__ == '__main__':
    IndexBuilder().run()
