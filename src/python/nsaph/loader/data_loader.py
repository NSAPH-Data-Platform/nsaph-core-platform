"""
Domain Data Loader

Provides Command line interface for loading data from a
single or a set of column-formatted files
into NSAPH PostgreSQL Database.

Input (aka source) files can be either in FST or in CSV format.

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
import os
import fnmatch

from nsaph.loader.index_builder import IndexBuilder
from psycopg2.extensions import connection

from typing import List, Tuple, Callable, Any

from nsaph import ORIGINAL_FILE_COLUMN
from nsaph.data_model.inserter import Inserter
from nsaph.data_model.utils import DataReader, entry_to_path
from nsaph.loader import LoaderBase
from nsaph.loader.loader_config import LoaderConfig, Parallelization
from nsaph_utils.utils.io_utils import get_entries, is_dir, sizeof_fmt


class DataLoader(LoaderBase):
    """
    Class for data loader
    """

    def __init__(self, context: LoaderConfig = None):
        if not context:
            context = LoaderConfig(__doc__).instantiate()
        super().__init__(context)
        self.context: LoaderConfig = context
        self.page = context.page
        self.log_step = context.log
        self._connections = None
        self.csv_delimiter = None
        if self.context.incremental and self.context.autocommit:
            raise ValueError("Incompatible arguments: autocommit is "
                             + "incompatible with incremental loading")
        if self.context.drop:
            if self.table:
                raise Exception("Drop is incompatible with other arguments")
        self.set_table()
        return

    def set_table(self, table: str = None):
        if table is not None:
            self.table = table
        if self.table:
            nc = len(self.domain.list_columns(self.table))
            if self.domain.has_hard_linked_children(self.table) or nc > 20:
                if self.page is None:
                    self.page = 100
                if self.log_step is None:
                    self.log_step = 10000
            else:
                if self.page is None:
                    self.page = 1000
                if self.log_step is None:
                    self.log_step = 1000000

    def print_ddl(self):
        for ddl in self.domain.ddl:
            print(ddl)
        for ddl in self.domain.indices:
            print(ddl)

    def print_table_ddl(self, table: str):
        fqn = self.domain.fqn(table)
        for ddl in self.domain.ddl_by_table[fqn]:
            print(ddl)
        for ddl in self.domain.indices_by_table[fqn]:
            print(ddl)

    def is_parallel(self) -> bool:
        if self.context.threads < 2:
            return False
        if self.context.parallelization == Parallelization.none:
            return False
        return True

    def get_connections(self) -> List[connection]:
        if self._connections is None:
            if self.is_parallel():
                self._connections = [
                    self._connect() for _ in range(self.context.threads)
                ]
            else:
                self._connections = [self._connect()]
        return self._connections

    def get_connection(self):
        return self.get_connections()[0]

    def get_files(self) -> List[Tuple[Any,Callable]]:
        objects = []
        for path in self.context.data:
            if not is_dir(path):
                objects.append(path)
                continue
            if self.context.pattern:
                ptn = "using pattern {}".format(self.context.pattern)
            else:
                ptn = "(all entries)"
            logging.info(
                "Looking for relevant entries in {} {}.".
                format(path, ptn)
            )
            entries, f = get_entries(path)
            if not self.context.pattern:
                objects += [(e,f) for e in entries]
                continue
            for e in entries:
                if isinstance(e, str):
                    name = e
                else:
                    name = e.name
                for pattern in self.context.pattern:
                    if fnmatch.fnmatch(name, pattern):
                        objects.append((e, f))
                        break
        if not objects:
            logging.warning("WARNING: no entries have been found")
        return objects

    def has_been_ingested(self, file:str, table):
        tfqn = self.domain.fqn(table)
        sql = "SELECT 1 FROM {} WHERE {} = '{}' LIMIT 1"\
            .format(tfqn, ORIGINAL_FILE_COLUMN, file)
        logging.debug(sql)
        with self.get_connection().cursor() as cursor:
            cursor.execute(sql)
            exists = len([r for r in cursor]) > 0
        return exists

    def reset(self):
        if not self.context.reset:
            return
        try:
            with self._connect() as connxn:
                if not self.context.sloppy:
                    tables = self.domain.drop(self.table, connxn)
                    for t in tables:
                        IndexBuilder.drop_all(connxn, self.domain.schema, t)
                self.execute_with_monitor(
                    lambda: self.domain.create(connxn, [self.table]),
                    connxn=connxn
                )
        except:
            logging.exception("Exception resetting table {}".format(self.table))
            raise

    def drop(self):
        schema = self.domain.schema
        with self._connect() as connxn:
            with (connxn.cursor()) as cursor:
                sql = "DROP SCHEMA IF EXISTS {} CASCADE".format(schema)
                print(sql)
                cursor.execute(sql)
            IndexBuilder.drop_all(connxn, schema)

    def run(self):
        if self.context.drop:
            self.drop()
            return
        if not self.context.table:
            self.print_ddl()
            return
        acted = False
        if self.context.reset:
            if self.context.dryrun:
                self.print_table_ddl(self.context.table)
            else:
                self.reset()
            acted = True
        if self.context.data:
            self.load()
            acted = True
        if not acted:
            raise ValueError("No action has been specified")
        return

    def commit(self):
        if not self.context.autocommit and self._connections:
            for cxn in self._connections:
                cxn.commit()
        return

    def rollback(self):
        for cxn in self._connections:
            cxn.rollback()
        return

    def close(self):
        if self._connections is None:
            return
        for cxn in self._connections:
            cxn.close()
        self._connections = None
        return

    def load(self):
        try:
            logging.info("Processing: " + '; '.join(self.context.data))
            for entry in self.get_files():
                try:
                    if self.context.incremental:
                        ff = os.path.basename(entry_to_path(entry))
                        logging.info("Checking if {} has been already ingested.".format(ff))
                        exists = self.has_been_ingested(ff, self.context.table)
                        if exists:
                            logging.warning("Skipping already imported file " + ff)
                            continue
                    logging.info("Importing: " + entry_to_path(entry))
                    self.import_data_from_file(data_file=entry)
                    if self.context.incremental:
                        self.commit()
                        logging.info("Committed: " + entry_to_path(entry))
                except Exception as x:
                    logging.exception("Exception: " + entry_to_path(entry))
                    if self.context.incremental:
                        self.rollback()
                        logging.info("Rolled back and skipped: " + entry_to_path(entry))
                    else:
                        raise x
            self.commit()
        finally:
            self.close()

    def import_data_from_file(self, data_file):
        table = self.table
        buffer = self.context.buffer
        limit = self.context.limit
        connections = self.get_connections()
        if self.domain.has("quoting") or self.domain.has("header"):
            q = self.domain.get("quoting")
            h = self.domain.get("header")
        else:
            q = None
            h = None
        domain_columns = self.domain.list_source_columns(table)
        with DataReader(data_file,
                        buffer_size=buffer,
                        quoting=q,
                        has_header=h,
                        columns=domain_columns,
                        delimiter=self.csv_delimiter) as reader:
            if reader.count is not None or reader.size is not None:
                logging.info("File size: {}; Row count: {:,}".format(
                    sizeof_fmt(reader.size),
                    reader.count if reader.count is not None else -1)
                )
            inserter = Inserter(
                self.domain,
                table,
                reader,
                connections,
                page_size=self.page
            )
            inserter.import_file(limit=limit, log_step=self.log_step)
        return


if __name__ == '__main__':
    loader = DataLoader()
    loader.run()
