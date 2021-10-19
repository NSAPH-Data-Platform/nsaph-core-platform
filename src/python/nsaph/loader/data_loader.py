"""
Domain Data Loader

Intended to load data from a single or a set of column-formatted files
into NSAPH PostgreSQL Database.
Input (aka source) files can be either in FST or in CSV format

"""
import os
from pathlib import Path
import logging
import fnmatch

from psycopg2.extensions import connection

from typing import List, Tuple, Callable, Any

from nsaph import init_logging, ORIGINAL_FILE_COLUMN
from nsaph.data_model.domain import Domain
from nsaph.data_model.inserter import Inserter
from nsaph.data_model.utils import DataReader, entry_to_path
from nsaph.db import Connection
from nsaph.loader.conf import LoaderConfig, Parallelization
from nsaph.reader import get_entries


def is_dir(path: str) -> bool:
    """
    Determine if a certain path specification refers
        to a collection of files or a single entry.
        Examples of collections are folders (directories)
        and archives

    :param path: path specification
    :return: True if specification refers to a collection of files
    """

    return (path.endswith(".tar")
            or path.endswith(".tgz")
            or path.endswith(".tar.gz")
            or path.endswith(".zip")
            or os.path.isdir(path)
    )


class DataLoader:
    """
    Class for data loader
    """

    @staticmethod
    def get_domain(name):
        src = Path(__file__).parents[3]
        registry_path = os.path.join(src, "yml", name + ".yaml")
        domain = Domain(registry_path, name)
        domain.init()
        return domain

    def __init__(self, context: LoaderConfig = None):
        init_logging()
        if not context:
            context = LoaderConfig(__doc__).instantiate()
        self.context = context
        self.domain = self.get_domain(context.domain)
        self.table = context.table
        self.page = context.page
        self.log_step = context.log
        self._connections = None
        if self.context.incremental and self.context.autocommit:
            raise ValueError("Incompatible arguments: autocommit is "
                             + "incompatible with incremental loading")
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
        return

    def print_ddl(self):
        for ddl in self.domain.ddl:
            print(ddl)

    def _connect(self) -> connection:
        c = Connection(self.context.db, self.context.connection).connect()
        if self.context.autocommit is not None:
            c.autocommit = self.context.autocommit
        return c

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
            logging.info("Looking for relevant entries in {}.".format(path))
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
        with self._connect() as connxn:
            tables = self.domain.drop(self.table, connxn)
            self.domain.create(connxn, tables)

    def run(self):
        if not self.context.table:
            self.print_ddl()
            return
        if self.context.reset:
            self.reset()
        self.load()
        return

    def commit(self):
        if not self.context.autocommit:
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
        self.reset()
        connections = self.get_connections()
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
                    if self.context.incremental:
                        logging.exception("Exception: " + entry_to_path(entry))
                        self.rollback()
                        logging.info("Rolled back and skipped: " + entry_to_path(entry))
                    else:
                        raise x
            self.commit()
        finally:
            self.close()

    def import_data_from_file(self, data_file):
        table = self.context.table
        buffer = self.context.buffer
        limit = self.context.limit
        connections = self.get_connections()
        if self.domain.has("quoting") or self.domain.has("header"):
            q = self.domain.get("quoting")
            h = self.domain.get("header")
        else:
            q = None
            h = None
        with DataReader(data_file, buffer_size=buffer, quoting=q, has_header=h) as reader:
            if h is False:
                reader.columns = self.domain.list_columns(table)
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
