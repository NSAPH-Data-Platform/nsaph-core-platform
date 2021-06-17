"""
Domain Data Loader

Intended to load data from a single or a set of column-formatted files
into NSAPH PostgreSQL Database.
Input (aka source) files can be either in FST or in CSV format

"""
import os
from pathlib import Path
from typing import List

from psycopg2.extensions import connection

from nsaph import init_logging
from nsaph.data_model.domain import Domain
from nsaph.data_model.inserter import Inserter
from nsaph.data_model.utils import DataReader
from nsaph.db import Connection
from nsaph.loader.conf import Config, Parallelization


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

    def __init__(self, context: Config = None):
        if not context:
            context = Config(__doc__).instantiate()
        self.context = context
        if context.domain:
            self.domain = self.get_domain(context.domain)
        self.table = context.table
        self.page = context.page
        self.log_step = context.log
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

    def get_connection(self) -> connection:
        c = Connection(self.context.db, self.context.connection).connect()
        if self.context.autocommit is not None:
            c.autocommit = self.context.autocommit
        return c

    def get_connections(self) -> List[connection]:
        if self.context.threads < 2 or self.context.parallelization == Parallelization.none:
            return [self.get_connection()]
        return [self.get_connection() for _ in range(self.context.threads)]

    def reset(self):
        if not self.context.reset:
            return
        with self.get_connection() as connection:
            tables = self.domain.drop(self.table, connection)
            self.domain.create(connection, tables)

    def get_data_reader(self, path):
        if self.domain.has("quoting") or self.domain.has("header"):
            q = self.domain.get("quoting")
            h = self.domain.get("header")
            reader = DataReader(path, buffer_size=self.context.buffer, quoting=q, has_header=h)
            if h is False:
                reader.columns = self.domain.list_columns(self.table)
            return reader
        return DataReader(path, buffer_size=self.context.buffer)

    def load_data(self):
        self.reset()
        connections = self.get_connections()
        try:
            for path in self.context.data:
                with self.get_data_reader(path) as reader:
                    inserter = Inserter(self.domain, self.table, reader, connections, page_size=self.page)
                    inserter.import_file(limit=self.context.limit, log_step=self.log_step)
            for c in connections:
                c.commit()
        finally:
            for c in connections:
                c.close()


if __name__ == '__main__':
    init_logging()
    loader = DataLoader()
    #loader.print_ddl()
    loader.load_data()
