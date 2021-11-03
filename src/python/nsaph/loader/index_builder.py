"""
This module supports building indices on domain tables
and monitoring the build progress
"""


import logging
import threading
import time
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
        self.monitor = DBActivityMonitor(context)

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
                name = self.domain.fqn(find_name(index))
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
        for msg in self.monitor.get_indexing_progress():
            logging.info(msg)


if __name__ == '__main__':
    IndexBuilder().run()
