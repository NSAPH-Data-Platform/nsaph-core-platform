import logging
import threading
import time
from datetime import datetime

from nsaph import init_logging
from nsaph.data_model.common import common_args, get_domain
from nsaph.data_model.model import INDEX_REINDEX, INDEX_INCREMENTAL
from nsaph.db import Connection
from nsaph.index import print_stat


def find_name(sql):
    x = sql.split(" ")
    for i in range(0, len(x)):
        if x[i] == "ON":
            return x[i-1]
    raise Exception(sql)


def execute(arguments, connection):
    domain = get_domain(arguments)
    if arguments.force:
        flag = INDEX_REINDEX
    elif arguments.incremental:
        flag = INDEX_INCREMENTAL
    else:
        flag = None

    if arguments.table is not None:
        indices = domain.indices_by_table[domain.fqn(arguments.table)]
    else:
        indices = domain.indices
    print(indices)

    for index in indices:
        name = find_name(index)
        with (connection.cursor()) as cursor:
            if flag == INDEX_REINDEX:
                sql = "DROP INDEX IF EXISTS {name}".format(name=name)
                logging.info(str(datetime.now()) + ": " + sql)
                cursor.execute(sql)
            if flag == INDEX_INCREMENTAL:
                sql = index.replace(name, "IF NOT EXISTS " + name)
            else:
                sql = index
            logging.info(str(datetime.now()) + ": " + sql)
            cursor.execute(sql)


def build_indices(arguments):
    with Connection(arguments.db, arguments.connection) as connection:
        connection.autocommit = arguments.autocommit
        x = threading.Thread(target=execute, args=(arguments, connection))
        x.start()
        n = 0
        step = 100
        while x.is_alive():
            time.sleep(0.1)
            n += 1
            if (n % step) == 0:
                print_stat(arguments.db, arguments.connection)
                if n > 100000:
                    step = 6000
                elif n > 10000:
                    step = 600
        x.join()


if __name__ == '__main__':
    init_logging()
    parser = common_args (description="Build indices")
    parser.add_argument("--force", action='store_true',
                        help="Force reindexing if index already exists")
    parser.add_argument("--incremental", "-i", action='store_true',
                        help="Skip over indices that already exist")

    build_indices(parser.parse_args())

