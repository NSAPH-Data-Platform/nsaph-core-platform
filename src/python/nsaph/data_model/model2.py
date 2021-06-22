import argparse
import os
from pathlib import Path

from nsaph import init_logging
from nsaph.data_model.domain import Domain
from nsaph.data_model.inserter import Inserter
from nsaph.data_model.utils import DataReader
from nsaph.db import Connection


def print_ddl (domain):
    for ddl in domain.ddl:
        print(ddl)
    for ddl in domain.indices:
        print(ddl)


def get_domain(arguments):
    src = Path(__file__).parents[3]
    registry_path = os.path.join(src, "yml", arguments.domain + ".yaml")
    domain = Domain(registry_path, arguments.domain)
    domain.init()
    return domain


def run():
    arguments = args()
    
    init_logging()
    domain = get_domain(arguments)
    if not arguments.table:
        print_ddl(domain)
        return
    table = arguments.table
    if "enrollments" in table:
        page = 100 if arguments.page is None else arguments.page
        log_step = 10000 if arguments.log is None else arguments.log
    else:
        page = 1000 if arguments.page is None else arguments.page
        log_step = 1000000 if arguments.log is None else arguments.log

    connections = [Connection(arguments.db, arguments.connection).connect() for _ in range(arguments.threads)]
    try:
        for connection in connections:
            connection.autocommit = arguments.autocommit
        if arguments.reset:
            connection = connections[0]
            tables = domain.drop(table, connection)
            domain.create(connection, tables)
            if not connection.autocommit:
                connection.commit()
        if domain.has("quoting") or domain.has("header"):
            q = domain.get("quoting")
            h = domain.get("header")
            with DataReader(arguments.data, buffer_size=arguments.buffer, quoting=q, has_header=h) as reader:
                if h is False:
                    reader.columns = domain.list_columns(table)
                inserter = Inserter(domain, table, reader, connections, page_size=page)
                inserter.import_file(limit=arguments.limit, log_step=log_step)
        else:
            with DataReader(arguments.data, buffer_size=arguments.buffer) as reader:
                inserter = Inserter(domain, table, reader, connections, page_size=page)
                inserter.import_file(limit=arguments.limit, log_step=log_step)
        for connection in connections:
            connection.commit()
    finally:
        for connection in connections:
            connection.close()


def args():
    parser = argparse.ArgumentParser (description="Create database for a given domain")
    parser.add_argument("--domain",
                        help="Name of the domain",
                        default="medicaid",
                        required=False)
    parser.add_argument("--table", "-t",
                        help="Name of the table to load data into",
                        required=False)
    parser.add_argument("--data",
                        help="Path to a data file or directory",
                        required=False)
    parser.add_argument("--reset", action='store_true',
                        help="Force recreating table(s) if it/they already exist")
    parser.add_argument("--autocommit", action='store_true',
                        help="Use autocommit")
    parser.add_argument("--db",
                        help="Path to a database connection parameters file",
                        default="database.ini",
                        required=False)
    parser.add_argument("--connection",
                        help="Section in the database connection parameters file",
                        default="nsaph2",
                        required=False)
    parser.add_argument("--page", type=int, help="Explicit page size for the database")
    parser.add_argument("--log", type=int, help="Explicit interval for logging")
    parser.add_argument("--limit", type=int, help="Load at most specified number of records")
    parser.add_argument("--buffer", type=int, help="Buffer size for converting fst files")
    parser.add_argument("--threads", type=int, help="Number of threads writing into the database", default=1)

    arguments = parser.parse_args()
    return arguments


if __name__ == '__main__':
    run()
    #test_step2(sys.argv)
    #print_ddl()