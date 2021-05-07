import argparse
import logging
import os
import sys
from pathlib import Path

from nsaph import init_logging
from nsaph.data_model.domain import Domain
from nsaph.data_model.inserter import Inserter
from nsaph.data_model.utils import DataReader
from nsaph.db import Connection





def test_init(argv):
    if len(argv) > 1:
        path_to_csv = argv[1]
    else:
        path_to_csv = "data/medicaid/maxdata_demographics.csv.gz"
    src = Path(__file__).parents[3]
    registry_path = os.path.join(src, "yml", "medicaid.yaml")
    domain = Domain(registry_path, "medicaid")
    domain.init()
    for index in domain.indices:
        print(index)
    with Connection("database.ini", "nsaph2 local") as connection, DataReader(path_to_csv) as reader:
        cursor = connection.cursor()
        domain.create(cursor)
        inserter = Inserter(domain, "demographics", reader, cursor)
        inserter.import_file(os.path.basename(path_to_csv), log_step=100000)
        connection.commit()


def test_step2(argv):
    if len(argv) > 1:
        path_to_fst = argv[1]
    else:
        path_to_fst = "data/medicaid/medicaid_mortality_2001.fst"
    src = Path(__file__).parents[3]
    registry_path = os.path.join(src, "yml", "medicaid.yaml")
    domain = Domain(registry_path, "medicaid")
    domain.init()
    with Connection("database.ini", "nsaph2") as connection, DataReader(path_to_fst) as reader:
        cursor = connection.cursor()
        inserter = Inserter(domain, "enrollments_year", reader, cursor, page_size=100)
        inserter.import_file(os.path.basename(path_to_fst), limit=None, log_step=10000)
        connection.commit()


def print_ddl (domain):
    for ddl in domain.ddl:
        print(ddl)


def get_domain(arguments):
    src = Path(__file__).parents[3]
    registry_path = os.path.join(src, "yml", arguments.domain + ".yaml")
    domain = Domain(registry_path, arguments.domain)
    domain.init()
    return domain


def run():
    arguments = args()
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

    with Connection(arguments.db, arguments.connection) as connection:
        connection.autocommit = arguments.autocommit
        if arguments.reset:
            tables = domain.drop(table, connection)
            domain.create(connection, tables)
        with DataReader(arguments.data, buffer_size=arguments.buffer) as reader:
            inserter = Inserter(domain, table, reader, connection, page_size=page)
            inserter.import_file(os.path.basename(arguments.data), limit=arguments.limit, log_step=log_step)
        connection.commit()


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

    arguments = parser.parse_args()
    return arguments


if __name__ == '__main__':
    init_logging()
    run()
    #test_step2(sys.argv)
    #print_ddl()