import fnmatch
import logging
import os
from typing import List, Tuple, Callable, Any

from nsaph import init_logging, ORIGINAL_FILE_COLUMN
from nsaph.data_model.common import common_args, print_ddl, get_domain
from nsaph.data_model.domain import Domain
from nsaph.data_model.inserter import Inserter
from nsaph.data_model.utils import DataReader, entry_to_path
from nsaph.db import Connection
from nsaph.reader import get_entries


def is_dir(path: str) -> bool:
    return (path.endswith(".tar")
            or path.endswith(".tgz")
            or path.endswith(".tar.gz")
            or path.endswith(".zip")
            or os.path.isdir(path)
    )


def get_files(arguments) -> List[Tuple[Any,Callable]]:
    path = arguments.data
    if not is_dir(path):
        return [path]
    logging.info("Looking for relevant entries.")
    entries, f = get_entries(path)
    if not arguments.pattern:
        return [(e,f) for e in entries]
    objects = []
    for e in entries:
        if isinstance(e, str):
            name = e
        else:
            name = e.name
        if fnmatch.fnmatch(name, arguments.pattern):
            objects.append((e, f))
    return objects


def has_been_ingested(file:str, connection, table):
    sql = "SELECT 1 FROM {} WHERE {} = '{}' LIMIT 1".format(table, ORIGINAL_FILE_COLUMN, file)
    logging.debug(sql)
    with connection.cursor() as cursor:
        cursor.execute(sql)
        exists = len([r for r in cursor]) > 0
    return exists


def connect(arguments):
    return [
        Connection(arguments.db, arguments.connection).connect()
        for _ in range(arguments.threads)
    ]


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

    connections = connect(arguments)
    try:
        for connection in connections:
            connection.autocommit = arguments.autocommit
        if arguments.reset:
            connection = connections[0]
            tables = domain.drop(table, connection)
            domain.create(connection, tables)
            if not connection.autocommit:
                connection.commit()

        logging.info("Processing: " + arguments.data)
        for entry in get_files(arguments):
            try:
                if arguments.incremental:
                    ff = os.path.basename(entry_to_path(entry))
                    logging.info("Checking if {} has been already ingested.".format(ff))
                    exists = has_been_ingested(ff, connections[0], domain.fqn(table))
                    if exists:
                        logging.warning("Skipping already imported file " + ff)
                        continue
                logging.info("Importing: " + entry_to_path(entry))
                import_data(domain=domain,
                            connections=connections,
                            data=entry,
                            buffer=arguments.buffer,
                            limit=arguments.limit,
                            log_step=log_step,
                            table=table,
                            page=page
                )
                if arguments.incremental:
                    for connection in connections:
                        connection.commit()
                    logging.info("Committed: " + entry_to_path(entry))
            except Exception as x:
                if arguments.incremental:
                    logging.exception("Exception: " + entry_to_path(entry))
                    for connection in connections:
                        connection.rollback()
                    logging.info("Rolled back and skipped: " + entry_to_path(entry))
                else:
                    raise x
        for connection in connections:
            connection.commit()
    finally:
        for connection in connections:
            connection.close()


def import_data(domain: Domain, connections: List[Connection],
                data, table, buffer, page, log_step, limit):
    if domain.has("quoting") or domain.has("header"):
        q = domain.get("quoting")
        h = domain.get("header")
        with DataReader(data, buffer_size=buffer, quoting=q, has_header=h) as reader:
            if h is False:
                reader.columns = domain.list_columns(table)
            inserter = Inserter(domain, table, reader, connections, page_size=page)
            inserter.import_file(limit=limit, log_step=log_step)
    else:
        with DataReader(data, buffer_size=buffer) as reader:
            inserter = Inserter(domain, table, reader, connections, page_size=page)
            inserter.import_file(limit=limit, log_step=log_step)



def args():
    parser = common_args("Create database for a given domain")
    parser.add_argument("--data",
                        help="Path to a data file or directory. Can be a "
                             + "single CSV, gzipped CSV or FST file or a directory recursively "
                             + "containing CSV files. Can also be a tar, tar.gz (or tgz) or zip archive "
                             + "containing CSV files",
                        required=False)
    parser.add_argument("--pattern",
                        help="pattern for files in a directory or an archive, e.g. \"**/maxdata_*_ps_*.csv\"")
    parser.add_argument("--reset", action='store_true',
                        help="Force recreating table(s) if it/they already exist")
    parser.add_argument("--incremental", action='store_true',
                        help="Commit every file and skip over files that have already been ingested")
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