import os
from argparse import ArgumentParser
from pathlib import Path

from nsaph.data_model.domain import Domain


def common_args(description: str) -> ArgumentParser:
    parser = ArgumentParser (description=description)
    parser.add_argument("--domain",
                        help="Name of the domain",
                        default="medicaid",
                        required=False)
    parser.add_argument("--table", "-t",
                        help="Name of the table to load data into",
                        required=False)
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

    return parser


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