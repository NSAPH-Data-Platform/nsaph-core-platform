import argparse

from nsaph.model import Table
from nsaph.db import Connection
from nsaph.reader import name, get_entries


def add_gis_columns(table: Table, db: str = None, section: str = None):
    with Connection(filename=db, section=section) as connection:
        cur = connection.cursor()
        table.make_fips_column(cur)
        table.make_iso_column("state_name", cur, include="")
        connection.commit()


if __name__ == '__main__':
    parser = argparse.ArgumentParser (description="Create table and load data")
    parser.add_argument("--tdef", "-t",
                        help="Path to a table definition file for a table",
                        required=True)
    parser.add_argument("--db",
                        help="Path to a database connection parameters file",
                        default="database.ini",
                        required=False)
    parser.add_argument("--section",
                        help="Section in the database connection parameters file",
                        default="postgres",
                        required=False)

    args = parser.parse_args()

    table = Table(args.tdef, None)
    add_gis_columns (table, args.db, args.section)
