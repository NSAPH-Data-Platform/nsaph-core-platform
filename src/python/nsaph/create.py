import argparse

from nsaph.model import Table
from nsaph.db import Connection
from nsaph.reader import name, get_entries


def create_table(table: Table, force: bool = False, db: str = None,
                 section: str = None):
    entries, open_function = get_entries(table.file_path)
    table.open_entry_function = open_function
    with Connection(filename=db, section=section) as connection:
        if force:
            try:
                table.drop(connection.cursor())
            except:
                connection.rollback()
        cur = connection.cursor()
        table.create(cur)
        for e in entries:
            print ("Adding: " + name(e))
            table.add_data(cur, e)


if __name__ == '__main__':
    parser = argparse.ArgumentParser (description="Create table and load data")
    parser.add_argument("--tdef", "-t",
                        help="Path to a table definition file for a table",
                        required=True)
    parser.add_argument("--data",
                        help="Path to a data file",
                        required=False)
    parser.add_argument("--force", action='store_true',
                        help="Force recreating table if it already exists")
    parser.add_argument("--db",
                        help="Path to a database connection parameters file",
                        default="database.ini",
                        required=False)
    parser.add_argument("--section",
                        help="Section in the database connection parameters file",
                        default="postgres",
                        required=False)

    args = parser.parse_args()

    table = Table(args.tdef, None, data_file=args.data)
    create_table (table, args.force, args.db, args.section)
