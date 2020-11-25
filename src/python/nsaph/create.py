import argparse

from nsaph.model import Table
from nsaph.db import Connection
from nsaph.reader import name, get_entries


def create_table(table: Table, force: bool = False):
    entries, open_function = get_entries(table.file_path)
    table.open_entry_function = open_function
    with Connection() as connection:
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
    parser.add_argument("--config", "-c",
                        help="Path to a config file for a table",
                        required=True)
    parser.add_argument("--force", action='store_true',
                        help="Force recreating table if it is already exists")

    args = parser.parse_args()

    table = Table(args.config, None)
    create_table (table)
