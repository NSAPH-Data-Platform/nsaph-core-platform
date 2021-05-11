import argparse

from nsaph import init_logging
from nsaph.data_model.model import Table
from nsaph.db import Connection

## Area Deprivation Index (ADI)
## https://www.neighborhoodatlas.medicine.wisc.edu/

def add_gis_columns(table: Table, db: str = None, section: str = None):
    with Connection(filename=db, section=section) as connection:
        cur = connection.cursor()
        table.parse_fips12(cur)
        table.make_int_column(cur, "adi_natrank", "nat_rank", index=False)
        table.make_int_column(cur, "adi_staternk", "state_rank", index=False)
        table.make_iso_column("fips5", cur, include="nat_rank")
        connection.commit()


if __name__ == '__main__':
    init_logging()
    parser = argparse.ArgumentParser (description="Post-process ADI data")
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
