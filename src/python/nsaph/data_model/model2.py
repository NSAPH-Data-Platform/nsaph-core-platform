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
    with Connection("database.ini", "nsaph2") as connection, DataReader(path_to_csv) as reader:
        cursor = connection.cursor()
        domain.create_tables(cursor)
        inserter = Inserter(domain, "demographics", reader, cursor)
        inserter.import_file(os.path.basename(path_to_csv))
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

def print_ddl():
    src = Path(__file__).parents[3]
    registry_path = os.path.join(src, "yml", "medicaid.yaml")
    domain = Domain(registry_path, "medicaid")
    domain.init()
    for ddl in domain.ddl:
        print(ddl)



if __name__ == '__main__':
    init_logging()
    test_step2(sys.argv)
    #print_ddl()