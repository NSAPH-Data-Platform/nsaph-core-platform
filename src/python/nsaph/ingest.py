#  Copyright (c) 2021. Harvard University
#
#  Developed by Research Software Engineering,
#  Faculty of Arts and Sciences, Research Computing (FAS RC)
#  Author: Michael A Bouzinier
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#

import logging
import sys

from nsaph.analyze import analyze
from nsaph.create import create_table
from nsaph.index import build_indices
from nsaph.db import Connection
from nsaph.data_model.model import INDEX_REINDEX


def ingest(args: list, force: bool = False):
    logging.getLogger().setLevel("DEBUG")
    Connection.default_section = "postgresql10"
    table = analyze(*args)

    create_table(table, force)
    logging.info("Table created: " + table.table)

    logging.info("Building indices:")
    flag = INDEX_REINDEX if force else None
    build_indices(table, flag)
    print ("All DONE")




if __name__ == '__main__':
    #test_connection()
    ingest(sys.argv[1:], True)

