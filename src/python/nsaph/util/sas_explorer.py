#  Copyright (c) 2022. Harvard University
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
import sys

from nsaph.data_model.utils import DataReader
from sas7bdat import SAS7BDAT

from nsaph_utils.utils.io_utils import sizeof_fmt


def info(file_path: str):
    reader = SAS7BDAT(file_path, skip_header=True)
    columns = [
                column.name if isinstance(column.name, str)
                else column.name.decode("utf-8")
                for column in reader.columns
    ]
    for i, column in enumerate(columns):
        print (column, reader.columns[i].type)

    header = reader.header.properties
    print(header)

    print("Row count: {:,}".format(header.row_count))
    s = header.page_count * header.page_length
    print("Size: {}".format(sizeof_fmt(s)))

    simulate(file_path)

    count = 0
    for row in reader:
        count += 1
        if (count % 100000) == 0:
            print(count)
        if count > header.row_count:
            print(row)
        if count > header.row_count + 20:
            break
    print(count)


def simulate(file_path: str, page = 100):
    count = 0
    with DataReader(file_path) as reader:
        while True:
            c = 0
            for row in reader.rows():
                count += 1
                c += 1
                if c >= page:
                    if (count % 1000000) == 0:
                        print(row)
                    break
            if c < 1:
                break
    return


if __name__ == '__main__':
    info(sys.argv[1])
    