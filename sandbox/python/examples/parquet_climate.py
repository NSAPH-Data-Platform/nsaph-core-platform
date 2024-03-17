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
import sys

import pyarrow
import pyarrow.parquet as parquet
from pyarrow import schema
from pyarrow.dataset import partitioning
import pandas


def avg_for_county(path_to_dir: str, var: str, county: str):
    p = partitioning(schema=schema([("year", pyarrow.int32())]), flavor="filename")
    table = parquet.read_table(path_to_dir, partitioning=p)
    print(f"Table shape is: {str(table.shape)}")
    print(f"Table schema is: {str(table.schema)}")
    if var not in table.column_names:
        raise ValueError(f"Unknown variable: {var}. Available columns are: {str(table.column_names)}")
    df: pandas.DataFrame = table.to_pandas().filter(items=['year','month', 'county', 'state', var])
    df = df[df.county == county]
    years = sorted(set(df.year.values))
    result = df.groupby(['year', 'month'])[var].mean()
    print('      ', end='')
    for m in range(1,13):
        print(f'{m:10.0f}', end='')
    print()
    for y in years:
        print(f'{y:6}', end='')
        for m in range(1,13):
            v = result[(y,m)]
            print(f'{v:10.2f}', end='')
        print()
    print()
    return


if __name__ == '__main__':
    avg_for_county(sys.argv[1], sys.argv[2], sys.argv[3])


    
    



