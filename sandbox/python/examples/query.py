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
import json
import sys
from decimal import Decimal

from psycopg2.extras import RealDictCursor

from nsaph.db import Connection


SQL = '''
SELECT 
    t.county,
    AVG(tmmx) - 273.15 AS tmmx,
    AVG(rmin) AS rmin,
    AVG(rmax) AS rmax,
    AVG(median_household_income) AS income,
    AVG(arithmetic_mean) AS pm25
FROM 
    gridmet.county_tmmx as t
        join gridmet.county_rmin as r1 on r1.county = t.county and r1.observation_date = t.observation_date
        join gridmet.county_rmax as r2 on r2.county = t.county and r2.observation_date = t.observation_date
        join census.county_interp as c on c.geoid = t.county AND (c.year = EXTRACT (YEAR FROM t.observation_date))
        join epa.pm25_daily as p on p.date_local = t.observation_date AND ((state_code || county_code)::int) = t.county and p.state_code ~ '^[0-9]+$'  
GROUP BY 1
ORDER BY 1
'''


def query(db_ini_file: str, db_conn_name: str):
    connection = Connection(db_ini_file,
                            db_conn_name,
                            silent=True,
                            app_name_postfix=".sample_query")
    count = 0
    with connection.connect() as cnxn:
        with cnxn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(SQL)
            for row in cursor:
                row = {k: float(row[k]) if isinstance(row[k], Decimal) else row[k] for k in row}
                s = json.dumps(row)
                print(s)
                count += 1
    print("Returned {:d} rows".format(count))


if __name__ == '__main__':
    query(sys.argv[1], sys.argv[2])
                

