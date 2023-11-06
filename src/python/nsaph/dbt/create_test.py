"""
A utility to generate a test based on a sample table
"""
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

import datetime
import logging
import threading
import time
from enum import Enum
from typing import List, Dict, Optional, Callable
from psycopg2.extras import RealDictCursor
from psycopg2.extensions import connection

from nsaph import init_logging
from nsaph.db import Connection
from nsaph.dbt.dbt_config import DBTConfig


class CType(Enum):
    categorical = "categorical"
    text = "text"
    integral = "integral"
    numeric = "numeric"
    date = 'date'


class Column:
    def __init__(self, name: str, ctype: CType, is_indexed: bool):
        self.name = name
        self.type = ctype
        self.is_indexed = is_indexed


class TableFingerprint:
    CATEGORICAL_THRESHOLD = 24
    def __init__(self, context: DBTConfig = None):
        if not context:
            context = DBTConfig(None, __doc__).instantiate()
        self.context = context
        if not self.context.table:
            raise ValueError("'--table' is a required option")
        init_logging(name="generate-test-" + self.context.table)
        if '.' in self.context.table:
            t = self.context.table.split('.')
            self.table = t[1]
            self.schema = t[0]
        else:
            self.table = self.context.table
            self.schema = "public"
        self.fqtn = f"{self.schema}.{self.table}"
        self.count_distinct = "SELECT COUNT(DISTINCT {c}) FROM " + self.fqtn
        self.columns: List[Column] = []
        self.test_cases: List[str] = []
        cnxn = Connection(self.context.db, self.context.connection)
        self.catalog = cnxn.parameters["database"]
        self.get_columns()

    def get_columns(self):
        i1 = """
                       coalesce(a.attname,                                                     
                                (('{' || pg_get_expr(                                          
                                            i.indexprs,                                        
                                            i.indrelid                                         
                                         )                                                     
                                      || '}')::text[]                                          
                                )[k.i]                                                         
                               ) AS index_column                                     
        """
        i2 = f"""
            CASE WHEN COLUMN_NAME IN (
                SELECT
                    {i1} 
                FROM pg_index i                                                                
                   CROSS JOIN LATERAL unnest(i.indkey) WITH ORDINALITY AS k(attnum, i)         
                   LEFT JOIN pg_attribute AS a                                                 
                      ON i.indrelid = a.attrelid AND k.attnum = a.attnum                       
                WHERE 
                    i.indrelid = '{self.fqtn}'::regclass
                    AND k.i = 1
            ) THEN true ELSE false END AS indexed
        """
        sql = f"""
            SELECT
                COLUMN_NAME,
                data_type,
                {i2}
            FROM 
                information_schema.columns
            WHERE 
                table_name = '{self.table}'
                AND table_schema = '{self.schema}'
                AND table_catalog = '{self.catalog}'
            ORDER BY 
                1        
        """
        with Connection(self.context.db, self.context.connection) as cnxn:
            with cnxn.cursor() as cursor:
                cursor.execute(sql)
                columns = [row for row in cursor]

            for c in columns:
                if c[1] in ['integer']:
                    t = CType.integral
                elif c[1] in ['numeric']:
                    t = CType.numeric
                elif c[1] in ['date']:
                    t = CType.date
                else:
                    t = CType.text
                dc = 1024*1024*1024
                if t != 'numeric':
                    with cnxn.cursor() as cursor:
                        cursor.execute(self.count_distinct.format(c=c[0]))
                        for row in cursor:
                            dc = row[0]
                if dc < self.CATEGORICAL_THRESHOLD and t == CType.text:
                    t = CType.categorical
                if t == CType.integral and dc > self.CATEGORICAL_THRESHOLD:
                    t = CType.numeric
                self.columns.append(Column(c[0], t, c[2]))
        return

    def get_categories(self) -> List[Column]:
        return [
            c for c in self.columns if c.is_indexed and c.type in
                    [CType.integral, CType.categorical]
        ]

    def generate_tests(self):
        for c in self.columns:
            self.test_column(c)

    def test_column(self, c: Column):
        if c.type in [CType.text, CType.categorical, CType.integral, CType.date]:
            s1 = self.count_distinct.format(c=c.name)
            test_case = self.test_exact(s1, c.name, "count distinct")
            self.test_cases.append(test_case)
        if c.type in [CType.text, CType.categorical]:
            s2 = f"SELECT MD5(string_agg({c.name}::varchar, '')) FROM {self.fqtn}"
            test_case = self.test_exact(s2, c.name, "MD5 value")
            self.test_cases.append(test_case)
        if c.type in [CType.numeric, CType.integral]:
            s3 = f"SELECT AVG({c.name}) FROM {self.fqtn}"
            test_case = self.test_approximate(s3, c.name, "Mean value")
            self.test_cases.append(test_case)
            s4 = f"SELECT VARIANCE({c.name}) FROM {self.fqtn}"
            test_case = self.test_approximate(s4, c.name, "Variance")
            self.test_cases.append(test_case)

    def test_case_sql(self, name: str, test: str, condition: str) -> str:
        return "SELECT \n" \
               + f"\t'{self.fqtn}.{name}' As table_column,\n" \
               + f"\t'{test}' As Testing,\n" \
               + "\tCASE \n" \
               + f"\t\tWHEN {condition} \n" \
               + "\t\tTHEN true ELSE false END AS passed\n"

    def test_exact(self, sql: str, name: str, test: str) -> str:
        v = None
        with Connection(self.context.db, self.context.connection) as cnxn:
            with cnxn.cursor() as cursor:
                cursor.execute(sql)
                for row in cursor:
                    v = row[0]
        condition = f"({sql}) = '{str(v)}'"
        test_case = self.test_case_sql(name, test, condition)
        return test_case

    def test_approximate(self, sql: str, name: str, test: str) -> str:
        v = None
        with Connection(self.context.db, self.context.connection) as cnxn:
            with cnxn.cursor() as cursor:
                cursor.execute(sql)
                for row in cursor:
                    v = row[0]
        if v >= 0:
            v1 = 0.99 * float(v)
            v2 = 1.01 * float(v)
        else:
            v2 = 0.99 * float(v)
            v1 = 1.01 * float(v)
        condition = f"({sql}) BETWEEN {str(v1)} AND {str(v2)}"
        test_case = self.test_case_sql(name, test, condition)
        return test_case

    def union(self):
        return "UNION ALL\n".join(self.test_cases)

    def write_test_script(self):
        first = True
        with open(self.context.script[0], "wt") as script:
            for test_case in self.test_cases:
                if not first:
                    print("UNION ALL", file=script)
                first = False
                print("-- Test case start", file=script)
                print(test_case, file=script)
                print("-- Test case end", file=script)


if __name__ == '__main__':
    fingerprint = TableFingerprint()
    fingerprint.generate_tests()
    # print(fingerprint.union())
    fingerprint.write_test_script()
