"""
A command line utility to export results of SQL query as Parquet files
"""
import logging
import os.path
from argparse import ArgumentParser
from typing import Dict, Callable, Optional, List

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


import pyarrow as pa
import pyarrow.dataset as ds
from psycopg2.extras import RealDictCursor
from psycopg2.extensions import connection
from contextlib import contextmanager

from pyarrow import RecordBatch
from pyarrow.dataset import Scanner

from nsaph import init_logging
from nsaph.db import Connection


@contextmanager
def result_set(conn: connection, sql: str):
    with conn.cursor(cursor_factory=RealDictCursor) as cursor:
        cursor.execute(sql)
        yield cursor


class PgPqQuery:
    def __init__(self, cnxn: connection, sql: str, destination: str):
        self.sql = sql
        self.connection = cnxn
        self.destination = destination
        self.partition_columns = []
        self.cur_partition = {}
        self.transformer: Optional[Callable] = None
        self.parquet_partitioning = None
        with self.connection.cursor() as cursor:
            cursor.execute(self.metadata_sql())
            columns = [(desc.name, desc.type_code) for desc in cursor.description]
            type_codes = list({str(c[1]) for c in columns})
            in_clause = "(" + ','.join(type_codes)  + ")"
            cursor.execute("SELECT oid, typname FROM pg_type WHERE oid in " + in_clause)
            mapping = {int(row[0]): row[1] for row in cursor}
        schema = []
        for c in columns:
            name = c[0]
            vtype = mapping[c[1]].lower()
            if vtype in ['int2', 'int4', 'int8']:
                pa_type = pa.int32()
            elif vtype.startswith("int"):
                pa_type = pa.int64()
            elif vtype in ["str", "varchar", "text"]:
                pa_type = pa.string()
            elif vtype.startswith("float"):
                pa_type = pa.float64()
            elif vtype in ["date"]:
                pa_type = pa.date32()
            elif vtype in ["time"]:
                pa_type = pa.date64()
            elif vtype in ["timestamp"]:
                pa_type = pa.timestamp('ns')
            else:
                pa_type = pa.string()
            schema.append((name, pa_type))
        self.column_types = {
            s[0]: s[1] for s in schema
        }
        self.schema = pa.schema(schema)
        self.count = 0
        return

    def metadata_sql(self) -> str:
        idx = self.sql.lower().find("limit")
        if idx < 0:
            return self.sql + "\nLIMIT 0"
        i = idx
        for c in self.sql[idx + 5:]:
            if c.isdigit() or c.isspace():
                i += 1
                continue
            break
        sql = self.sql[:idx] + "LIMIT 0" + self.sql[i:]
        return sql

    def set_partitioning(self, columns: List[str]):
        types = []
        for c in columns:
            self.partition_columns.append(c)
            pa_type = self.column_types[c]
            types.append(pa_type)
        self.cur_partition = {
            p: None for p in self.partition_columns
        }
        self.parquet_partitioning = ds.partitioning(
            pa.schema([
                (self.partition_columns[i], types[i])
                for i in range(len(types))
            ]), flavor="hive"
        )

    def transform(self, row: Dict) -> Dict:
        if self.transformer:
            row = self.transformer(row)
        return {p: row[p] for p in row}

    def export(self):
        self.count = 0
        scanner = Scanner.from_batches(source=self.batches(), schema = self.schema)
        ds.write_dataset(scanner, self.destination, partitioning=self.parquet_partitioning, format="parquet")
        return 

    def batch(self, data: List[Dict]):
        self.count += len(data)
        logging.info(f"Flushing {str(len(data))} records. Total: {str(self.count)}")
        return RecordBatch.from_pylist(mapping=data, schema=self.schema)

    def batches(self):
        batch_size = 10000
        with result_set(self.connection, self.sql) as rs:
            data = []
            for row in rs:
                if len(data) >= batch_size:
                    yield self.batch(data)
                    data.clear()
                data.append(self.transform(row))
            yield self.batch(data)

    @classmethod
    def main(cls):
        init_logging()
        parser = ArgumentParser (description="Import/Export resources")
        parser.add_argument("--sql", "-s",
                        help="SQL Query or a path to a file containing SQL query",
                        required=False)
        parser.add_argument("--partition", "-p",
                        help="Columns to be used for partitioning",
                        nargs='+',
                        required=False)
        parser.add_argument("--output", "--destination", "-o",
                        help="Path to a directory, where the files will be exported",
                        required=True)
        parser.add_argument("--db",
                            help="Path to a database connection parameters file",
                            default="database.ini",
                            required=True)
        parser.add_argument("--connection", "-c",
                            help="Section in the database connection parameters file",
                            default="nsaph2",
                            required=True)

        arguments = parser.parse_args()
        if os.path.isfile(arguments.sql):
            with open(arguments.sql) as inp:
                sql = '\n'.join([line for line in inp])
        else:
            sql = arguments.sql
        with Connection(arguments.db, arguments.connection) as db:
            instance = cls(db, sql, arguments.output)
            if arguments.partition:
                instance.set_partitioning(arguments.partition)
            instance.export()


if __name__ == '__main__':
    PgPqQuery.main()