#  Copyright (c) 2024. Harvard University
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
import gc
import os
import shutil
from argparse import ArgumentParser
from contextlib import contextmanager
from random import randint, randbytes
from typing import List, Dict

from memory_profiler import profile
from pyarrow import RecordBatch
from pyarrow.dataset import Scanner
import pyarrow as pa
import pyarrow.dataset as ds
from pympler import summary, muppy, tracker
from pympler.classtracker import ClassTracker

from nsaph_utils.utils.io_utils import sizeof_fmt
from psycopg2.extras import RealDictCursor
from psycopg2.extensions import connection

from nsaph.db import Connection
from nsaph_utils.utils.profile_utils import qmem

batch_size = 10000


def me_prof():
    all_objects = muppy.get_objects()
    all_objects_summary = summary.summarize(all_objects)
    summary.print_(all_objects_summary)


@contextmanager
def result_set(conn: connection, sql: str, cursor_name: str, batch_size: int):
    with conn.cursor(cursor_factory=RealDictCursor, name=cursor_name) as cursor:
        cursor.itersize = batch_size
        cursor.execute(sql)
        yield cursor


def run_query(cnxn: connection, sql: str):
    n = 0
    with result_set(cnxn, sql, "test-cursor-1", batch_size) as rs:
        data = []
        for row in rs:
            if len(data) >= batch_size:
                data.clear()
                n += 1
                print('*', end='')
                if (n % 100) == 0:
                    print()
                    mem = sizeof_fmt(qmem())
                    print(f"{n* batch_size}: memory: {mem}")
            data.append(row)
    return


class PyArrowTest:
    def __init__(self):
        self.count = 0
        self.count_batches = 0
        schema = [
            ('a', pa.string()),
            ('b', pa.int64()),
            ('c', pa.int32())
        ]
        self.schema = pa.schema(schema)
        self.parquet_partitioning = ds.partitioning(
            pa.schema([
                ('b', pa.int64()),
                ('c', pa.int32())
            ]), flavor="hive"
        )
        self.class_tracker = ClassTracker()
        self.tracker = tracker.SummaryTracker()
        self.class_tracker.track_object(self, resolution_level=10)
        self.class_tracker.create_snapshot("0")

    def me_track(self):
        self.tracker.print_diff()
        self.class_tracker.stats.print_summary()
        self.class_tracker.create_snapshot(str(self.count_batches))

    def batch(self, data: List[Dict]):
        self.count += len(data)
        self.count_batches += 1
        print('*', end='')
        if batch_size > 100000 or (self.count_batches % 100) == 0:
            mem = sizeof_fmt(qmem())
            pmem = sizeof_fmt(pa.total_allocated_bytes())
            print()
            print(f"Received the {self.count_batches}-th batch of {len(data)} records. "
                  f"Total: {self.count}. Memory: {mem}. PyArrow: {pmem}")
            #me_prof()
            self.me_track()
        rb = RecordBatch.from_pylist(mapping=data, schema=self.schema)
        self.class_tracker.track_object(rb)
        return rb

    @profile
    def batches(self):
        data = []
        count = 0
        while True:
            row = {
                'a': str(randbytes(10)),
                'b': randint(1999, 2012),
                'c': randint(1,12)
            }
            if len(data) >= batch_size:
                yield self.batch(data)
                data.clear()
                #pa.default_memory_pool().release_unused()
                #gc.collect()
                count += 1
                if count > 1000:
                    break
            data.append(row)
        yield self.batch(data)

    def export(self):
        self.count = 0
        datadir = "test_data"
        if os.path.exists(datadir):
            shutil.rmtree(datadir)
        scanner = Scanner.from_batches(source=self.batches(), schema = self.schema)
        self.class_tracker.track_object(scanner, resolution_level=10)
        ds.write_dataset(scanner, datadir, partitioning=self.parquet_partitioning, format="parquet")
        return


def run_write():
    test = PyArrowTest()
    test.export()


def main():
    parser = ArgumentParser (description="Import/Export resources")
    parser.add_argument("--sql", "-s",
                    help="SQL Query or a path to a file containing SQL query",
                    required=False)
    parser.add_argument("--db",
                        help="Path to a database connection parameters file",
                        default="database.ini",
                        required=False)
    parser.add_argument("--connection", "-c",
                        help="Section in the database connection parameters file",
                        default="nsaph2",
                        required=False)
    parser.add_argument("--action", "-a",
                        help = "Read or Write",
                        choices=["w","r"],
                        required=True)

    arguments = parser.parse_args()
    if arguments.action == "w":
        run_write()
    elif arguments.action == "r":
        if not arguments.sql or not arguments.db or not arguments.connection:
            raise ValueError("Not all arguments are provided")
        if os.path.isfile(arguments.sql):
            with open(arguments.sql) as inp:
                sql = '\n'.join([line for line in inp])
        else:
            sql = arguments.sql
        with Connection(arguments.db, arguments.connection) as db:
            run_query (db, sql=sql)
    else:
        raise ValueError("Invalid action: " + arguments.action)


if __name__ == '__main__':
    main()
