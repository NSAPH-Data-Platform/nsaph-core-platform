import os.path
import threading
from contextlib import contextmanager
from datetime import timedelta, datetime
import logging
from collections import OrderedDict
from typing import Optional, List
from pyresourcepool.pyresourcepool import ResourcePool
from timeit import default_timer as timer

from sortedcontainers import SortedDict
from psycopg2.extras import execute_values

from nsaph.data_model.domain import Domain
from nsaph.data_model.utils import split, DataReader, regex
from nsaph.fips import fips_dict
from util.executors import BlockingThreadPoolExecutor


def compute(how, row):
    try:
        value = eval(how["eval"])
    except:
        value = None
    return value


class Inserter:

    def __init__(self, domain, root_table_name, reader: DataReader, connections, page_size = 1000):
        self.tables = []
        self.page_size = page_size
        self.ready = False
        self.reader = reader
        if isinstance(connections, list):
            self.connections = ResourcePool(connections)
            self.capacity = len(connections)
        else:
            self.connections = ResourcePool([connections])
            self.capacity = 1
        self.domain = domain
        self.write_lock = threading.Lock()
        self.read_lock = threading.Lock()
        table = domain.find(root_table_name)
        self.prepare(root_table_name, table)
        self.timings = dict()
        self.timestamps = dict()
        self.current_row = 0
        self.pushed_rows = 0
        self.last_logged_row = 0

    def prepare(self, name, table):
        self.tables.append(self.Table(name, table))
        if "children" in table:
            for child_table in table["children"]:
                child_table_def = table["children"][child_table]
                if child_table_def.get("hard_linked"):
                    self.tables.append(self.Table(child_table, child_table_def))
        self.ready = True
        for table in self.tables:
            logging.info(table.insert)

    def read_batch(self):
        batch = self.Batch()
        with self.read_lock:
            for row in self.reader.rows():
                self.current_row += 1
                is_valid = True
                for table in self.tables:
                    records = table.read(row)
                    if records is None:
                        is_valid = False
                        logging.warning("Illegal row #{:d}: {}".format(self.current_row, str(row)))
                        break
                    batch.add(table.name,records)
                if not is_valid:
                    continue
                batch.inc()
                if batch.rows >= self.page_size:
                    break
        return batch

    def import_file(self, limit = None, log_step = 1000000) -> int:
        logging.info(
            "Autocommit is: {}. Page size = {:d}. Writer threads: {:d}. Logging progress every {:d} records"
                .format(self.get_autocommit(), self.page_size, self.capacity, log_step)
        )
        self.reset_timer()
        self.stamp_time("start")
        if self.capacity > 1:
            max_tasks = self.capacity * 2 + 1
            with BlockingThreadPoolExecutor(max_queue_size=max_tasks, max_workers=self.capacity + 1) as executor:
                l = self._loop(executor, limit, log_step)
                executor.wait()
        else:
            l = self._loop(None, limit, log_step)
        logging.info("Total records imported from {}: {:d}".format(self.reader.path, l))
        return l

    def _loop(self, executor: Optional[BlockingThreadPoolExecutor], limit = None, log_step = 1000000) -> int:
        l: int = 0
        l1 = l
        while self.ready:
            with self.timer("read"):
                batch = self.read_batch()
            if batch.is_empty():
                self.ready = False
            l += batch.rows
            if batch.size() > 0:
                if executor:
                    executor.submit(self.push, batch)
                else:
                    self.push(batch)
            if l - l1 >= log_step:
                self.log_progress()
                l1 = l
            if limit and l >= limit:
                break
        return l

    def push(self, batch):
        with self.connections.get_resource() as connection:
            for table in self.tables:
                records = batch[table]
                with connection.cursor() as cursor, self.timer("store"):
                    try:
                        execute_values(cursor, table.insert, records, page_size=len(records))
                    except Exception as x:
                        msg = str(x)
                        logging.error("Error {}; while executing: {} with {:d} records"
                                      .format(msg, table.insert, len(records)))
                        self.drilldown(connection, table.insert, records)
        with self.write_lock:
            self.pushed_rows += batch.rows

    @staticmethod
    def drilldown(connection, sql: str, records: list):
        if not connection.autocommit:
            connection.rollback()
        cursor = connection.cursor()
        if not records:
            raise Exception("Empty records array for " + sql)
        n = len(records[0])
        sql = sql.replace("%s", "({})".format(','.join(["%s" for _ in range(n)])))
        for record in records:
            try:
                cursor.execute(sql, record)
            except Exception as x:
                msg = str(x)
                s = ", ".join([str(v) for v in record])
                logging.error("Drill down: error {} while executing: {} ({})".format(msg, sql, s))
                raise x

    def get_autocommit(self):
        ac = [
            connection.autocommit for connection in self.connections._objects
        ]
        if all(ac):
            return "ON"
        if all([(not a) for a in ac]):
            return "OFF"
        return ", ".join(["ON" if a else "OFF" for a in ac])

    def log_progress(self):
        t0 = self.get_timestamp("start")
        t1 = self.get_timestamp("last_logged", t0)
        now = timer()
        rate1 = float(self.pushed_rows - self.last_logged_row) / (now - t1)
        rate = float(self.pushed_rows) / (now - t0)
        rt = self.get_cumulative_timing("read")
        st = self.get_cumulative_timing("store")
        t = rt + st
        rts = str(timedelta(seconds=rt))
        sts = str(timedelta(seconds=st))
        if self.capacity > 1:
            rtl = ["{:.2f}".format(t) for t in self.get_timings("read")]
            stl = ["{:.2f}".format(t) for t in self.get_timings("store")]
            rts = "{} = {}".format(" + ".join(rtl), rts)
            sts = "{} = {}".format(" + ".join(stl), sts)
        with self.write_lock:
            self.last_logged_row = self.pushed_rows
            self.stamp_time("last_logged")
            path = os.path.basename(self.reader.path)
            logging.info(
                "Records imported from {}: {:d} => {:d}; rate: {:f} rec/sec; read: {:d}% / store: {:d}%"
                    .format(path, self.current_row, self.pushed_rows, rate, int(rt*100/t), int(st*100/t))
            )
            logging.debug(
                "Current rate: {:f} rec/sec, time read: {}; time store: {}"
                    .format(rate1, rts, sts)
            )

    def Batch(self):
        return self._Batch(self)

    class _Batch:
        def __init__(self, parent):
            self.data = {
                table.name: [] for table in parent.tables
            }
            self.rows = 0

        def add(self, table: str, records: list):
            self.data[table].extend(records)

        def inc(self):
            self.rows += 1

        def size(self):
            return min([len(records) for records in self.data.values()])

        def is_empty(self) -> bool:
            return self.rows == 0 and all([len(records) == 0 for records in self.data.values()])

        def __getitem__(self, item):
            if isinstance(item, Inserter._Table):
                item = item.name
            return self.data[item]

    def Table(self, *args, **kwargs):
        return self._Table(self, *args, **kwargs)

    class _Table:
        def __init__(self, parent, name:str, table: dict):
            self.reader = parent.reader
            self.name = parent.domain.fqn(name)
            self.mapping = None
            self.range_columns = None
            self.computes = None
            self.pk = None
            self.insert = None
            self.range = None
            self.arrays = dict()
            self.audit = None
            if "invalid.records" in table:
                self.audit = table["invalid.records"]
            self.prepare(table)

        def prepare(self, table: dict):
            primary_key = table["primary_key"]
            columns = table["columns"]
            self.mapping = SortedDict()
            self.computes = SortedDict()
            self.range_columns = OrderedDict()
            for c in columns:
                name, column = split(c)
                source = None
                if "source" in column:
                    if isinstance(column["source"], str):
                        source = column["source"]
                    elif isinstance(column["source"], dict):
                        t = column["source"]["type"]
                        if t == "column":
                            source = column["source"]["column"]
                        elif t == "multi_column":
                            if not self.range:
                                raise Exception("Multi columns require range: " + name)
                            pattern = column["source"]["pattern"]
                            self.range_columns[name] = dict()
                            _, rng = self.range
                            for v in rng:
                                source = pattern.format(v)
                                self.range_columns[name][v] = self.reader.columns.index(source)
                            continue
                        elif t == "compute":
                            self.computes[name] = column["source"]
                            continue
                        elif t == "range":
                            if self.range:
                                raise Exception("Only one range is supported column {}: {}".format(name, str(column["source"])))
                            if "values" not in column["source"]:
                                raise Exception("Range must specify values for column {}: {}".format(name, str(column["source"])))
                            values = column["source"]["values"]
                            self.range = (name, values)
                            continue
                        elif t == "generated":
                            continue
                        else:
                            raise Exception("Invalid source for column {}: {}".format(name, str(column["source"])))
                    else:
                        raise Exception("Invalid source for column {}: {}".format(name, str(column["source"])))
                else:
                    for f in self.reader.columns:
                        if name.lower() == f.lower():
                            source = f
                            break
                if not source:
                    raise Exception("Source was not found for column {}".format(name))
                if Domain.is_array(column):
                    r = regex(source)
                    i0 = len(self.reader.columns)
                    i1 = 0
                    for i, clmn in enumerate(self.reader.columns):
                        if r.fullmatch(clmn):
                            self.mapping[i] = name
                            i0 = min(i0, i)
                            i1 = max(i1, i)
                    self.arrays[i0] = i1
                else:
                    source_index = self.reader.columns.index(source)
                    self.mapping[source_index] = name
            inverse_mapping = {
                item[1]: item[0] for item in self.mapping.items()
            }
            for c in self.computes.values():
                parameters = [
                    inverse_mapping[p] for p in c.get("parameters", [])
                ] + [
                    self.reader.columns.index(p) for p in c.get("columns", [])
                ]
                arguments = ["empty"] + [
                    "row[{:d}]".format(p) for p in parameters
                ]
                code = c["code"].format(*arguments)
                c["eval"] = code
            self.pk = {i for i in self.mapping if self.mapping[i] in primary_key}
            cc = []
            for i in self.mapping:
                name = self.mapping[i]
                if name not in cc:
                    cc.append(name)
            cc.extend(self.computes.keys())
            if self.range:
                cc.append(self.range[0])
            cc.extend(self.range_columns.keys())
            column_list = ", ".join(cc)
            self.insert = "INSERT INTO {table} ({columns})  VALUES %s".format(table=self.name, columns=column_list)

        def read(self, row) -> Optional[list]:
            if self.range:
                return self.read_multi(row)
            record = []
            is_valid = self.map(row,  record)
            if not is_valid:
                return None
            for c in self.computes:
                value = compute(self.computes[c], row)
                record.append(value)
            return [record]

        def map(self, row, record):
            array = None
            array_end = None
            for i in self.mapping:
                if i in self.arrays:
                    array = []
                    array_end = self.arrays[i]
                is_end = array_end == i
                value = row[i]
                if not value:
                    if i in self.pk and self.audit is None:
                        return False
                    else:
                        value = None
                if array is None:
                    record.append(value)
                else:
                    array.append(value)
                    if is_end:
                        record.append(array)
                        array = None
                        array_end = None
            return True

        def read_multi(self, row) -> Optional[list]:
            records = []
            range_column, values = self.range
            for v in values:
                record = []
                is_valid = self.map(row, record)
                if not is_valid:
                    return None
                for c in self.computes:
                    value = compute(self.computes[c], row)
                    record.append(value)
                record.append(v)
                for c in self.range_columns:
                    record.append(row[self.range_columns[c][v]])
                records.append(record)

            return records

    @contextmanager
    def timer(self, context: str):
        t0 = timer()
        yield
        t1 = timer()
        tid = threading.get_ident()
        if tid not in self.timings:
            self.timings[tid] = dict()
        self.timings[tid][context] = self.timings[tid].get(context, 0) + (t1 - t0)

    def get_thread_timing(self, context: str) -> float:
        tid = threading.get_ident()
        if tid not in self.timings:
            return 0
        return self.timings[tid].get(context, 0)

    def get_timings(self, context: str) -> List[float]:
        return [self.timings[tid].get(context, 0) for tid in self.timings]

    def get_cumulative_timing(self, context: str):
        return sum(self.get_timings(context))

    def stamp_time(self, context: str):
        self.timestamps[context] = timer()

    def get_timestamp(self, context: str, default = 0):
        return self.timestamps.get(context, default)

    def reset_timer(self):
        self.timestamps.clear()
        self.timings.clear()

