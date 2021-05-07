import datetime
import logging
from collections import OrderedDict
from typing import Optional

from sortedcontainers import SortedDict
from psycopg2.extras import execute_values

from nsaph.data_model.utils import split, DataReader
from nsaph.fips import fips_dict


def compute(how, row):
    try:
        value = eval(how["eval"])
    except:
        value = None
    return value


class Inserter:

    def __init__(self, domain, root_table_name, reader: DataReader, connection, page_size = 1000):
        self.tables = []
        self.page_size = page_size
        self.ready = False
        self.reader = reader
        self.connection = connection
        self.domain = domain
        table = domain.find(root_table_name)
        self.prepare(root_table_name, table)
        self.times = dict()

    def prepare(self, name, table):
        self.tables.append(self.Table(self, name, table))
        if table["children"]:
            for child_table in table["children"]:
                child_table_def = table["children"][child_table]
                if child_table_def.get("hard_linked"):
                    self.tables.append(self.Table(self, child_table, child_table_def))
        self.ready = True
        for table in self.tables:
            logging.info(table.insert)

    def next(self) -> int:
        l: int = 0
        batch = {
            table.name: [] for table in self.tables
        }
        t0 = datetime.datetime.now()
        for row in self.reader.rows():
            is_valid = True
            for table in self.tables:
                records = table.read(row)
                if records is None:
                    is_valid = False
                    logging.warning("Illegal row #{:d}: {}".format(l, str(row)))
                    break
                batch[table.name].extend(records)
            if not is_valid:
                continue
            l += 1
            if l >= self.page_size:
                break
        t = datetime.datetime.now()
        self.times["read"] = self.times.get("read", 0) + (t - t0).microseconds
        t0 = t
        if len(batch) > 0:
            self.push(batch)
        else:
            self.ready = False
        t = datetime.datetime.now()
        self.times["store"] = self.times.get("store", 0) + (t - t0).microseconds
        return l

    def push(self, batch):
        for table in self.tables:
            records = batch[table.name]
            if False:
                print(table.insert)
                for r in records:
                    print("({})".format(", ".join([str(e) for e in r])))
            try:
                with self.connection.cursor() as cursor:
                    execute_values(cursor, table.insert, records, page_size=len(records))
            except Exception as x:
                logging.error("While executing: " + table.insert)
                raise x

    def get_autocommit(self):
        if self.connection.autocommit:
            return "ON"
        return "OFF"

    def import_file(self, path, limit = None, log_step = 1000000):
        logging.info("Autocommit is: " + self.get_autocommit())
        l: int = 0
        l1 = l
        t0 = datetime.datetime.now()
        self.times.clear()
        while self.ready:
            l += self.next()
            if l - l1 >= log_step:
                l1 = l
                t1 = datetime.datetime.now()
                rate = float(l) / (t1 - t0).seconds
                rt = self.times["read"]
                st = self.times["store"]
                t = rt + st
                logging.info(
                    "Records imported from {}: {:d}; rate: {:f} rec/sec; read: {:d}% / store: {:d}%"
                        .format(path, l, rate, int(rt*100/t), int(st*100/t)))
            if limit and l >= limit:
                break
        logging.info("Total records imported from {}: {:d}".format(path, l))
        return

    class Table:
        def __init__(self, parent, name:str, table: dict):
            self.reader = parent.reader
            self.name = parent.domain.fqn(name)
            self.mapping = None
            self.range_columns = None
            self.computes = None
            self.pk = None
            self.insert = None
            self.range = None
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
                source_index = self.reader.columns.index(source)
                self.mapping[source_index] = name
            inverse_mapping = {
                item[1]: item[0] for item in self.mapping.items()
            }
            for c in self.computes.values():
                parameters = [
                    inverse_mapping[p] for p in c["parameters"]
                ]
                arguments = ["empty"] + [
                    "row[{:d}]".format(p) for p in parameters
                ]
                code = c["code"].format(*arguments)
                c["eval"] = code
            self.pk = {i for i in self.mapping if self.mapping[i] in primary_key}
            cc = [self.mapping[i] for i in self.mapping]
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
            for i in self.mapping:
                value = row[i]
                if not value:
                    if i in self.pk:
                        return False
                    else:
                        record.append(None)
                else:
                    record.append(value)
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


