import csv
import logging
import os
import sys
from pathlib import Path

from nsaph_utils.utils.io_utils import as_dict, fopen
from psycopg2.extras import execute_values

from nsaph import init_logging
from nsaph.db import Connection
from nsaph.model import INDEX_DDL_PATTERN, INDEX_NAME_PATTERN, index_method


def compute_column(column: dict) -> str:
    pass


class Domain:
    def __init__(self, spec, name):
        self.domain = name
        self.spec = as_dict(spec)
        if "schema" in self.spec[self.domain]:
            self.schema = self.spec[self.domain]["schema"]
        elif "schema" in self.spec:
            self.schema = self.spec["schema"]
        else:
            self.schema = None
        self.indices = []
        self.ddl = []
        self.page_size = 1000
        self.conucrrent_indices = False
        index_policy = self.spec[self.domain].get("index")
        if index_policy is None or index_policy in ["selected"]:
            self.index_policy = "selected"
        elif index_policy in ["explicit"]:
            self.index_policy = "explicit"
        elif index_policy in ["all", "unless excluded"]:
            self.index_policy = "all"
        else:
            raise Exception("Invalid indexing policy: " + index_policy)

    def generate_ddl(self) -> None:
        if self.schema:
            self.ddl = ["CREATE SCHEMA IF NOT EXISTS {};".format(self.schema)]
        else:
            self.ddl = []
        tables = self.spec[self.domain]["tables"]
        nodes = {t: tables[t] for t in tables}
        for node in nodes:
            self.ddl_for_node((node, nodes[node]))
        return

    def fqn(self, table):
        if self.schema:
            return self.schema + '.' + table
        return table

    @staticmethod
    def basename(table):
        return table.split('.')[-1]

    def ddl_for_node(self, node, parent = None) -> None:
        table, definition = node
        columns = definition["columns"]
        features = []
        table = self.fqn(table)
        fk = None
        if parent is not None:
            ptable, pdef = parent
            if "primary_key" not in pdef:
                raise Exception("Parent table {} must define primary key".format(ptable))
            fk_columns = pdef["primary_key"]
            fk_name = "{}_to_{}".format(self.basename(table), ptable)
            fk_column_list = ", ".join(fk_columns)
            fk = "CONSTRAINT {name} FOREIGN KEY ({columns}) REFERENCES {parent} ({columns})"\
                .format(name=fk_name, columns=fk_column_list, parent=self.fqn(ptable))
            for column in pdef["columns"]:
                c, _ = self.split(column)
                if c in fk_columns:
                    columns.append(column)

        features.extend([self.column_spec(column) for column in columns])

        if "primary_key" in definition:
            pk_columns = definition["primary_key"]
            pk = "PRIMARY KEY ({})".format(", ".join(pk_columns))
            features.append(pk)

        if fk:
            features.append(fk)

        create_table = "CREATE TABLE {name} (\n\t{features}\n);".format(name=table, features=",\n\t".join(features))
        self.ddl.append(create_table)

        for column in columns:
            if not self.need_index(column):
                continue
            self.indices.append(self.get_index_ddl(table, column))

        if "children" in definition:
            children = {t: definition["children"][t] for t in definition["children"]}
            for child in children:
                self.ddl_for_node((child, children[child]), parent=node)

    def need_index(self, column) -> bool:
        if self.index_policy == "all":
            return True
        n, c = self.split(column)
        if "index" in c:
            return True
        if self.index_policy == "selected":
            return index_method(n) is not None
        return False

    def get_index_ddl(self, table, column) -> str:
        if self.conucrrent_indices:
            option = "CONCURRENTLY"
        else:
            option = ""

        method = None
        iname = None
        if "index" in column:
            index = column["index"]
            if isinstance(index, str):
                iname = index
            else:
                if "name" in index:
                    iname = index["name"]
                if "using" in index:
                    method = index["using"]
        cname, column = self.split(column)
        if method:
            pass
        elif self.is_array(column):
            method = "GIN"
        else:
            method = "BTREE"
        if not iname:
            iname = INDEX_NAME_PATTERN.format(table = table.split('.')[-1], column = cname)
        return INDEX_DDL_PATTERN.format(
            option = option,
            name = iname,
            table = table,
            column = cname,
            method = method
        ) + ";"


    @staticmethod
    def is_array(column) -> bool:
        if "type" not in column:
            return False
        type = column["type"]
        return type.endswith("]")

    def split(self, column) -> (str, dict):
        if isinstance(column, str):
            return column, {}
        if not isinstance(column, dict):
            raise Exception("Unsupported type for column spec: " + str(column))
        name = None
        for entry in column:
            name = entry
            break
        column = column[name]
        if isinstance(column, str):
            column = {"type": column}
        if not isinstance(column, dict):
            raise Exception("Unsupported spec type for column: " + name)
        return name, column

    def column_spec(self, column) -> str:
        name, column = self.split(column)
        t = column.get("type", "VARCHAR")
        if "source" in column and column["source"] == "generated":
            if not "code" in column["source"]:
                raise Exception("Generated column must specify the compute code")
            code = column["source"]["code"]
            return "{} {} {}".format(name, t, code)
        return "{} {}".format(name, t)

    def map(self, columns, csv_columns) -> dict:
        mapping = dict()
        for c in columns:
            name, column = self.split(c)
            source = None
            if "source" in column:
                if isinstance(column["source"], str):
                    source = column["source"]
                elif isinstance(column["source"], dict):
                    t = column["source"]["name"]["type"]
                    if t == "column":
                        source = column["source"]["column"]
                    elif t == "multi_column":
                        raise Exception("Not implemented: " + t)
                    elif t == "compute":
                        raise Exception("Not implemented: " + t)
                    else:
                        raise Exception("Invalid source for column {}: {}".format(name, str(column["source"])))
                else:
                    raise Exception("Invalid source for column {}: {}".format(name, str(column["source"])))
            else:
                for f in csv_columns:
                    if name.lower() == f.lower():
                        source = f
                        break
            if not source:
                raise Exception("Source was not found for column {}".format(name))
            source_index = csv_columns.index(source)
            mapping[source_index] = name
        return mapping

    def read(self, reader, mapping: dict, pk: set):
        l = 1
        records = []
        for row in reader:
            record = []
            is_valid = True
            for i in sorted(mapping):
                if not row[i]:
                    if i in pk:
                        logging.warning("Illegal row #{:d}: {}".format(l, str(row)))
                        is_valid = False
                        break
                    else:
                        record.append(None)
                else:
                    record.append(row[i])
            if not is_valid:
                continue
            records.append(record)
            l += 1
            if l > self.page_size:
                break
        return records

    def import_csv(self, node, path, cursor):
        if isinstance(node, str):
            table = node
            definition = self.spec[self.domain]["tables"][table]
        else:
            table, definition = node
        table = self.fqn(table)
        columns = definition["columns"]
        with fopen(path, "r") as csv_file:
            reader = csv.reader(csv_file, quoting=csv.QUOTE_NONNUMERIC)
            header = next(reader)
            csv_columns = header
            mapping = self.map(columns, csv_columns)
            column_list = ", ".join([mapping[i] for i in sorted(mapping)])
            sql = "INSERT INTO {table} ({columns})  VALUES %s".format(table=table, columns=column_list)
            l = 0
            pk = {i for i in mapping if mapping[i] in definition["primary_key"]}
            while True:
                records = self.read(reader, mapping, pk)
                l += len(records)
                if records:
                    execute_values(cursor, sql, records, page_size=self.page_size)
                if len(records) < self.page_size:
                    break
                if (l % 1000000) == 0:
                    logging.info("Records imported: {:d}".format(l))
        return

    def create_tables(self, cursor):
        for statement in self.ddl:
            logging.info(statement)
        sql = "\n".join(self.ddl)
        cursor.execute(sql)
        logging.info("Tables created")



def test(argv):
    if len(argv) > 1:
        path_to_csv = argv[1]
    else:
        path_to_csv = "data/medicaid/maxdata_demographics.csv.gz"
    src = Path(__file__).parents[2]
    registry_path = os.path.join(src, "yml", "medicaid.yaml")
    domain = Domain(registry_path, "medicaid")
    domain.generate_ddl()
    for index in domain.indices:
        print(index)
    with Connection("database.ini", "nsaph2") as connection:
        cursor = connection.cursor()
        domain.create_tables(cursor)
        domain.import_csv("demographics", path_to_csv, cursor)
        connection.commit()


if __name__ == '__main__':
    init_logging()
    test(sys.argv)