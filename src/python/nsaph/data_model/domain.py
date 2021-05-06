import logging

from nsaph_utils.utils.io_utils import as_dict

from nsaph.data_model.utils import basename, split, DataReader
from nsaph.model import index_method, INDEX_NAME_PATTERN, INDEX_DDL_PATTERN


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

    def init(self) -> None:
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

    def find(self, table, root = None):
        if not root:
            tables = self.spec[self.domain]["tables"]
        else:
            tables = root["children"]
        if table in tables:
            return tables[table]
        for t in tables:
            d = self.find(table, tables[t])
            if d is not None:
                return d
        return None


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
            fk_name = "{}_to_{}".format(basename(table), ptable)
            fk_column_list = ", ".join(fk_columns)
            fk = "CONSTRAINT {name} FOREIGN KEY ({columns}) REFERENCES {parent} ({columns})"\
                .format(name=fk_name, columns=fk_column_list, parent=self.fqn(ptable))
            for column in pdef["columns"]:
                c, _ = split(column)
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
        n, c = split(column)
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
        cname, column = split(column)
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

    @staticmethod
    def is_generated(column):
        if not isinstance(column, dict):
            return False
        if "source" not in column:
            return False
        if not isinstance(column["source"], dict):
            return False
        if "type" not in column["source"]:
            return False
        return "generated" == column["source"]["type"].lower()

    def column_spec(self, column) -> str:
        name, column = split(column)
        t = column.get("type", "VARCHAR")
        if self.is_generated(column):
            if not "code" in column["source"]:
                raise Exception("Generated column must specify the compute code")
            code = column["source"]["code"]
            return "{} {} {}".format(name, t, code)
        return "{} {}".format(name, t)

    def create_tables(self, cursor):
        for statement in self.ddl:
            logging.info(statement)
        sql = "\n".join(self.ddl)
        cursor.execute(sql)
        logging.info("Tables created")