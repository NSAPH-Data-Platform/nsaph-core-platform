import os
from pathlib import Path

from nsaph_utils.utils.io_utils import as_dict
from nsaph.model import INDEX_DDL_PATTERN, INDEX_NAME_PATTERN, index_method


class Domain:
    def __init__(self, spec, name):
        self.domain = name
        self.spec = as_dict(spec)
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


    def generate_ddl(self) -> None:
        self.ddl = []
        tables = self.spec[self.domain]["tables"]
        nodes = {t: tables[t] for t in tables}
        for node in nodes:
            self.ddl_for_node((node, nodes[node]))
        return

    def ddl_for_node(self, node, parent = None) -> None:
        table, definition = node
        features = []

        fk = None
        if parent is not None:
            ptable, pdef = parent
            if "primary_key" not in pdef:
                raise Exception("Parent table {} must define primary key".format(ptable))
            fk_columns = pdef["primary_key"]
            fk_name = "{}_to_{}".format(table, ptable)
            fk_column_list = ", ".join(fk_columns)
            fk = "CONSTRAINT {name} FOREIGN KEY ({columns}) REFERENCES {parent} ({columns})"\
                .format(name=fk_name, columns=fk_column_list, parent=ptable)
            for column in pdef["columns"]:
                c, _ = self.split(column)
                if c in fk_columns:
                    features.append(self.column_spec(column))

        columns = definition["columns"]
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
            iname = INDEX_NAME_PATTERN.format(table = table, column = cname)
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



if __name__ == '__main__':
    src = Path(__file__).parents[2]
    registry_path = os.path.join(src, "yml", "medicaid.yaml")
    domain = Domain(registry_path, "medicaid")
    domain.generate_ddl()
    for statement in domain.ddl:
        print(statement)
    for index in domain.indices:
        print(index)