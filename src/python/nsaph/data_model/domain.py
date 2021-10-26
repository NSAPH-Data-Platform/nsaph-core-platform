"""
Domain is a Python module dedicated to
generation of various SQL required for manipulation
with data in a certain knowledge domain

See 
"""

import logging
import re
from typing import Optional, Dict, List

from nsaph_utils.utils.io_utils import as_dict

from nsaph.data_model.utils import basename, split
from nsaph.data_model.model import index_method, INDEX_NAME_PATTERN, INDEX_DDL_PATTERN


AUDIT_INSERT = """INSERT INTO {target} 
                ({columns}, REASON) 
                VALUES ({values}, '{reason}');"""

VALIDATION_PROC = """
CREATE OR REPLACE FUNCTION {schema}.validate_{source}() RETURNS TRIGGER AS ${schema}_{source}_validation$
-- Validate foreign key for {schema}.{source}
    BEGIN
        IF ({condition_pk}) THEN
            {action_pk}
            RETURN NULL;
        END IF;
        IF NOT EXISTS (
            SELECT FROM {parent_table} as t
            WHERE
                {condition_fk} 
        ) THEN
            {action_fk}
            RETURN NULL;
        END IF;
        IF EXISTS (
            SELECT FROM {schema}.{source} as t
            WHERE
                {condition_dup} 
        ) THEN
            {action_dup}
            RETURN NULL;
        END IF;
        RETURN NEW;
    END;
 
${schema}_{source}_validation$ LANGUAGE plpgsql;
"""

VALIDATION_TRIGGER = """
    CREATE TRIGGER {schema}_{name}_validation BEFORE INSERT ON {table}
        FOR EACH ROW EXECUTE FUNCTION {schema}.validate_{name}();
"""


CREATE_VIEW = """
CREATE {OBJECT} {name} AS
SELECT
    {features}
FROM {source}
"""

CREATE_VIEW_GROUP_BY = """
WHERE {not_null}
GROUP BY {id};
"""


class Domain:
    CREATE = "CREATE TABLE {name}"

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
        self.indices_by_table = dict()
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
        for s in self.spec[self.domain]:
            if s.startswith("schema."):
                self.ddl.append("CREATE SCHEMA IF NOT EXISTS {};".format(self.spec[self.domain][s]))
        tables = self.spec[self.domain]["tables"]
        nodes = {t: tables[t] for t in tables}
        for node in nodes:
            self.ddl_for_node((node, nodes[node]))
        return

    def list_columns(self, table) -> list:
        t = self.spec[self.domain]["tables"][table]
        cc = [list(c.keys())[0] for c in t["columns"]]
        return cc

    def has_hard_linked_children(self, table) -> bool:
        t = self.spec[self.domain]["tables"][table]
        if "children" in t:
            children = {c: t["children"][c] for c in t["children"]}
            for child in children:
                if child.get("hard_linked"):
                    return True
        return False

    def has(self, key: str) -> bool:
        keys = key.split('/')
        s = self.spec[self.domain]
        for k in keys:
            if k in s:
                s = s[k]
            else:
                return False
        return True

    def get(self, key: str) -> Optional[str]:
        keys = key.split('/')
        s = self.spec[self.domain]
        for k in keys:
            if k in s:
                s = s[k]
            else:
                return None
        return s

    def fqn(self, table):
        if self.schema:
            return self.schema + '.' + table
        return table

    def find(self, table: str, root = None) -> Optional[dict]:
        if not root:
            tables = self.spec[self.domain]["tables"]
        elif "children" in root:
            tables = root["children"]
        else:
            return None
        if table in tables:
            return tables[table]
        for t in tables:
            d = self.find(table, tables[t])
            if d is not None:
                return d
        return None

    def find_dependent(self, table: str) -> Dict:
        t = self.find(table)
        if t is None:
            raise LookupError("Table {} does not exist in domain {}".format(table, self.domain))
        result = {self.fqn(table): self.find(table)}
        if "children" in t:
            for child in t["children"]:
                result.update(self.find_dependent(child))
        t2 = self.spillover_table(table, t)
        if t2:
            result[t2] = ""
        return result

    def drop(self, table, connection) -> list:
        tables = self.find_dependent(table)
        with connection.cursor() as cursor:
            for t in tables:
                obj = tables[t]
                if "create" in obj:
                    kind = obj["create"]["type"]
                else:
                    kind = "TABLE"
                sql = "DROP {TABLE} IF EXISTS {} CASCADE".format(t, TABLE=kind)
                logging.info(sql)
                cursor.execute(sql)
            if not connection.autocommit:
                connection.commit()
        return [t for t in tables]

    def spillover_table(self, table, definition):
        if "invalid.records" in definition:
            validation = definition["invalid.records"]
            action = validation["action"].lower()
            spec = self.spec[self.domain]
            if action == "insert":
                target = validation["target"]
                if "schema" in target:
                    ts = target["schema"]
                    if ts[0] == '$':
                        ts = spec[ts[1:]]
                else:
                    ts = spec["schema"]
                if "table" in target:
                    tt = target["table"]
                    if tt[0] == '$':
                        tt = spec[tt[1:]]
                else:
                    tt = table
                return "{}.{}".format(ts, tt)
        return None

    def ddl_for_node(self, node, parent = None) -> None:
        table_basename, definition = node
        columns = definition["columns"]
        cnames = {split(column)[0] for column in columns}
        features = []
        table = self.fqn(table_basename)
        fk = None
        ptable = None
        fk_columns = None
        create = None
        object_type = None
        is_view = False
        if "create" in definition:
            create = definition["create"]
            if "type" in create:
                object_type = create["type"]
                is_view = "view" in object_type.lower()
        if parent is not None:
            ptable, pdef = parent
            if "primary_key" not in pdef:
                raise Exception("Parent table {} must define primary key".format(ptable))
            fk_columns = pdef["primary_key"]
            fk_name = "{}_to_{}".format(table_basename, ptable)
            fk_column_list = ", ".join(fk_columns)
            fk = "CONSTRAINT {name} FOREIGN KEY ({columns}) REFERENCES {parent} ({columns})"\
                .format(name=fk_name, columns=fk_column_list, parent=self.fqn(ptable))
            for column in pdef["columns"]:
                c, _ = split(column)
                if c in fk_columns and c not in cnames:
                    columns.append(column)

        if is_view:
            features = [self.view_column_spec(column, definition, table) for column in columns]
        else:
            features.extend([self.column_spec(column) for column in columns])

        pk_columns = None

        if is_view:
            #     CREATE {OBJECT} {name} AS
            #     SELECT
            #     {features}
            #     FROM {source}
            #     -------------------
            #     WHERE {id} IS NOT NULL
            #     GROUP BY {id}

            create_table = CREATE_VIEW.format(
                OBJECT=object_type,
                name=table,
                features = ",\n\t".join(features),
                source=create["from"]
            )
            if "group by" in create:
                group_by = ','.join(create["group by"])
                not_null = " AND ".join(["{} IS NOT NULL".format(c) for c in create["group by"]])
                create_table += CREATE_VIEW_GROUP_BY.format(id=group_by, not_null=not_null)
                reverse_map = {
                    cdef["source"]: c
                    for c, cdef in [split(column) for column in columns]
                    if cdef and "source" in cdef and isinstance(cdef["source"], str)
                }
                definition["primary_key"] = [
                    reverse_map[c] if c in reverse_map else c
                    for c in create["group by"]
                ]
            else:
                create_table = create_table.strip() + ';'
        else:
            if "primary_key" in definition:
                pk_columns = definition["primary_key"]
                pk = "PRIMARY KEY ({})".format(", ".join(pk_columns))
                features.append(pk)

            if fk:
                features.append(fk)
            create_table = (self.CREATE + " (\n\t{features}\n);").format(name=table, features=",\n\t".join(features))
        self.ddl.append(create_table)
        if "invalid.records" in definition:
            validation = definition["invalid.records"]
            action = validation["action"].lower()
            t2 = self.spillover_table(table_basename, definition)
            if t2:
                ff = [f for f in features if "CONSTRAINT" not in f and "PRIMARY KEY" not in f]
                ff.append("REASON VARCHAR(16)")
                ff.append("recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ")
                create_table = (self.CREATE + " (\n\t{features}\n);").format(name=t2, features=",\n\t".join(ff))
                self.ddl.append(create_table)
            self.add_fk_validation(table, pk_columns, action, t2, columns, ptable, fk_columns)

        for column in columns:
            if not self.need_index(column):
                continue
            ddl = self.get_index_ddl(table, column)
            self.indices.append(ddl)
            if table not in self.indices_by_table:
                self.indices_by_table[table] = []
            self.indices_by_table[table].append(ddl)

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

    def extract_generation_code(self, column, other_columns, qualifier):
        code = column["source"]["code"]
        pos1 = code.lower().index("as") + len("as")
        pos2 = code.lower().index("stored")
        expression = code[pos1:pos2].strip()
        for col in other_columns:
            n, c = split(col)
            expression = expression.replace(n, "{}.{}".format(qualifier, n))
        return expression

    def column_spec(self, column) -> str:
        name, column = split(column)
        t = column.get("type", "VARCHAR")
        if self.is_generated(column):
            if not "code" in column["source"]:
                raise Exception("Generated column must specify the compute code")
            code = column["source"]["code"]
            return "{} {} {}".format(name, t, code)
        return "{} {}".format(name, t)

    def view_column_spec(self, column, table, table_fqn) -> str:
        name, column = split(column)
        if "source" in column:
            if isinstance(column["source"], str):
                sql = column["source"]
            elif isinstance(column["source"], dict):
                sql = self.view_column_joined(column["source"], table)
            else:
                raise SyntaxError("Invalid source definition for column {}.{}".format(table_fqn, name))
            sql = sql.strip().replace('\n', "\n\t\t")
            if "{identifiers}" in sql.lower():
                idf = self.list_identifiers(table)
                s = "({})".format(', '.join(idf))
                sql = sql.format(identifiers=s)
            sql += " AS {}".format(name)
            return sql
        return name

    def find_mapped_column_name(self, column1, table2) -> str:
        tdef = self.find(table2)
        for c in tdef["columns"]:
            cname, cdef = split(c)
            if "source" in cdef:
                if cdef["source"] == column1:
                    return cname
        return column1

    def view_column_joined(self, source, table) -> str:
        select = source["select"]
        joined_table = source["from"]
        t2 = self.fqn(joined_table)
        create = table["create"]
        t1 = create["from"]
        conditions = []
        if "group by" in create:
            for c1 in create["group by"]:
                c2 = self.find_mapped_column_name(c1, joined_table)
                condition = "{t1}.{c1} = {t2}.{c2}".format(t1=t1, t2=t2, c1=c1, c2=c2)
                conditions.append(condition)
        if "where" in source:
            conditions.append(source["where"])
        sql = "(\nSELECT \n\t{what} \nFROM {table}".format(what = select, table = t2)
        if conditions:
            sql += "\nWHERE {condition}".format(condition = "\n\tAND ".join(conditions))
        sql += "\n)"
        return sql

    @staticmethod
    def list_identifiers(table):
        identifiers = []
        for (name, definition) in [split(column) for column in table["columns"]]:
            if definition.get("identifier") != True:
                continue
            if "source" in definition:
                s = definition["source"]
                source_column = re.search(r'\((.*?)[)|,]',s).group(1)
                source_column = source_column.lower().replace("distinct", "").strip()
                if source_column:
                    identifiers.append(source_column)
                else:
                    identifiers.append(s)
            else:
                identifiers.append(name)
        return identifiers

    @classmethod
    def matches(cls, create_statement, list_of_tables) -> bool:
        create_statement = create_statement.strip()
        for t in list_of_tables:
            if create_statement.startswith(cls.CREATE.format(name=t)):
                return True
            for create in ["CREATE TRIGGER", "CREATE OR REPLACE FUNCTION"]:
                if create_statement.startswith(create) and t in create_statement:
                    return True

        return False

    def create(self, connection, list_of_tables = None):
        with connection.cursor() as cursor:
            if list_of_tables:
                statements = [
                    s for s in self.ddl if self.matches(s, list_of_tables) or s.startswith("CREATE SCHEMA")
                ]
            else:
                statements = self.ddl
            for statement in statements:
                logging.info(statement)
            sql = "\n".join(statements)
            cursor.execute(sql)
            if not connection.autocommit:
                connection.commit()
            logging.info("Schema and all tables for domain {} have been created".format(self.domain))

    def add_fk_validation(self, table, pk, action, target, columns, pt, fk_columns):
        columns_as_dict = {}
        for c in columns:
            name, definition = split(c)
            columns_as_dict[name] = definition
        if action == "insert":
            cc = []
            for c in columns:
                name, definition = split(c)
                if not self.is_generated(definition):
                    cc.append(name)
            vv = ["NEW.{}".format(c) for c in cc]
            actions = [
                AUDIT_INSERT.format(target=target, columns=','.join(cc), values=','.join(vv), reason=r)
                for r in ["DUPLICATE", "FOREIGN KEY", "PRIMARY KEY"]
            ]
        elif action == "ignore":
            actions = ["", "", ""]
        else:
            raise Exception("Invalid action on validation for table {}: {}".format(table, action))
        conditions = []
        for constraint in [pk, fk_columns]:
            cols = []
            for c in constraint:
                column = columns_as_dict[c]
                if self.is_generated(column):
                    exp = self.extract_generation_code(column, columns, "NEW")
                    cols.append("{exp} = t.{c}".format(exp=exp,c=c))
                else:
                    cols.append("NEW.{c} = t.{c}".format(c=c))
            conditions.append("\n\t\t\t\tAND ".join(cols))
        conditions.append("\n\t\t\t\tOR ".join(["NEW.{c} IS NULL ".format(c=c) for c in pk]))
        # OR NEW.{c} = ''
        t = basename(table)

        sql = VALIDATION_PROC.format(schema=self.schema, source=t, parent_table=self.fqn(pt),
                                     condition_dup = conditions[0], action_dup = actions[0],
                                     condition_fk = conditions[1], action_fk = actions[1],
                                     condition_pk = conditions[2], action_pk = actions[2],
        )
        self.ddl.append(sql)
        sql = VALIDATION_TRIGGER.format(schema=self.schema, name=t, table=table).strip()
        self.ddl.append(sql)
