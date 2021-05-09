import argparse
import json
import os
from collections import OrderedDict
from pathlib import Path
from typing import Dict, List, Set

import yaml
from nsaph_utils.utils.io_utils import as_dict

from nsaph import init_logging
from nsaph.db import Connection, ResultSet


def fqn(t):
    return t
    #return '"public"."{}"'.format(t)


class Query:
    def __init__(self, user_request, connection):
        request = as_dict(user_request)
        src = Path(__file__).parents[3]
        registry_path = os.path.join(src, "yml", "gridmet.yaml")
        with open(registry_path) as rf:
            self.registry = yaml.safe_load(rf)
        self.request = request
        if isinstance(connection, Connection):
            self.connection = connection
        else:
            self.connection = Connection(*connection)
        self.cursor = None
        self.sql = None
        self.metadata = None

    def execute(self):
        self.cursor.execute(self.sql)
        return ResultSet(cursor=self.cursor, metadata=self.metadata)

    def prepare(self):
        connection = self.connection.connect()
        self.metadata = self.connection.get_database_types()
        self.cursor = connection.connections()
        self.sql = generate(self.registry, self.request)

    def __enter__(self):
        self.prepare()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.connection.close()


def find_tables(column: str, tables: Dict) -> List[str]:
    children = dict()
    result = []
    for t in tables:
        tdef = tables[t]
        if column in tdef["columns"]:
            result.append(t)
        if "children" in tdef:
            children = tdef["children"]
    if result:
        return result
    if children:
        return find_tables(column, children)
    raise Exception("Column named {} is not found in any of teh defined tables"
                    .format(column))


def generate_select(variables: Dict) -> str:
    select = ["{}.{}".format(variables[v][0], v) for v in variables]
    return "SELECT \n\t" +  ",\n\t".join(select) + "\n"


def collect_tables(source: Dict, tables: Dict, result: Dict):
    join_columns = dict()
    for t in source:
        if t not in tables:
            continue
        result[t] = []
        for c in tables[t]:
            if c in join_columns:
                join_columns[c].add(t)
            else:
                join_columns[c] = {t}
    for t in result:
        for c in tables[t]:
            if c not in join_columns:
                continue
            if len(join_columns[c]) < 2:
                continue
            for t2 in join_columns[c]:
                if t2 != t:
                    result[t].append((c, t2))
    for t in result:
        if "children" in source[t]:
            child_tables = source[t]["children"]
            children_result = OrderedDict()
            collect_tables(child_tables, tables, children_result)
            for child in children_result:
                parent = child_tables[child]["parent"]
                children_result[child].append((parent, t))
            result.update(children_result)
    return


def generate_from(variables: Dict, aux: Dict, source: Dict) -> str:
    columns = dict(variables)
    columns.update(aux)
    tables = {
        columns[v][0]: set() for v in columns
    }
    for c in columns:
        for t in columns[c]:
            if t in tables:
                tables[t].add(c)

    from_tables = OrderedDict()
    collect_tables(source, tables, from_tables)
    sql = "FROM \n\t"
    from_elements = []
    tt = set()
    for t in from_tables:
        element = fqn(t)
        joins = from_tables[t]
        if joins:
            element += " ON "
            join_clause = []
            for join in joins:
                c, t2 = join
                if t2 not in tt:
                    continue
                join_clause.append("{t1}.{c} = {t2}.{c}"
                                   .format(t1=t, t2=t2, c=c))
            element += " AND ".join(join_clause)
        tt.add(t)
        from_elements.append(element)
    sql += "\n\t   JOIN ".join(from_elements)
    return sql


def generate_where(variables: Dict, tables: Dict, used_tables: Set) -> str:
    where = []
    for v in variables:
        tt = find_tables(v, tables)
        tt = [t for t in tt if t in used_tables]
        if not tt:
            raise Exception("System Error")
        t = tt[0]
        expr = variables[v]
        if isinstance(expr, str):
            condition = "{}.{} = '{}'".format(t, v, expr)
            where.append(condition)
        elif isinstance(expr, list):
            values = ", ".join(["'{}'".format(str(e)) for e in expr])
            condition = "{}.{} IN ({})".format(t, v, values)
            where.append(condition)
        elif isinstance(expr, dict):
            for field in expr:
                e = expr[field]
                condition = "EXTRACT ({} FROM {}.{}) = {}".format(field,
                                                                  t, v, e)
                where.append(condition)
    return "WHERE \n\t" + "\n\tAND ".join(where) + "\n"


def all_tables(variables: Dict) -> Set:
    tables = set()
    for v in variables:
        for t in variables[v]:
            tables.add(t)
    return tables


def reduce_tables(variables: Dict) -> bool:
    tables = all_tables(variables)
    reduced = False
    for t in tables:
        required = False
        for v in variables:
            if t not in variables[v]:
                continue
            if len(variables[v]) == 1:
                required = True
                break
        if not required:
            for v in variables:
                if t in variables[v]:
                    variables[v].remove(t)
                    reduced = True
    if reduced:
        reduced = reduce_tables(variables)
    return reduced


def generate_order_by(request: dict) -> str:
    group = None
    if "package" in request:
        if "group" in request["package"]:
            group = request["package"]["group"]
    if not group:
        return ""
    if isinstance(group, str):
        return "\nORDER BY {}".format(group)
    if isinstance(group, list):
        return "\nORDER BY {}".format(", ".join(group))
    else:
        raise Exception("Invalid specification for grouping: ".
                        format(str(group)))


def generate(system, user) -> (str, List):
    source_name = user["source"]
    source = system[source_name]

    variables = dict()
    for v in user["variables"]:
        t = find_tables(v, source["tables"])
        variables[v] = t
    reduce_tables(variables)

    select = generate_select(variables)
    tables = all_tables(variables)
    filters = dict()
    for v in user["restrict"]:
        if v in variables:
            continue
        t = find_tables(v, source["tables"])
        if len(set(t) & tables) > 0:
            continue
        filters[v] = t
    reduce_tables(filters)
    tables.update(all_tables(filters))
    source_tables = source["tables"]

    from_clause = generate_from(variables, filters, source_tables)
    where = generate_where(user["restrict"], source_tables, tables)
    order_by = generate_order_by(user)

    sql = select + "\n" + from_clause + "\n" + where + order_by
    return sql


if __name__ == '__main__':
    init_logging()
    parser = argparse.ArgumentParser (description="Create table and load data")
    parser.add_argument("--request", "-r",
                        help="Path to a table definition file for a table",
                        default=None,
                        required=False)
    parser.add_argument("--db",
                        help="Path to a database connection parameters file",
                        default="database.ini",
                        required=False)
    parser.add_argument("--section",
                        help="Section in the database connection parameters file",
                        default="postgresql",
                        required=False)

    args = parser.parse_args()
    if not args.request:
        d = os.path.dirname(__file__)
        args.request = os.path.join(d, "../../../yml/ellen.yaml")

    with Query(args.request, (args.db, args.section)) as q:
        print(q.sql)
        count = 0
        rs = q.execute()
        for r in rs:
            count += 1
            print(r)
    print("Count rows: {:d}".format(count))


