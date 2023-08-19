#  Copyright (c) 2023. Harvard University
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

"""
Module to create list of tables from the Domain Data Model
"""
import re
import sys
from typing import Dict, Optional, List

from nsaph_utils.utils.io_utils import as_dict


def fqn(schema: str, name: str) -> str:
    return schema + '.' + name


def qstr(s):
    return f'"{str(s)}"'

class Table:
    @classmethod
    def get_aggregation(cls, schema: str, name: str, create_block: dict):
        if "group by" not in create_block:
            return None
        group_by_block :Dict[str, Dict] = create_block["group by"]
        assert isinstance(group_by_block, list)
        parent = create_block["from"]
        assert isinstance(parent, str)
        return Aggregation(schema, name, parent, group_by_block)

    def __init__(self, schema: str, name: str, table_block: dict):
        print("Adding: " + fqn(schema, name))
        Domain.tables[fqn(schema, name)] = self
        self.name = name
        self.schema = schema
        self.block = table_block
        self.aggregation:Optional[Aggregation] = None
        self.union:Optional[Union] = None
        self.join:Optional[Join] = None
        self.parent = None
        self.type: str = "Table"
        self.master = None
        if "create" in table_block:
            create_block:Dict[str, Dict] = table_block["create"]
            self.aggregation = self.get_aggregation(schema, name, create_block)
            if "type" in create_block:
                self.type = str(create_block["type"])
            if "from" in create_block:
                from_block = create_block["from"]
                if self.aggregation is None:
                    if isinstance(from_block, list):
                        self.union = Union(schema, from_block)
                    elif isinstance(from_block, str):
                        if "join" in from_block:
                            self.join = Join(schema, from_block)
                        else:
                            tname = fqn(schema, from_block)
                            self.master = Domain.tables[tname]
        if "children" in table_block:
            children_block = table_block["children"]
            assert isinstance(children_block, dict)
            for child in children_block:
                child_table = Table(schema, child, children_block[child])
                child_table.parent = self
                relation = Relation("parent-child", self, child_table)
                Domain.relations.append(relation)
        if self.aggregation is not None:
            relation = Relation("aggregation", self.aggregation.parent, self, self.aggregation.on)
        elif self.parent is not None:
            relation = Relation("parent-child", self.parent, self)
        elif self.master is not None:
            relation = Relation("transformation", self.master, self)
        elif self.join is not None:
            for t in self.join.tables:
                relation = Relation("join", t, self, self.join.on)
                Domain.relations.append(relation)
            relation = None
        elif self.union is not None:
            for t in self.union.tables:
                relation = Relation("union", t, self)
                Domain.relations.append(relation)
            relation = None
        else:
            relation = None
        if relation is not None:
            Domain.relations.append(relation)

    def __str__(self) -> str:
        return self.type + " " + fqn(self.schema, self.name)


class Aggregation:
    def __init__(self, schema: str, name: str, parent: str, columns: List):
        self.table = Domain.tables[fqn(schema, name)]
        if '.' not in parent:
            parent = fqn(schema, parent)
        self.parent = Domain.tables[parent]
        self.columns = columns
        self.on = "On " + ", ".join(self.columns)


class Union:
    def __init__(self, schema: str, tables: List[str]):
        self.tables = []
        for t in tables:
            if '.' not in t:
                t = fqn(schema, t)
            pattern = re.compile(t.replace('*', ".*"))
            for t2 in Domain.tables:
                if pattern.fullmatch(t2):
                    self.tables.append(Domain.tables[t2])
        return


class Join:
    def __init__(self, schema: str, s: str):
        natural_join = "natural" in s
        s = s.lower()
        for keyword in ["natural", "left", "right", "full"]:
            s = s.replace(keyword, '')
        i = s.find("on")
        if i > 0:
            s = s[:i]
            self.on = s[i:]
        elif natural_join:
            self.on = "natural"
        else:
            self.on = None
        tables = s.split("join")
        self.tables = []
        for table in tables:
            table = table.strip()
            if '.' not in table:
                table = fqn(schema, table)
            self.tables.append(Domain.tables[table])
        return


class Relation:
    def __init__(self, reltype: str, x: Table, y: Table, data = None):
        self.type = reltype
        self.x = x
        self.y = y
        self.data = data

    def label(self):
        return self.data if self.data is not None else ''

    def as_edge_attr(self):
        if self.data is None:
            return qstr(self.type)
        return f'"{self.type} {self.label()}"'

    def __str__(self) -> str:
        data = self.label()
        return f'{self.type}: {str(self.x)} => {str(self.y)} {data}'


class Domain:
    tables: Dict[str, Table] = dict()
    relations: List[Relation] = []

    @classmethod
    def add(cls, path):
        yml = as_dict(path)
        for d in yml:
            print("Processing: " + d)
            domain = yml[d]
            if "schema" in domain:
                schema = domain["schema"]
            else:
                schema = domain
            for t in domain["tables"]:
                Table(schema, t, domain["tables"][t])

    @classmethod
    def list(cls):
        print("Tables")
        for t in cls.tables:
            print("\t" + str(cls.tables[t]))
        print("Relations")
        for r in cls.relations:
            print("\t" + str(r))

    @classmethod
    def to_dot(cls, out = sys.stdout):
        print("digraph {", file=out)
        for t in cls.tables:
            node_id = qstr(cls.tables[t])
            print(f"\t{node_id};", file=out)
        for r in cls.relations:
            node1 = qstr(r.x)
            node2 = qstr(r.y)
            label = r.as_edge_attr()
            print(f"\t{node1} -> {node2} [label={label}];", file=out)
        print("}", file=out)


if __name__ == '__main__':
    for a in sys.argv[1:]:
        Domain.add(a)
    Domain.list()
    Domain.to_dot()

