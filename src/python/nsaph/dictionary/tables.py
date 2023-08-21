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
import os.path
import re
import sys
from typing import Dict, Optional, List

import nsaph.data_model.domain
from nsaph.dictionary.columns import Column
from nsaph_utils.utils.io_utils import as_dict


def fqn(schema: str, name: str) -> str:
    return schema + '.' + name


def qstr(s):
    return f'"{str(s)}"'


def hstr(s):
    s = s.replace("\n", "<br/>")
    return f'<{str(s)}>'


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
        DomainDict.tables[fqn(schema, name)] = self
        self.name = name
        self.schema = schema
        self.block = table_block
        self.aggregation:Optional[Aggregation] = None
        self.union:Optional[Union] = None
        self.join:Optional[Join] = None
        self.parent = None
        self.type: str = "Table"
        self.master = None
        create_block = None
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
                            self.master = DomainDict.tables[tname]
        if "children" in table_block:
            children_block = table_block["children"]
            assert isinstance(children_block, dict)
            for child in children_block:
                child_table = Table(schema, child, children_block[child])
                child_table.parent = self
                relation = Relation("parent-child", self, child_table)
                DomainDict.relations.append(relation)
        if self.aggregation is not None:
            relation = Relation("aggregation", self.aggregation.parent, self, self.aggregation.on)
        elif self.parent is not None:
            relation = Relation("parent-child", self.parent, self)
        elif self.master is not None:
            relation = Relation("transformation", self.master, self)
        elif self.join is not None:
            for t in self.join.tables:
                relation = Relation("join", t, self, self.join.on)
                DomainDict.relations.append(relation)
            relation = None
        elif self.union is not None:
            for t in self.union.tables:
                relation = Relation("union", t, self)
                DomainDict.relations.append(relation)
            relation = None
        else:
            relation = None
        if relation is not None:
            DomainDict.relations.append(relation)
        self.columns = dict()
        if "columns" in table_block:
            columns_block = table_block["columns"]
            for column_block in columns_block:
                column = Column(fqn(self.schema, self.name), column_block)
                self.columns[column.name.lower()] = column
        if self.master is not None and create_block and "select" in create_block:
            identifiers = nsaph.data_model.domain.Domain.get_select_from(table_block)
            select_block = create_block["select"]
            if '*' in select_block:
                for c in self.master.columns:
                    p_column = self.master.columns[c]
                    column_block = {c: p_column.block}
                    column = Column(fqn(self.schema, self.name), column_block)
                    column.expression = None
                    column.copied = True
                    self.columns[column.name.lower()] = column
        self.predecessors = None

    def __str__(self) -> str:
        return self.type + " " + fqn(self.schema, self.name)

    def get_predecessors(self):
        if self.predecessors is None:
            self.predecessors = []
            for relation in DomainDict.relations:
                if relation.y == self:
                    self.predecessors.append(relation)
        return self.predecessors

    def get_column_links(self, column: Column) -> List:
        pc = column.predecessors
        pt = self.get_predecessors()
        links = []
        for c in pc:
            c = c.lower()
            for r in pt:
                t = r.x
                if c in t.columns:
                    link = ColumnLink(r, t.columns[c], column)
                    links.append(link)
        return links

    def calculate_column_lineage(self, column: Column, nodes: List[Column], edges: List):
        nodes.append(column)
        links = self.get_column_links(column)
        for link in links:
            edges.append(link)
            link.relation.x.calculate_column_lineage(link.x, nodes, edges)
        return

    def column_lineage_to_dot(self, column_name: str, out):
        column = self.columns[column_name]
        nodes: List[Column] = []
        edges: List[ColumnLink] = []
        self.calculate_column_lineage(column, nodes, edges)
        print("digraph {", file=out)
        for node in nodes:
            node_id = qstr(node.fqn)
            node_label = '<' + node.describe_html() + '>'
            print(f"\t{node_id} [label={node_label},shape=box];", file=out)
        for edge in edges:
            node1 = qstr(edge.x.fqn)
            node2 = qstr(edge.y.fqn)
            attrs = edge.relation.as_edge_attr()
            if edge.y.copied:
                attrs["label"] = "Copied"
            elif "transformation" in attrs["label"]:
                attrs["label"] = attrs["label"].replace("transformation", "transformed")
            elif "aggregation" in attrs["label"]:
                attrs["label"] = attrs["label"].replace("aggregation", "aggregated")
            alist = [f'{key}={attrs[key]}' for key in attrs]
            astring = ','.join(alist)
            print(f"\t{node1} -> {node2} [{astring}];", file=out)
        print("}", file=out)


class Aggregation:
    def __init__(self, schema: str, name: str, parent: str, columns: List):
        self.table = DomainDict.tables[fqn(schema, name)]
        if '.' not in parent:
            parent = fqn(schema, parent)
        self.parent = DomainDict.tables[parent]
        self.columns = columns
        self.on = "On " + ", ".join(self.columns)


class Union:
    def __init__(self, schema: str, tables: List[str]):
        self.tables = []
        for t in tables:
            if '.' not in t:
                t = fqn(schema, t)
            pattern = re.compile(t.replace('*', ".*"))
            for t2 in DomainDict.tables:
                if pattern.fullmatch(t2):
                    self.tables.append(DomainDict.tables[t2])
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
            self.tables.append(DomainDict.tables[table])
        return


class Relation:
    def __init__(self, reltype: str, x: Table, y: Table, data = None):
        self.type = reltype
        self.x = x
        self.y = y
        self.data = data

    def label(self):
        return self.data if self.data is not None else ''

    def as_edge_attr(self) -> Dict:
        attrs = dict()
        if self.data is None:
            attrs["label"] = '\t' + qstr(self.type)
        else:
            attrs["label"] = f'"\t{self.type} {self.label()}"'
        if self.type == "aggregation":
            attrs["penwidth"] = 5
            attrs["color"] = "blue"
        elif self.type == "union":
            attrs["penwidth"] = 2
            attrs["color"] = "chocolate"
        elif self.type == "parent-child":
            attrs["penwidth"] = 2
            attrs["color"] = "darkorchid"
        else:
            attrs["penwidth"] = 1
            attrs["color"] = "black"
        return attrs

    def __str__(self) -> str:
        data = self.label()
        return f'{self.type}: {str(self.x)} => {str(self.y)} {data}'


class ColumnLink:
    def __init__(self, r: Relation, x: Column, y: Column):
        self.relation = r
        self.x = x
        self.y = y


class DomainDict:
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
            attrs = r.as_edge_attr()
            alist = [f'{key}={attrs[key]}' for key in attrs]
            astring = ','.join(alist)
            print(f"\t{node1} -> {node2} [{astring}];", file=out)
        print("}", file=out)

    @classmethod
    def generate_graphs(cls, of: str = None, print_lineage = True, generate_image = True):
        if of is None:
            of = "tables.dot"
        with open(of, "w") as graph:
            cls.to_dot(graph)
        if generate_image:
            os.system(f"dot -T png -O {of}")
        if print_lineage:
            n = 0
            if not os.path.isdir("tables"):
                os.mkdir("tables")
            for t in cls.tables:
                d = os.path.join("tables", t)
                if not os.path.isdir(d):
                    os.mkdir(d)
                table = cls.tables[t]
                for c in table.columns:
                    f = os.path.join(d, c) + ".dot"
                    with open(f, "w") as graph:
                        table.column_lineage_to_dot(c, graph)
                    if generate_image:
                        os.system(f"dot -T png -O {f}")
                        n += 1
                        if (n % 10) == 0:
                            print('*', end='')
                            if (n % 300) == 0:
                                print()
                                print(t)



if __name__ == '__main__':
    for a in sys.argv[1:]:
        DomainDict.add(a)
    DomainDict.list()
    DomainDict.generate_graphs(generate_image=False)


