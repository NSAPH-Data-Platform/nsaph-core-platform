#  Copyright (c) 2024.  Harvard University
#
#   Developed by Research Software Engineering,
#   Harvard University Research Computing and Data (RCD) Services.
#
#   Author: Michael A Bouzinier
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#          http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#
#
import math
from typing import Dict, Optional, List

HTML = """
<!DOCTYPE html>
<html class="writer-html5" lang="en" >
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>{title}</title>
</head>
<body>
{body}
</body>
<html>
"""


class DataModelElement:
    def __init__(self, block: Optional[Dict]):
        self.block = block
        self.reference: Optional[str] = None
        self.description: Optional[Dict] = None
        if self.block is None:
            return 
        if "description" in self.block:
            descr = self.block["description"]
            if isinstance(descr, dict):
                self.description = descr
            else:
                self.description = {"text": descr}
        if "reference" in self.block:
            self.reference = self.block["reference"]
        else:
            self.reference = None


def tab_indent(indent: int) -> str:
    return ''.join('\t' for _ in range(indent))


class Graph:
    def __init__(self):
        self.nodes: Dict[str,str] = dict()
        self.edges = []
        self.outer_group = []
        self.groups:Dict[str,List] = dict()

    def add_edge(self, edge):
        self.edges.append(edge)

    def add_node(self, node_id, node_repr, group = None):
        if node_id in self.nodes:
            if group is None and node_id in self.outer_group:
                return
            if group and node_id in self.groups[group]:
                return 
            raise ValueError("Duplicate node: " + node_id)
        self.nodes[node_id] = node_repr
        if group is None:
            self.outer_group.append(node_id)
        else:
            if group not in self.groups:
                self.groups[group] = []
            self.groups[group].append(node_id)

    def print(self, file, indent: int):
        space = tab_indent(indent)
        print("digraph {", file=file)
        for node in self.outer_group:
            print(f"{space}\t{self.nodes[node]}", file=file)
        for group in self.groups:
            self.print_group(group, indent=indent, file=file)
        print(file=file)
        
        for edge in self.edges:
            print(repr(edge), file=file)
        print("}", file=file)
        return

    def print_group(self, group_id, file, indent: int, max_h = 4):
        print(f"// Group: {group_id}", file=file)
        space = tab_indent(indent)
        group = self.groups[group_id]
        nn = len(group)
        if nn < max_h:
            for node in group:
                print(f"{space}\t{self.nodes[node]}", file=file)
            print(file=file)
            return
        n = max(round(math.sqrt(nn)), max_h)
        i = 0
        m = None
        while i < nn:
            print(f"{space}{{ rank = same; ", file=file)
            nodes = []
            for j in range(i, i + n):
                if j >= nn:
                    break
                node_id = group[j]
                node_string = self.nodes[node_id]
                nodes.append(node_id)
                print(f"{space}\t{node_string}", file = file)
            print(f"{space}}}", file=file)
            if m:
                s = " ".join(nodes)
                print(f"\t{m} -> {{ {s} }} [ style = invis ];", file=file)
            idx = min((int(i + n / 2)), nn - 1)
            m = group[idx]
            i += n

        print(f"// End Group: {group_id}", file=file)
        print(file=file)


def fqn(schema: str, name: str) -> str:
    return schema + '.' + name


def qstr(s):
    return f'"{str(s)}"'


def hstr(s):
    s = s.replace("\n", "<br/>")
    return f'<{str(s)}>'


def attrs2string(attrs: Dict[str,str]) -> str:
    alist = [f'{key}={attrs[key]}' for key in attrs]
    return ','.join(alist)


def add_html_row(cols: List[str], border: int = 1, align: str = None, tag = "td") -> str:
    if len(cols) == 1:
        value = cols[0]
        if not align:
            align = "center"
        return f'<tr><{tag}  align = "{align}" border = "{border}">{value}</{tag}></tr>\n'
    if not align:
        align = "left"
    text = '<tr align = "{align}" border = "{border}">'
    for c in cols:
        text += f'<{tag}>{c}</{tag}>'
    text += '</tr>\n'
    return text


def add_row(cols: List[str]) -> str:
    return add_html_row(cols)


def start_invisible_row() -> str:
    return '<tr border = "0"><td>'


def end_invisible_row() -> str:
    return '</td></tr>'


def add_header_row(cols: List[str]) -> str:
    return add_html_row(cols, tag="th")


def start_table(border: int = 1, align: str = "left"):
    return f'\n<br/><TABLE border = "{border}" align = "{align}">\n'


def end_table():
    return '\n</TABLE><br/>\n'


def hr():
    return "<hr/>"




