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
from typing import Dict, List, Optional, Set

import sqlparse
import yaml
from sqlparse.sql import IdentifierList, Parenthesis, Function, Identifier
import html

from nsaph.dictionary.element import HTML, DataModelElement, qstr, attrs2string
from nsaph.data_model.domain import Domain


def noop(x):
    return


class Column(DataModelElement):
    def __init__(self, table_name: str, column_block: Dict):
        if isinstance(column_block, dict):
            for name in column_block:
                self.name = name
                super().__init__(column_block[name])
                break
        else:
            self.name = str(column_block)
            super().__init__(None)
        self.qualified_name = table_name + '.' + self.name
        self.predecessors: Set[str] = set()
        self.functions: List[str] = []
        self.datatype = "string"
        self.column_type = ""
        self.expression = []
        self.copied = False
        self.casts = dict()
        self.requires = []
        if self.block is None:
            return
        if "type" in self.block:
            self.datatype = self.block["type"]
        if "source" in self.block:
            self.expand_macro2()
            src_block = self.block["source"]
            if isinstance(src_block, list):
                for item in src_block:
                    self.predecessors.add(item)
            elif isinstance(src_block, dict):
                if "type" in src_block:
                    self.column_type = src_block["type"]
                if "code" in src_block:
                    self.parse_expr(src_block["code"])
            elif isinstance(src_block, str):
                self.parse_expr(src_block)
        if "requires" in self.block:
            req = self.block["requires"]
            if isinstance(req, list):
                self.requires = req
            else:
                self.requires = [req]
        if "cast" in self.block:
            for t in self.block["cast"]:
                self.casts[t] = self.block["cast"][t]
        return

    def is_transformed(self):
        return self.expression or self.casts

    @classmethod
    def expand_macro1(cls, block: Dict) -> Optional[List[Dict]]:
        if not isinstance(block, dict):
            return [block]
        name = list(block.keys())[0]
        if '$' in name:
            x = Domain.parse_wildcard_column_spec(name)
            if x is not None:
                prefix, var, values, postfix = x
                b1 = yaml.safe_dump(block[name])
                expansion = []
                for v in values:
                    bi = b1.replace(f'${var}', str(v))
                    b = {f'{prefix}{str(v)}': yaml.safe_load(bi)}
                    expansion.append(b)
                return expansion
        return [block]

    def expand_macro2(self):
        src_block = self.block["source"]
        if isinstance(src_block, list):
            original = src_block
        elif isinstance(src_block, str):
            original = [src_block]
        else:
            return
        expansion = []
        expanded = False
        for s in original:
            if '$' in s:
                var = s[s.find('$') + 1]
                values = self.block[var]
                for v in values:
                    expansion.append(s.replace(f'${var}', v))
                expanded = True
            else:
                expansion.append(s)
        if expanded:
            self.block["source"] = expansion
        return

    def describe(self) -> str:
        text = f'{self.name} ({self.datatype}) {self.column_type}\n'
        if self.reference:
            text += "See: " + self.reference + '\n\n'
        try:
            if self.description:
                if "text" in self.description:
                    text += self.description["text"] + '\n'
                for key in self.description:
                    if key == "text":
                        continue
                    text += key + ': ' + str(self.description[key]) + '\n'
        except:
            print("ERROR:")
            print(str(self.description))
        if self.expression:
            exp = ';\n'.join(self.expression)
            text += "\n\n" + exp + '\n'
        return text

    def describe_html(self) -> str:
        text = "\n<TABLE>\n"
        text += "<tr>"
        text += f'<td align = "center" border = "0"><FONT POINT-SIZE="20"><b>{self.qualified_name}</b></FONT></td>'
        text += f'<td align = "center" border = "0">{self.datatype}</td>'
        text += "</tr>\n"

        if self.column_type:
            text += f'<tr><td  align = "center" border = "0"><i>{self.column_type}</i></td></tr>\n'

        if self.reference:
            text += f'<tr><td  align = "center" border = "0"><i>For more information see: {self.reference}</i></td></tr>\n'
        if self.description and "text" in self.description:
            value = html.escape(self.description["text"])
            text += f'<tr><td  align = "center" border = "0">{value}</td></tr>\n'

        n = 0
        if self.description is not None:
            for key in self.description:
                if key == "text":
                    continue
                value = html.escape(str(self.description[key]))
                text += f'\t<tr><td align = "left">{key}</td><td align = "left">{value}</td></tr>\n'
                if n == 0:
                    text += "<hr/>\n"
                n += 1
        if self.expression:
            if n > 0:
                text += "<hr/>\n"
                n = 0
            for exp in self.expression:
                value = html.escape(exp)
                text += f'\t<tr><td align = "left">{value}</td></tr>\n'
        for key in self.casts:
            if n > 0:
                text += "<hr/>\n"
            value = html.escape(str(self.casts[key]))
            text += f'<tr><td align = "left">{key} &rarr;</td><td align = "left">{value}</td></tr>\n'
        text += "\n</TABLE>\n"
        return text

    def html(self, of: str, svg = None):
        body = self.describe_html()
        if svg:
            body += "<hr/>\n"
            body += f'<object data="{svg}" type="image/svg+xml"> </object>'
        block = HTML.format(
            title = f"Column {self.qualified_name}",
            body = body
        )
        with open(of, "wt") as out:
            print(block, file=out)

    def to_dot(self):
        node_id = qstr(self.qualified_name)
        node_label = '<' + self.describe_html() + '>'
        attrs = {
            "label": node_label,
            "shape": "box"
        }
        if self.reference:
            attrs["URL"] = qstr(self.reference)
            attrs["target"] = "_blank"
        return f"\t{node_id} [{attrs2string(attrs)}];"

    def __repr__(self):
        return self.to_dot()

    def parse_expr(self, exp: str):
        parsed = sqlparse.parse(exp)[0]
        self.find_all_names(parsed)
        self.expression.append(exp)

    def find_all_names(self, element):
        if hasattr(element, "tokens"):
            n = len(element.tokens)
            for i in range(0, n):
                t = element.tokens[i]
                if str(t.ttype) == "Token.Name":
                    if isinstance(element, IdentifierList) or isinstance(element, Parenthesis):
                        self.predecessors.add(str(t))
                    elif isinstance(element, Function):
                        self.functions.append(str(t))
                    elif isinstance(element, Identifier) and hasattr(element, "parent"):
                        if isinstance(element.parent, Function):
                            self.functions.append(str(t))
                        elif isinstance(element.parent, IdentifierList) or isinstance(element.parent, Parenthesis):
                            self.predecessors.add(str(t))
                        else:
                             noop(t)
                    else:
                        noop(t)
                else:
                    self.find_all_names(t)
        return

    def __str__(self) -> str:
        repr = "Column: " + self.name
        if self.predecessors:
            repr += " <= " + ', '.join(self.predecessors)
        if self.functions:
            repr += " [" + ', '.join(self.functions) + ']'
        return repr


c1 = """
age_min:
    source: |
      CASE
        WHEN MIN(age) <> MAX(age) THEN MIN(age)
      END
"""

c2 = """
fips3_valdiated:
    source: public.validate_zip_fips(MAX(zip), MAX(fips2), MAX(fips3))
"""

c3="""
ssa2_list:
    source: "string_agg(distinct ssa2, ',')"
"""

c4 = """
fips5:
    source: "(MAX(fips2) || MAX(fips3))"

"""


if __name__ == '__main__':
    for c in [c1, c2, c3, c4]:
        a = yaml.safe_load(c)
        column = Column("xxx", a)
        print(column)


