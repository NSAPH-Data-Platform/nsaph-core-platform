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
from sqlparse.sql import IdentifierList, Token, Parenthesis, Function, Identifier


def noop(x):
    return


class Column:
    def __init__(self, table_name: str, column_block: Dict):
        if isinstance(column_block, dict):
            for name in column_block:
                self.name = name
                self.block = column_block[name]
                break
        else:
            self.name = str(column_block)
            self.block = None
        self.fqn = table_name + '.' + self.name
        self.predecessors: Set[str] = set()
        self.functions: List[str] = []
        self.reference: Optional[str] = None
        self.description: Optional[Dict] = None
        self.datatype = "string"
        self.column_type = ""
        self.expression = None
        if self.block is None:
            return
        if "description" in self.block:
            descr = self.block["description"]
            if isinstance(descr, dict):
                self.description = descr
            else:
                self.description = {"text": descr}
        if "type" in self.block:
            self.datatype = self.block["type"]
        if "source" in self.block:
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
                    text += key + ': ' + str(self.description[key])
        except:
            print("ERROR:")
            print(str(self.description))
        if self.expression:
            text += "\n\n" + self.expression + '\n'
        return text

    def parse_expr(self, exp: str):
        parsed = sqlparse.parse(exp)[0]
        self.find_all_names(parsed)
        self.expression = exp

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


