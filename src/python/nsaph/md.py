#  Copyright (c) 2021. Harvard University
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

import logging
import os
import sys
from typing import Dict

import marko
from marko.inline import CodeSpan, RawText


def fill_c_desc(element, d: Dict):
    if isinstance(element, list):
        if (len(element) == 2
                and isinstance(element[0], CodeSpan)
                and isinstance(element[1], RawText)):
            text = element[1].children
            if text.startswith(':'):
                text = text[1:].strip()
            d[element[0].children] = text
            return
        for child in element:
            fill_c_desc(child, d)
    else:
        if hasattr(element, "children"):
            fill_c_desc(element.children, d)
    return



def parse(path: str, columns):
    try:
        if os.path.isfile(path):
            is_file = True
        else:
            is_file = False
    except:
        is_file = False
    if is_file:
        with open(path) as d:
            txt = d.read()
    else:
        txt = path
    document = marko.parse(txt)
    variables = dict()
    for child in document.children:
        if isinstance(child, marko.block.List):
            fill_c_desc(child, variables)
    cds = dict()
    for column in columns:
        if column in variables:
            cds[column] = variables[column]

    return txt, cds


if __name__ == '__main__':
    path = sys.argv[1]
    columns = sys.argv[2:]
    txt, cds = parse(path, columns)
    logging.info(txt)
    logging.info(cds)