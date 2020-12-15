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