from nsaph_utils.utils.pyfst import FSTReader
from nsaph_utils.utils.io_utils import fopen
import csv


def split(node) -> (str, dict):
    """
    Given a node in an array produced from JSON/YAML structure
    returns a name and definition associated with the node

    :param node: an array node, usually from YAML or JSON file
    :return: a tuple consisting of a name and an array containing
        definitions
    """
    if isinstance(node, str):
        return node, {}
    if not isinstance(node, dict):
        raise Exception("Unsupported type for column spec: " + str(node))
    name = None
    for entry in node:
        name = entry
        break
    node = node[name]
    if isinstance(node, str):
        node = {"type": node}
    if not isinstance(node, dict):
        raise Exception("Unsupported spec type for node: " + name)
    return name, node


def basename(table):
    """
    Given fully qualified table name (schema.table)
    returns just a basename of a table

    :param table: a fully qualified table name
    :return: just the base name of the table, a piece of name after teh last dot
    """
    return table.split('.')[-1]


class DataReader:
    """
    Generalized reader for columns-structured files, such as CSV and FST
    """

    def __init__(self, path: str):
        self.path = path
        self.reader = None
        self.columns = None
        self.to_close = None

    def open_fst(self):
        self.reader = FSTReader(self.path, 200000)
        self.reader.open()
        self.to_close = self.reader
        self.columns = list(self.reader.columns.keys())

    def open_csv(self):
        self.to_close = fopen(self.path, "rt")
        self.reader = csv.reader(self.to_close, quoting=csv.QUOTE_NONNUMERIC)
        header = next(self.reader)
        self.columns = header

    def __enter__(self):
        if self.path.lower().endswith(".fst"):
            self.open_fst()
        elif ".csv" in self.path.lower():
            self.open_csv()
        else:
            raise Exception("Unsupported file format: " + self.path)
        return self

    def rows(self):
        return self.reader

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.to_close.close()
