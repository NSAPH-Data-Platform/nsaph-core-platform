import argparse
import os
from typing import List

import h5py
import numpy

from nsaph.db import ResultSet
from nsaph.requests.query import Query


class Dataset:
    def __init__(self, t):
        self.data = []
        self.indices = []
        self.type = t
        self.max_len = 1

    def clear(self):
        self.data.clear()

    def add_index(self, idx):
        self.indices.append(idx)

    def append(self, row: list):
        if self.type == float:
            values = [float(row[i]) for i in self.indices]
        else:
            values = [row[i] for i in self.indices]
        if self.type == str:
            self.max_len = max([self.max_len] + [len(v) for v in values])
        self.data.append(values)

    def type_name(self):
        return self.type.__name__

    def to_hdf5(self, parent, name):
        ds_name = "{}_{}".format(name, self.type_name())
        if self.type == str:
            dtype = "<S{}".format(self.max_len)
            h5 = parent.create_dataset(ds_name, data=self.data, dtype=dtype)
        else:
            h5 = parent.create_dataset(ds_name, data=self.data)
        for v in enumerate(self.indices):
            h5.attrs[v[1]] = v[0]
        return h5


def dtype(t):
    if t == int:
        return numpy.dtype(numpy.int32)
    if t == float:
        return numpy.dtype(numpy.float)
    return numpy.dtype(numpy.str)


def append(datasets: List, row):
    for dataset in datasets:
        dataset.append(row)
    return


def map2ds(rs: ResultSet, groups: list) -> List:
    datasets = {}
    for i in range(0, len(rs.header)):
        h = rs.header[i]
        t = rs.types[i]
        if h not in groups:
            if t.startswith("int"):
                if int not in datasets:
                    datasets[int] = Dataset(int)
                datasets[int].add_index(h)
            elif t.startswith("numeric") or t.startswith("float"):
                if float not in datasets:
                    datasets[float] = Dataset(float)
                datasets[float].add_index(h)
            else:
                if str not in datasets:
                    datasets[str] = Dataset(str)
                datasets[str].add_index(h)
    return list(datasets.values())


def store(parent, name:str, datasets: list, attrs):
    for dataset in datasets:
        h5ds = dataset.to_hdf5(parent, name)
        for attr in attrs:
            h5ds.attrs[attr] = attrs[attr]
        dataset.clear()


def export(request: dict, rs: ResultSet, path: str):
    name = os.path.basename(path).split('.')[0]
    groups = None
    if "package" in request:
        if "group" in request["package"]:
            groups = request["package"]["group"]
    if isinstance(groups, str):
        groups = [groups]
    ng = len(groups)
    data_columns = {}
    i = 0
    for h in rs.header:
        if h not in groups:
            data_columns[i] = h
            i += 1

    row = next(rs)
    current = {g: row[g] for g in groups}

    with h5py.File(path, "w") as f:
        f.attrs["name"] = request["name"]
        f.attrs["file_name"] = name
        h5 = f
        for g in groups:
            h5.attrs["grouping"] = g
            h5 = h5.create_group(str(row[g]))
        g = groups[-1]
        h5.attrs["grouping"] = g
        datasets = map2ds(rs, groups)
        idx = 0
        append(datasets, row)
        for row in rs:
            i = ng
            while i > 0:
                g = groups[i-1]
                if row[g] == current[g]:
                    break
                i -= 1
            if i < ng:
                attrs = {g: str(current[g]) for g in groups}
                store(h5, str(row[groups[-1]]), datasets, attrs)
                current = {g: row[g] for g in groups}
                ii = i
                for i in range(ii, ng):
                    h5 = h5.parent
                for i in range(ii, ng):
                    g = groups[i]
                    h5.attrs["grouping"] = g
                    h5 = h5.create_group(str(row[g]))
            idx += 1
            append(datasets, row)
        attrs = {g: str(current[g]) for g in groups}
        store(h5, str(row[groups[-1]]), datasets, attrs)


if __name__ == '__main__':
    #init_logging()
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
        export(q.request, rs, q.request["name"] + ".hdf5")
    print("All done")