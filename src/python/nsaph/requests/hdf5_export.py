"""
A utility to export result of quering the database in HDF5 format
"""

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

import argparse
import os
from typing import List

import h5py
import numpy

from nsaph.db import ResultSetDeprecated
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


def map2ds(rs: ResultSetDeprecated, groups: list) -> List:
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


def _export(request: dict, rs: ResultSetDeprecated, path: str):
    """
    Internal method. Use :func:export

    :param request: A dictionary containing user request, normally the result
        of parsed YAML file
    :param rs: ResultSet obtained by executing the generated query
    :param path:  Output path where to export HDF5 file
    :return: None
    """

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


def export(path_to_user_request: str, db_ini: str, connection_name: str):
    """
    Executes query specified by a user request and exports results
    as HDF5 file

    :param path_to_user_request: a path to YAML file containing user request
        specification
    :param db_ini: a path to database.ini file, containing connection
        parameters
    :param connection_name: section name in teh db.ini file

    :return:
    """

    with Query(path_to_user_request, (db_ini, connection_name)) as q:
        print(q.sql)
        count = 0
        rs = q.execute()
        _export(q.request, rs, q.request["name"] + ".hdf5")
    print("All done")


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
        args.request = os.path.join(d, "../../../yml/example_request.yaml")

    export(args.request, args.db, args.section)
    print("All done")
