import argparse
import os

import h5py
import numpy

from nsaph.db import ResultSet
from nsaph.requests.query import Query


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
        for g in groups[:-1]:
            h5.attrs["grouping"] = g
            h5 = h5.create_group(row[g])
        g = groups[-1]
        h5.attrs["grouping"] = g
        #ds = h5.create_dataset(str(row[g]), shape=(len(data_columns),None))
        data =[]
        idx = 0
        data.append(tuple(row[data_columns[i]] for i in data_columns))
        for row in rs:
            i = ng - 1
            while i > 0 and row[g] != current[g]:
                g = groups[i]
                i -= 1
            if i < ng - 1:
                array = numpy.array(data)
                ds = h5.create_dataset(str(row[groups[-1]]), data=array)
                for g in groups:
                    ds.attrs[g] = str(current[g])
                data.clear()
                current = {g: row[g] for g in groups}
                h5 = ds.parent
                ii = i
                for i in range(ii, ng - 1):
                    h5 = h5.parent
                for i in range(ii, ng - 1):
                    g = groups[i]
                    h5.attrs["grouping"] = g
                    h5 = h5.create_group(row[g])
                ds = h5.create_dataset(str(row[g]), shape=(len(data_columns),))
                for g in groups:
                    ds.attrs[g] = str(current[g])
            idx += 1
            data.append(tuple(row[data_columns[i]] for i in data_columns))


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