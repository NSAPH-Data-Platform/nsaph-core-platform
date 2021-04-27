import csv
import datetime
import os
import sys
import rpy2.robjects as robjects
import rpy2.robjects.packages as rpackages
from rpy2.rinterface import NULL
from rpy2.robjects.vectors import DateVector
from rpy2.robjects import DataFrame

from src.python.nsaph.reader import fopen


def choose_cran_mirror():
    utils = rpackages.importr('utils')
    #mirrors = utils.getCRANmirrors()
    utils.chooseCRANmirror(ind=1)
    return utils


def ensure_packages():
    utils = choose_cran_mirror()
    packages = ["fst"]
    for p in packages:
        if not rpackages.isinstalled(p):
            utils.install_packages(p)
        rpackages.importr(p)
    return


def read_fst(path: str, start=1, end=None) -> (DataFrame, bool):
    ensure_packages()
    f = robjects.r["read_fst"]
    if start != 1 or end is not None:
        df = f(path, NULL, start, end)
        if end is None:
            complete = True
        elif df.nrow <= (end - start):
            complete = True
        else:
            complete = False
    else:
        df = f(path)
        complete = True
    return df, complete


EPOCH = datetime.date(year=1970, month=1, day=1).toordinal()
def vector2list(v):
    if DateVector.isrinstance(v):
        return [datetime.date.fromordinal(EPOCH + int(d)) if d == d else None for d in v]
    return list(v)


def write_csv(df: DataFrame, dest: str, append: bool):
    t0 = datetime.datetime.now()
    columns = {
        df.colnames[c]: vector2list(df[c]) for c in range(df.ncol)
    }
    t1 = datetime.datetime.now()

    if append:
        mode = "at"
    else:
        mode = "wt"
    with fopen(dest, mode) as output:
        writer = csv.DictWriter(output, columns, quoting=csv.QUOTE_NONNUMERIC)
        if not append:
            writer.writeheader()
        for r in range(df.nrow):
            row = {
                column: columns[column][r] for column in columns
            }
            writer.writerow(row)
    t2 = datetime.datetime.now()
    print("{} + {} = {}".format(str(t1-t0), str(t2-t1), str(t2-t0)))
    return


def convert(path: str):
    if not path.endswith(".fst"):
        raise Exception("Unknown format of file " + path)
    name = path[:-4]
    dest = name + ".csv.gz"
    complete = False
    n_rows = 0
    N = 10000
    start = 1
    append = False
    while not complete:
        end = start + N - 1
        df, complete = read_fst(path, start, end)
        start = end + 1
        n_rows += df.nrow
        print("Read {}: {:d}/{:d} x {:d}".format(path, n_rows, df.ncol, df.nrow), end="; ")
        write_csv(df, dest, append)
        append = True
    print("Complete. Total read {}: {:d} x {:d}".format(path, df.ncol, n_rows))
    return


if __name__ == '__main__':
    convert(sys.argv[1])
    print("All Done")