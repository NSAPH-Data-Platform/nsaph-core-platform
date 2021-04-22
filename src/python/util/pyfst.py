import csv
import datetime
import os
import sys
import rpy2.robjects as robjects
import rpy2.robjects.packages as rpackages
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


def read_fst(path: str) -> DataFrame:
    ensure_packages()
    f = robjects.r["read_fst"]
    df = f(path)
    return df


def write_csv(df: DataFrame, dest: str):
    t0 = datetime.datetime.now()
    columns = {
        df.colnames[c]: list(df[c]) for c in range(df.ncol)
    }
    t1 = datetime.datetime.now()

    with fopen(dest, "wt") as output:
        writer = csv.DictWriter(output, columns, quoting=csv.QUOTE_NONNUMERIC)
        for r in range(df.nrow):
            row = {
                column: columns[column][r] for column in columns
            }
            writer.writerow(row)
            if (r % 10000) == 0:
                print(r)
    t2 = datetime.datetime.now()
    print("{} + {} = {}".format(str(t1-t0), str(t2-t1), str(t2-t0)))
    return


def convert(path: str):
    if not path.endswith(".fst"):
        raise Exception("Unknown format of file " + path)
    name = path[:-4]
    dest = name + ".csv.gz"
    df = read_fst(path)
    print("Read {}: {:d} x {:d}".format(path, df.ncol, df.nrow))
    write_csv(df, dest)
    return


if __name__ == '__main__':
    convert(sys.argv[1])
    print("All Done")