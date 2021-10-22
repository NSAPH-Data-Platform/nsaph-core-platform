import gzip
import os
import glob
import random

from nsaph_utils.utils.io_utils import fopen


SEED = 1


def select(pattern: str, destination: str, threshold: float):
    files = glob.glob(pattern)
    random.seed(SEED)
    for f in files:
        name = os.path.basename(f)
        if not name.endswith('.gz'):
            name += ".gz"
        dest = os.path.basename(os.path.dirname(f))
        dest = os.path.join(destination, dest)
        if not os.path.isdir(dest):
            os.makedirs(dest)
        dest = os.path.join(dest, name)
        if os.path.isfile(dest):
            print("Skipping: {}".format(f))
            continue
        print("{} ==> {}".format(f, dest))

        with fopen(f, "rt") as src, gzip.open(dest, "wt") as output:
            n1 = 0
            n2 = 0
            for line in src:
                n1 += 1
                if random.random() < threshold:
                    output.write(line)
                    n2 += 1
        print("{:d}/{:d}".format(n2, n1))
    print("All Done")


if __name__ == '__main__':
    select("data/*/*.csv*", "random_data", 0.02)

