"""
A utility that prints the statistics about
currently running indexing processes
"""

from nsaph.db import Connection
from nsaph.index import print_stat

if __name__ == '__main__':
    with Connection() as connection:
        print_stat(connection)
