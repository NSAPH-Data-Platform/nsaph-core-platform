from nsaph.db import Connection
from nsaph.index import print_stat

with Connection() as connection:
    print_stat(connection)