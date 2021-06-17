"""
Domain Loader Configurator

Intended to configure loading of a single or a set of column-formatted files
into NSAPH PostgreSQL Database.
Input (aka source) files can be either in FST or in CSV format

Configurator assumes that the database schema is defined as a YAML or JSON file.
A separate tool is available to introspect source files and infer possible
database schema.
"""

from enum import Enum
from nsaph_utils.utils.context import Context, Argument, Cardinality


class Parallelization(Enum):
    lines = "lines"
    files = "files"
    none = "none"


class Config(Context):
    """
        Configurator class
    """

    _domain = Argument("domain",
        help = "Name of the domain",
        type = str,
        required = True,
        cardinality = Cardinality.single,
        valid_values = None
    )

    _table = Argument("table",
        help = "Name of the table to load data into",
        type = str,
        required = False,
        aliases = ["t"],
        default = None,
        cardinality = Cardinality.single
    )

    _data = Argument("data",
        help = "Path to a data file or directory",
        type = str,
        required = False,
        cardinality = Cardinality.multiple
    )

    _reset = Argument("reset",
        help = "Force recreating table(s) if it/they already exist",
        type = bool,
        default = False,
        cardinality = Cardinality.single
    )

    _autocommit = Argument("autocommit",
          help = "Use autocommit",
          type = bool,
          default = False,
          cardinality = Cardinality.single
    )

    _db = Argument("db",
        help = "Path to a database connection parameters file",
        type = str,
        default = "database.ini",
        cardinality = Cardinality.single
    )

    _connection = Argument(
        "connection",
        help = "Section in the database connection parameters file",
        type = str,
        default = "nsaph2",
        cardinality = Cardinality.single
    )

    _page = Argument(
        "page",
        help = "Explicit page size for the database",
        required = False,
        type = int
    )

    _log = Argument(
        "log",
        help = "Explicit interval for logging",
        required = False,
        type = int
    )

    _limit = Argument(
        "limit",
        help = "Load at most specified number of records",
        required = False,
        type = int
    )

    _buffer = Argument(
        "buffer",
        help = "Buffer size for converting fst files",
        required = False,
        type = int
    )

    _threads = Argument(
        "threads",
        help = "Number of threads writing into the database",
        default = 1,
        type = int
    )

    _parallelization = Argument(
        "parallelization",
        help = "Type of parallelization, if any",
        default = "lines",
        cardinality = Cardinality.single,
        valid_values = [v.value for v in Parallelization]
    )

    def __init__(self, doc):
        self.domain = None
        self.table = None
        self.data = None
        self.autocommit = None
        self.reset = None
        self.db = None
        self.connection = None
        self.page = None
        self.log = None
        self.limit = None
        self.buffer = None
        self.threads = None
        self.parallelization = None
        super().__init__(Config, doc)

    def validate(self, attr, value):
        value = super().validate(attr, value)
        if attr == self._parallelization.name:
            return Parallelization(value)
        return value
