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

from nsaph.loader.common import CommonConfig


class Parallelization(Enum):
    lines = "lines"
    files = "files"
    none = "none"


class LoaderConfig(CommonConfig):
    """
        Configurator class
    """

    _data = Argument("data",
        help = "Path to a data file or directory. Can be a "
                + "single CSV, gzipped CSV or FST file or a directory recursively "
                + "containing CSV files. Can also be a tar, tar.gz (or tgz) or zip archive "
                + "containing CSV files",
        type = str,
        required = False,
        cardinality = Cardinality.multiple
    )

    _pattern = Argument("pattern",
        help = "pattern for files in a directory or an archive, "
               + "e.g. \"**/maxdata_*_ps_*.csv\"",
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

    _incremental = Argument("incremental",
        help = "Commit every file and skip over files that "
               + "have already been ingested",
        type = bool,
        default = False,
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
        self.data = None
        self.reset = None
        self.page = None
        self.log = None
        self.limit = None
        self.buffer = None
        self.threads = None
        self.parallelization = None
        self.pattern = None
        self.incremental = None
        super().__init__(LoaderConfig, doc)

    def validate(self, attr, value):
        value = super().validate(attr, value)
        if attr == self._parallelization.name:
            return Parallelization(value)
        return value
