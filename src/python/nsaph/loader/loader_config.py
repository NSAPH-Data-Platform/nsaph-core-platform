"""
Domain Loader Configurator

Intended to configure loading of a single or a set of column-formatted files
into NSAPH PostgreSQL Database.
Input (aka source) files can be either in FST or in CSV format

Configurator assumes that the database schema is defined as a YAML or JSON file.
A separate tool is available to introspect source files and infer possible
database schema.
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

from enum import Enum
from typing import Optional

from nsaph_utils.utils.context import Context, Argument, Cardinality

from nsaph.loader.common import CommonConfig


class Parallelization(Enum):
    lines = "lines"
    files = "files"
    none = "none"


class DataLoaderAction(Enum):
    drop = "drop"
    load = "load"
    insert = "insert"
    print = "print"

    @classmethod
    def new(cls, value: str):
        if value is None:
            return None
        return cls(value)
    #default = "default"


class LoaderConfig(CommonConfig):
    """
    Configurator class for data loader
    """

    _action = Argument(
        "action",
        help = "Action to perform",
        type = str,
        required=False,
        valid_values = [v.value for v in DataLoaderAction]
    )

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

    _sloppy = Argument("sloppy",
        help = "Do not update existing tables",
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
        type = str
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
        self.action: Optional[DataLoaderAction] = None
        """ 
        If this option is given, then the whole domain schema will be dropped
        """

        self.data = None
        """
        Path to a data file or directory. Can be a 
        single CSV, gzipped CSV or FST file or a directory recursively 
        containing CSV files. Can also be a tar, tar.gz (or tgz) or zip archive 
        containing CSV files        
        """

        self.reset = None
        ''' Force recreating table(s) if it/they already exist '''

        self.page = None
        ''' Explicit page size for the database '''

        self.log = None
        ''' Explicit interval for logging '''

        self.limit = None
        ''' Load at most specified number of records '''

        self.buffer = None
        ''' Buffer size for converting fst files '''

        self.threads = None
        ''' Number of threads writing into the database '''

        self.parallelization = None
        ''' Type of parallelization, if any '''

        self.pattern = None
        """
        pattern for files in a directory or an archive, 
        e.g., \"\*\*/maxdata_\*_ps_\*.csv\"
        """

        self.incremental = None
        """
        Commit every file and skip over files that 
        have already been ingested
        """

        self.sloppy = False
        '''Do not update existing tables and views'''

        super().__init__(LoaderConfig, doc)

    def validate(self, attr, value):
        value = super().validate(attr, value)
        if attr == self._parallelization.name:
            return Parallelization(value)
        if attr == self._action.name:
            return DataLoaderAction.new(value)
        return value


