"""
Common options for data manipulation
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

from nsaph_utils.utils.context import Context, Argument, Cardinality


class DBConnectionConfig(Context):
    """
    Configuration class for connection to a database
    """

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

    def __init__(self, subclass, doc):
        self.autocommit = None
        ''' Use autocommit '''

        self.db = None
        ''' Path to a database connection parameters file '''

        self.connection = None
        ''' Section in the database connection parameters file '''

        if subclass is None:
            super().__init__(DBConnectionConfig, doc, include_default = False)
        else:
            super().__init__(subclass, doc, include_default = False)
            self._attrs += [
                attr[1:] for attr in DBConnectionConfig.__dict__
                if attr[0] == '_' and attr[1] != '_'
            ]


class DBTableConfig(DBConnectionConfig):
    _table = Argument("table",
        help = "Name of the table to manipulate",
        type = str,
        required = False,
        aliases = ["t"],
        default = None,
        cardinality = Cardinality.single
    )

    def __init__(self, subclass, doc):
        self.table = None
        ''' Name of the table to manipulate '''

        if subclass is None:
            super().__init__(DBTableConfig, doc)
        else:
            super().__init__(subclass, doc)
            self._attrs += [
                attr[1:] for attr in DBTableConfig.__dict__
                if attr[0] == '_' and attr[1] != '_'
            ]


class CommonConfig(DBTableConfig):
    """
    Abstract base class for configurators used for data loading

    """

    _domain = Argument("domain",
        help = "Name of the domain",
        type = str,
        required = True,
        cardinality = Cardinality.single,
        valid_values = None
    )

    _registry = Argument("registry",
        help = "Path to domain registry. "
               + "Registry is a directory or an archive "
               + "containing YAML files with domain "
               + "definition. Default is to use "
               + "the built-in registry",
        type = str,
        required = False,
        cardinality = Cardinality.single,
        valid_values = None
    )

    def __init__(self, subclass, doc):
        self.domain = None
        ''' Name of the domain '''

        self.registry = None
        """
        Path to domain registry. 
        Registry is a directory or an archive 
        containing YAML files with domain 
        definition. Default is to use 
        the built-in registry
        """

        if subclass is None:
            super().__init__(CommonConfig, doc)
        else:
            super().__init__(subclass, doc)
            self._attrs += [
                attr[1:] for attr in CommonConfig.__dict__
                if attr[0] == '_' and attr[1] != '_'
            ]
