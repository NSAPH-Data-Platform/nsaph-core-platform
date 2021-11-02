"""
Common options for data manipulation
"""
from nsaph_utils.utils.context import Context, Argument, Cardinality


class CommonConfig(Context):
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

    _table = Argument("table",
        help = "Name of the table to manipulate",
        type = str,
        required = False,
        aliases = ["t"],
        default = None,
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

        self.table = None
        ''' Name of the table to manipulate '''

        self.autocommit = None
        ''' Use autocommit '''

        self.db = None
        ''' Path to a database connection parameters file '''

        self.connection = None
        ''' Section in the database connection parameters file '''

        super().__init__(subclass, doc, include_default = False)
        self._attrs += [
            attr[1:] for attr in CommonConfig.__dict__
            if attr[0] == '_' and attr[1] != '_'
        ]
