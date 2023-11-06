from nsaph.loader.common import DBTableConfig
from nsaph_utils.utils.context import Argument, Cardinality


class DBTConfig(DBTableConfig):
    _script = Argument("script",
                      help = "Path to the file(s) containing test scripts."
                       + " When generating test, the file is used to output"
                       + " the script, while when running the tests, "
                       + "the scripts from the specified files are executed",
                      type = str,
                      required = True,
                      aliases = ["s"],
                      default = None,
                      cardinality = Cardinality.multiple
                      )

    def __init__(self, subclass, doc):
        self.script = None
        '''Path to the file(s) containing test scripts'''

        if subclass is None:
            super().__init__(DBTConfig, doc)
        else:
            super().__init__(subclass, doc)
            self._attrs += [
                attr[1:] for attr in DBTConfig.__dict__
                if attr[0] == '_' and attr[1] != '_'
            ]



