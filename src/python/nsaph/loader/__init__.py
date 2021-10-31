import glob
import os
from abc import ABC
from pathlib import Path

import sys
from typing import Iterable

from psycopg2.extensions import connection

from nsaph import init_logging
from nsaph.data_model.domain import Domain
from nsaph.loader.common import CommonConfig
from nsaph.db import Connection


def diff(files: Iterable[str]) -> bool:
    ff = list(files)
    for i in range(len(ff) - 1):
        ret = os.system("diff {} {}".format(ff[i], ff[i+1]))
        if ret != 0:
            return False
    return True


class LoaderBase(ABC):
    """
    Base class for tools responsible for data loading and processing
    """

    @staticmethod
    def is_dir(path: str) -> bool:
        """
        Determine if a certain path specification refers
            to a collection of files or a single entry.
            Examples of collections are folders (directories)
            and archives

        :param path: path specification
        :return: True if specification refers to a collection of files
        """

        return (path.endswith(".tar")
                or path.endswith(".tgz")
                or path.endswith(".tar.gz")
                or path.endswith(".zip")
                or os.path.isdir(path)
        )


    @staticmethod
    def is_yaml_or_json(path: str) -> bool:
        path = path.lower()
        for ext in [".yml", ".yaml", ".json"]:
            if path.endswith(ext) or path.endswith(ext + ".gz"):
                return True
        return  False

    @classmethod
    def get_domain(cls, name: str, registry: str = None) -> Domain:
        src = None
        registry_path = None
        if registry:
            if cls.is_yaml_or_json(registry):
                registry_path = registry
            elif not cls.is_dir(registry):
                raise ValueError("{} - is not a valid registry path".format(registry))
            elif os.path.isdir(registry):
                src = registry
            else:
                raise NotImplementedError("Not Implemented: registry in an archive")
        if not src:
            src = Path(__file__).parents[3]
        if not registry_path:
            registry_path = os.path.join(src, "yml", name + ".yaml")
        if not os.path.isfile(registry_path):
            files = set()
            for d in sys.path:
                files.update(glob.glob(
                    os.path.join(d, "**", name + ".yaml"),
                    recursive=True
                ))
            if not files:
                raise ValueError("File {} not found".format(name + ".yaml"))
            if len(files) > 1:
                if not diff(files):
                    raise ValueError("Ambiguous files {}".format(';'.join(files)))
            registry_path = files.pop()
        if not os.path.isfile(registry_path):
            raise ValueError("File {} does not exist".format(os.path.abspath(registry_path)))
        domain = Domain(registry_path, name)
        domain.init()
        return domain

    def __init__(self, context):
        init_logging()
        self.context = None
        self.domain = self.get_domain(context.domain, context.registry)
        self.table = context.table

    def _connect(self) -> connection:
        c = Connection(self.context.db, self.context.connection).connect()
        if self.context.autocommit is not None:
            c.autocommit = self.context.autocommit
        return c



