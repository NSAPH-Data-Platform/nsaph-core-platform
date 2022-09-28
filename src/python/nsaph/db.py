"""
API to establish database connection

Connection details and credentials are specified in
database.ini file

This module supports connecting via ssh tunnel.
This happens automatically if the given section of
database.ini contains ssh_user key.

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

import logging
import socket
import paramiko
import psycopg2
import os
import sshtunnel
from configparser import ConfigParser

from deprecated.sphinx import deprecated

from nsaph import app_name


class Connection:
    default_filename = 'database.ini'
    default_section = 'postgresql'
    @classmethod
    def read_config(cls, filename, section):
        home = os.getenv("NSAPH_HOME")
        if home and not os.path.isabs(filename):
            filename = os.path.join(home, filename)
        if not os.path.isfile(filename):
            raise ValueError(
                "File {} does not exist or is not readable".format(filename)
            )
        parser = ConfigParser()
        parser.read(filename)

        parameters = {}
        if parser.has_section(section):
            params = parser.items(section)
            for param in params:
                parameters[param[0]] = param[1]
        else:
            raise ValueError('Section {0} not found in the {1} file'
                            .format(section, filename))
        return parameters

    @staticmethod
    def host_name():
        return socket.gethostname().lower()

    @classmethod
    def resolve_host(cls,host):
        hosts = host.lower().split(':')
        if (len(hosts) < 2):
            return host
        hostname = cls.host_name().lower()
        if (hostname in hosts[1:]):
            return "localhost"
        return hosts[0]

    @staticmethod
    def default_port() -> int:
        return 5432

    def __init__(self, filename=None, section=None,
                 silent: bool = False, app_name_postfix = ""):
        if not filename:
            filename = Connection.default_filename
        if not section:
            section = Connection.default_section
        self.parameters = self.read_config(filename, section)
        if "application_name" not in self.parameters:
            name = "nsaph:" + app_name() + app_name_postfix
            self.parameters["application_name"] = name
        self.connection = None
        self.tunnel = None
        self.silent = silent
        self.types = None

    def connect_to_database(self, params):
        if not self.silent:
            logging.info('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params)
        return conn

    def connect(self):
        if "ssh_user" in self.parameters:
            self.connection = self.connect_via_tunnel()
        else:
            self.connection = self.connect_to_database(self.parameters)
        info = self.connection.info
        if not self.silent:
            logging.info("Connected to: {}@{}:{}/{}"
                  .format(info.user, info.host, info.port, info.dbname))
        return self.connection

    def connect_via_tunnel(self):
        host = self.parameters["host"]
        home = os.path.expanduser('~')
        mypkey = paramiko.RSAKey.from_private_key_file(os.path.join(home, ".ssh", "id_rsa"))
        port = self.parameters.get("port", self.default_port())
        self.tunnel = sshtunnel.SSHTunnelForwarder(
            (host, 22),
            ssh_username=self.parameters["ssh_user"],
            ssh_pkey=mypkey,
            remote_bind_address=("localhost", port)
        )
        self.tunnel.start()
        params = dict()
        params.update(self.parameters)
        del params["ssh_user"]
        params["port"] = self.tunnel.local_bind_port
        params["host"] = self.tunnel.local_bind_host
        return self.connect_to_database(params)

    def get_database_types(self):
        if not self.types:
            sql = "SELECT oid, typname from pg_catalog.pg_type"
            cursor = self.connection.cursor()
            cursor.execute(sql)
            rows = cursor.fetchall()
            self.types = {
                row[0]: row[1] for row in rows
            }
            cursor.close()
        return self.types

    def close(self):
        if (self.connection and  not self.connection.closed):
            self.connection.close()
        self.connection = None
        if (self.tunnel):
            self.tunnel.stop()
        self.tunnel = None

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def __enter__(self):
        return self.connect()


def test_connection ():
    with Connection() as conn:
        cur = conn.cursor()

        logging.info('PostgreSQL database version:')
        cur.execute('SELECT version()')

        db_version = cur.fetchone()
        logging.info(db_version)

        cur.close()
    logging.info('Database connection closed.')


@deprecated(
    reason="Use psycopg2.extras.RealDictCursor",
    version="0.2"
)
class ResultSetDeprecated:
    SIZE = 10000

    def __init__(self, cursor, metadata: dict):
        self.cursor = cursor
        description = self.cursor.description
        self.header = [c.name for c in description]
        self.types = [metadata[c.type_code] for c in description]
        self.rows = self.cursor.fetchmany(self.SIZE)
        self.idx = 0

    def __iter__(self):
        return self

    def __next__(self):
        self.idx += 1
        if self.idx > len(self.rows):
            self.rows = self.cursor.fetchmany(self.SIZE)
            if len(self.rows) < 1:
                raise StopIteration
            self.idx = 1
        row = self.rows[self.idx - 1]
        return {self.header[i]: row[i] for i in range(len(self.header))}


# class PreparedInsert:
#     def __init__(self, cursor, sql: str):




if __name__ == '__main__':
    test_connection()


