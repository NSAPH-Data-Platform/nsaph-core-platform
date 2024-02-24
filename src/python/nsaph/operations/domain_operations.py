#  Copyright (c) 2023-2023. Harvard University
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

import nsaph.dictionary.element


class DomainOperations:
    @classmethod
    def drop(cls, domain, table, connection, ex_handler = None) -> list:
        try:
            tables = domain.find_dependent(table)
            with connection.cursor() as cursor:
                for t in tables:
                    obj = tables[t]
                    if "create" in obj:
                        kind = obj["create"]["type"]
                    else:
                        kind = "TABLE"
                    sql = "DROP {TABLE} IF EXISTS {} CASCADE".format(t, TABLE=kind)
                    logging.info(sql)
                    cursor.execute(sql)
                if not connection.autocommit:
                    connection.commit()
            return [t for t in tables]
        except BaseException as ex:
            if ex_handler is not None:
                ex_handler.exception = ex
            else:
                raise ex

    @classmethod
    def create(cls, domain, connection, list_of_tables = None, ex_handler = None):
        try:
            with connection.cursor() as cursor:
                if list_of_tables:
                    statements = [ddl for ddl in  domain.common_ddl]
                    for t in list_of_tables:
                        statements += domain.ddl_by_table[domain.fqn(t)]
                else:
                    statements = domain.ddl
                for statement in statements:
                    logging.info(statement)
                sql = "\n".join(statements)
                cursor.execute(sql)
                if not connection.autocommit:
                    connection.commit()
                logging.info("Schema and all tables for domain {} have been created".format(domain.domain))
        except BaseException as ex:
            if ex_handler is not None:
                ex_handler.exception = ex
            else:
                raise ex
