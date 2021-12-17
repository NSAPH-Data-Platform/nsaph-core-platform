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

from airflow.plugins_manager import AirflowPlugin

from airflow.models.baseoperator import BaseOperatorLink


# A global operator extra link that redirect you to
# task logs stored in S3
class CWLLogLink(BaseOperatorLink):
    name = "CWL Log"

    def get_link(self, operator, dttm):
        template = "http://localhost:8280/log?dag_id={dag_id}&task_id={task_id}&execution_date={date}"
        url = template.format(dag_id=operator.dag_id,
                              task_id=operator.task_id,
                              date=dttm)
        return url


# Defining the plugin class
class AirflowCWLLogViewPlugin(AirflowPlugin):
    name = "cwl_log_viewer"
    global_operator_extra_links = [CWLLogLink(),]
