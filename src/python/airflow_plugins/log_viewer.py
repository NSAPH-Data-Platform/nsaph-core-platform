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
